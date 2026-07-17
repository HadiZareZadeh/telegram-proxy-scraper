"""Scrape Telegram sources and keep catalogs fresh."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from types import ModuleType

from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.tl.custom.message import Message

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import config_float
from fetch_mtproto.mtproto.ping import PingResult, check_and_reorganize, patch_telethon_faketls
from fetch_mtproto.mtproto.store import MTProtoProxy, ProxyCatalog
from fetch_mtproto.scraper.client import (
    connect_via_proxy,
    fastest_working_proxy,
    switch_to_proxy,
)
from fetch_mtproto.scraper.ingest import ingest_message
from fetch_mtproto.v2ray.ping import check_and_reorganize_v2ray
from fetch_mtproto.v2ray.settings import ingest_subscription_kwargs, v2ray_test_kwargs
from fetch_mtproto.v2ray.store import V2RayCatalog

log = logging.getLogger("mtproto-scraper")


@dataclass
class ConnectionState:
    """Shared Telegram client state for periodic proxy switching."""

    client: TelegramClient | None = None
    current_proxy: MTProtoProxy | None = None
    pending_fastest: MTProtoProxy | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


async def _request_fastest_proxy_switch(
    config: ModuleType,
    mt_catalog: ProxyCatalog,
    conn_state: ConnectionState,
) -> None:
    """Schedule a reconnect to the fastest working proxy after a periodic check."""
    fastest = fastest_working_proxy(mt_catalog, config)
    if fastest is None:
        return

    async with conn_state.lock:
        current = conn_state.current_proxy
        if current is not None and fastest.key == current.key:
            log.info("Already on fastest proxy: %s", fastest.to_link())
            return
        log.info(
            "Scheduled switch to fastest proxy after check: %s",
            fastest.to_link(),
        )
        conn_state.pending_fastest = fastest
        client = conn_state.client

    if client is not None and client.is_connected():
        try:
            await client.disconnect()
        except Exception:
            pass


async def scrape_source(
    client: TelegramClient,
    source: str | int,
    limit: int | None,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    *,
    ingest_kwargs: dict | None = None,
) -> tuple[int, int]:
    mt_total = 0
    v2_total = 0
    try:
        entity = await client.get_entity(source)
    except Exception as exc:
        log.error("Cannot resolve source %r: %s", source, exc)
        return 0, 0

    title = getattr(entity, "title", None) or getattr(entity, "username", None) or source
    log.info("Scanning %s …", title)

    kwargs: dict = {}
    if limit is not None:
        kwargs["limit"] = limit

    scanned = 0
    async for message in client.iter_messages(entity, **kwargs):
        scanned += 1
        if not isinstance(message, Message):
            continue
        mt_n, v2_n = await ingest_message(
            message,
            mt_catalog,
            v2_catalog,
            client=client,
            label=str(title),
            **(ingest_kwargs or {}),
        )
        mt_total += mt_n
        v2_total += v2_n

    log.info(
        "Finished %s — scanned %d messages, +%d MTProto, +%d V2Ray",
        title,
        scanned,
        mt_total,
        v2_total,
    )
    return mt_total, v2_total


async def resolve_sources(client: TelegramClient, sources: list) -> list:
    entities = []
    for source in sources:
        try:
            entity = await client.get_entity(source)
        except Exception as exc:
            log.error("Cannot resolve source %r: %s", source, exc)
            continue
        title = getattr(entity, "title", None) or getattr(entity, "username", None) or source
        entities.append(entity)
        log.info("Watching %s", title)
    return entities


async def watch_sources(
    client: TelegramClient,
    entities: list,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    catalog_lock: asyncio.Lock,
    *,
    ingest_kwargs: dict | None = None,
) -> None:
    @client.on(events.NewMessage(chats=entities))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        message = event.message
        if not isinstance(message, Message):
            return
        chat = await event.get_chat()
        label = getattr(chat, "title", None) or getattr(chat, "username", None) or event.chat_id
        async with catalog_lock:
            await ingest_message(
                message,
                mt_catalog,
                v2_catalog,
                client=client,
                label=str(label),
                **(ingest_kwargs or {}),
            )

    log.info(
        "Listening for new messages in %d source(s) — Ctrl+C to stop",
        len(entities),
    )
    await client.run_until_disconnected()


async def watch_with_reconnect(
    config: ModuleType,
    client: TelegramClient,
    current_proxy: MTProtoProxy | None,
    sources: list,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    catalog_lock: asyncio.Lock,
    conn_state: ConnectionState,
    *,
    ingest_kwargs: dict | None = None,
) -> TelegramClient:
    """Listen for new messages; reconnect via another proxy when the link drops."""
    delay = config_float(getattr(config, "RECONNECT_DELAY", None), 5.0)
    exclude_keys: set[str] = set()
    entities = await resolve_sources(client, sources)
    if not entities:
        log.error("No watchable sources — exiting.")
        return client

    conn_state.client = client
    conn_state.current_proxy = current_proxy

    while True:
        try:
            await watch_sources(
                client, entities, mt_catalog, v2_catalog, catalog_lock, ingest_kwargs=ingest_kwargs
            )
        except asyncio.CancelledError:
            raise

        pending: MTProtoProxy | None = None
        async with conn_state.lock:
            pending = conn_state.pending_fastest
            conn_state.pending_fastest = None

        if pending is not None:
            log.info("Switching to fastest proxy from scheduled check…")
            try:
                client, current_proxy = await switch_to_proxy(
                    config, pending, old_client=client
                )
            except Exception as exc:
                log.error(
                    "Switch to fastest proxy failed: %s — trying another proxy",
                    exc,
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                client, current_proxy = await connect_via_proxy(
                    config, mt_catalog, exclude_keys=exclude_keys
                )

            conn_state.client = client
            conn_state.current_proxy = current_proxy
            entities = await resolve_sources(client, sources)
            if entities:
                continue

            log.error(
                "No watchable sources after proxy switch — retrying in %.0f seconds",
                delay,
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(delay)
            continue

        log.warning("Telegram connection lost — switching proxy…")

        if current_proxy is not None:
            exclude_keys.add(current_proxy.key)
            async with catalog_lock:
                mt_catalog.apply_ping_results(
                    [
                        PingResult(
                            proxy=current_proxy,
                            latency=None,
                            error="connection lost",
                        )
                    ]
                )
            log.info("Marked proxy as failed: %s", current_proxy.to_link())

        try:
            await client.disconnect()
        except Exception:
            pass

        await asyncio.sleep(delay)

        while True:
            try:
                client, current_proxy = await connect_via_proxy(
                    config, mt_catalog, exclude_keys=exclude_keys
                )
            except Exception as exc:
                log.error(
                    "Reconnect failed: %s — retrying in %.0f seconds",
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            conn_state.client = client
            conn_state.current_proxy = current_proxy
            entities = await resolve_sources(client, sources)
            if entities:
                break

            log.error(
                "No watchable sources after reconnect — retrying in %.0f seconds",
                delay,
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(delay)


async def periodic_checks(
    config: ModuleType,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    catalog_lock: asyncio.Lock,
    conn_state: ConnectionState,
    interval: float,
) -> None:
    while True:
        await asyncio.sleep(interval)
        concurrency = getattr(config, "PING_CONCURRENCY", 20)
        timeout = getattr(config, "PING_TIMEOUT", 8.0)
        v2_kwargs = v2ray_test_kwargs(config)
        respect_backoff = bool(getattr(config, "PROBE_RESPECT_BACKOFF", True))
        mt_probe_kw = {"respect_backoff": respect_backoff}
        v2_probe_kw = {"respect_backoff": respect_backoff}

        def _mt_progress(done: int, total_n: int, result) -> None:
            if result.ok and result.latency is not None:
                log.info(
                    "[MTProto %d/%d] OK  %.0f ms  %s",
                    done,
                    total_n,
                    result.latency * 1000,
                    result.proxy.to_link(),
                )
            else:
                log.info("[MTProto %d/%d] FAIL %s", done, total_n, result.proxy.to_link())

        def _v2_progress(done: int, total_n: int, result) -> None:
            if result.ok and result.latency is not None:
                log.info(
                    "[V2Ray %d/%d] OK  %.0f ms  %s://%s:%s",
                    done,
                    total_n,
                    result.latency * 1000,
                    result.server.scheme,
                    result.server.host,
                    result.server.port,
                )
            else:
                log.info(
                    "[V2Ray %d/%d] FAIL %s://%s:%s (%s)",
                    done,
                    total_n,
                    result.server.scheme,
                    result.server.host,
                    result.server.port,
                    result.error or "error",
                )

        try:
            async with catalog_lock:
                log.info(
                    "Scheduled MTProto check (%d in probe queue / %d unique)…",
                    len(mt_catalog.probe_queue(**mt_probe_kw)),
                    len(mt_catalog.all_unique()),
                )
                mt_stats = await check_and_reorganize(
                    mt_catalog,
                    concurrency=concurrency,
                    timeout=timeout,
                    on_result=_mt_progress,
                    **mt_probe_kw,
                )
                log.info(
                    "MTProto: %d working / %d failed",
                    mt_stats.ok,
                    mt_stats.failed,
                )

                log.info(
                    "Scheduled V2Ray check (%d in probe queue / %d unique) via %s …",
                    len(v2_catalog.probe_queue(**v2_probe_kw)),
                    len(v2_catalog.all_unique()),
                    v2_kwargs["test_url"],
                )
                if not v2_kwargs["xray_bin"]:
                    log.warning(
                        "Skipping V2Ray check — Xray binary not found "
                        "(set xray.bin in config.yaml, install xray on PATH, or run setup to install it in xray/)"
                    )
                else:
                    v2_stats = await check_and_reorganize_v2ray(
                        v2_catalog,
                        on_result=_v2_progress,
                        **v2_kwargs,
                        **v2_probe_kw,
                    )
                    v2_ok, v2_fail = v2_catalog.counts()
                    log.info(
                        "V2Ray: %d working / %d failed "
                        "(this run: +ok=%d fail=%d)",
                        v2_ok,
                        v2_fail,
                        v2_stats.ok,
                        v2_stats.failed,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Scheduled check failed: %s", exc)
            continue

        await _request_fastest_proxy_switch(config, mt_catalog, conn_state)


async def ensure_authorized(client: TelegramClient) -> None:
    if await client.is_user_authorized():
        me = await client.get_me()
        log.info("Logged in as %s (id=%s)", me.username or me.first_name, me.id)
        return

    phone = input("Phone number (international format, e.g. +1234567890): ").strip()
    await client.send_code_request(phone)
    code = input("Login code: ").strip()
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        password = input("2FA password: ").strip()
        await client.sign_in(password=password)

    me = await client.get_me()
    log.info("Logged in as %s (id=%s)", me.username or me.first_name, me.id)


async def run_scraper(config: ModuleType) -> None:
    patch_telethon_faketls()
    db, mt_catalog, v2_catalog = open_catalogs(config)
    catalog_lock = asyncio.Lock()

    v2_ok, v2_fail = v2_catalog.counts()
    log.info(
        "Loaded MTProto %d working / %d failed from %s",
        len(mt_catalog.working),
        len(mt_catalog.failed),
        db.path.name,
    )
    log.info(
        "Loaded V2Ray %d working / %d failed from %s",
        v2_ok,
        v2_fail,
        db.path.name,
    )

    client, current_proxy = await connect_via_proxy(config, mt_catalog)
    conn_state = ConnectionState(client=client, current_proxy=current_proxy)
    check_task: asyncio.Task | None = None
    try:
        await ensure_authorized(client)

        sources = getattr(config, "SOURCES", [])
        if not sources:
            log.error("config.SOURCES is empty — add channels/groups to scrape.")
            return

        limit = getattr(config, "MESSAGES_PER_SOURCE", 500)
        ingest_kwargs = ingest_subscription_kwargs(config)
        mt_new = 0
        v2_new = 0
        for source in sources:
            mt_n, v2_n = await scrape_source(
                client,
                source,
                limit,
                mt_catalog,
                v2_catalog,
                ingest_kwargs=ingest_kwargs,
            )
            mt_new += mt_n
            v2_new += v2_n

        v2_ok, v2_fail = v2_catalog.counts()
        log.info(
            "Initial scan done. +%d MTProto, +%d V2Ray; "
            "MTProto %d/%d; V2Ray %d/%d",
            mt_new,
            v2_new,
            len(mt_catalog.working),
            len(mt_catalog.failed),
            v2_ok,
            v2_fail,
        )

        entities = await resolve_sources(client, sources)
        if not entities:
            log.error("No watchable sources — exiting.")
            return

        interval = config_float(getattr(config, "PROXY_CHECK_INTERVAL", None), 1800)
        if interval > 0:
            check_task = asyncio.create_task(
                periodic_checks(
                    config, mt_catalog, v2_catalog, catalog_lock, conn_state, interval
                )
            )
            log.info("Proxy / V2Ray re-check scheduled every %.0f seconds", interval)

        client = await watch_with_reconnect(
            config,
            client,
            current_proxy,
            sources,
            mt_catalog,
            v2_catalog,
            catalog_lock,
            conn_state,
            ingest_kwargs=ingest_kwargs,
        )
    finally:
        if check_task is not None:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass
        await client.disconnect()
        db.close()
