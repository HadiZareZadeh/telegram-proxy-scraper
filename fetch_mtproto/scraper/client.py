"""Telegram client connection helpers (direct + MTProto proxy)."""

from __future__ import annotations

import asyncio
import logging
from types import ModuleType

from telethon import TelegramClient, connection
from telethon.errors import AuthKeyDuplicatedError
from telethon.tl.functions.help import GetConfigRequest

from fetch_mtproto.config_loader import config_float, resolve_max_working
from fetch_mtproto.mtproto.ping import find_first_working_proxy
from fetch_mtproto.mtproto.store import MTProtoProxy, ProxyCatalog
from fetch_mtproto.paths import session_path

try:
    import TelethonFakeTLS
except ImportError:
    TelethonFakeTLS = None  # type: ignore

log = logging.getLogger("mtproto-scraper")


def _session_path(name: str) -> str:
    return str(session_path(name))


def _connection_for_proxy(proxy: MTProtoProxy):
    if proxy.is_fake_tls:
        if TelethonFakeTLS is None:
            raise RuntimeError(
                "Proxy uses Fake TLS (ee… secret). Install: pip install TelethonFakeTLS"
            )
        return TelethonFakeTLS.ConnectionTcpMTProxyFakeTLS
    return connection.ConnectionTcpMTProxyRandomizedIntermediate


def make_client(
    config: ModuleType,
    session: str,
    proxy: MTProtoProxy | None = None,
    *,
    connection_retries: int = 2,
) -> TelegramClient:
    kwargs: dict = {
        "session": _session_path(session),
        "api_id": config.API_ID,
        "api_hash": config.API_HASH,
        "connection_retries": connection_retries,
        "retry_delay": 1,
    }
    if proxy is not None:
        kwargs["connection"] = _connection_for_proxy(proxy)
        kwargs["proxy"] = proxy.as_telethon_tuple()
    return TelegramClient(**kwargs)


async def _probe_telegram(client: TelegramClient, timeout: float) -> None:
    await asyncio.wait_for(client(GetConfigRequest()), timeout=timeout)


async def try_connect(
    client: TelegramClient, label: str, *, timeout: float
) -> tuple[bool, BaseException | None]:
    try:
        await client.connect()
        if not client.is_connected():
            return False, None
        await _probe_telegram(client, timeout)
        log.info("Connected via %s", label)
        return True, None
    except Exception as exc:
        log.warning("Connect via %s failed: %s", label, exc)
        try:
            await client.disconnect()
        except Exception:
            pass
        return False, exc


async def connect_direct(config: ModuleType) -> TelegramClient:
    """Connect to Telegram without a proxy."""
    log.info("Connecting without proxy…")
    timeout = config_float(getattr(config, "PING_TIMEOUT", None), 8.0)
    client = make_client(config, config.SESSION_NAME, proxy=None)
    ok, _err = await try_connect(client, "direct (no proxy)", timeout=timeout)
    if not ok:
        raise RuntimeError(
            "Direct connection failed. Check your network, or add working "
            "tg://proxy?... links (they are stored in data/catalog.db)."
        )
    return client


def _proxy_candidates(
    catalog: ProxyCatalog,
    config: ModuleType,
    exclude_keys: set[str] | None = None,
) -> list[MTProtoProxy]:
    seen: set[str] = set()
    candidates: list[MTProtoProxy] = []
    working = catalog.working.all()
    max_working = resolve_max_working(getattr(config, "MTPROTO_MAX_WORKING", 0))
    if max_working is not None:
        working = working[:max_working]
    skip = exclude_keys or set()
    for proxy in (*working, *catalog.failed.all()):
        if proxy.key in seen or proxy.key in skip:
            continue
        seen.add(proxy.key)
        candidates.append(proxy)
    return candidates


async def connect_via_proxy(
    config: ModuleType,
    catalog: ProxyCatalog,
    *,
    exclude_keys: set[str] | None = None,
) -> tuple[TelegramClient, MTProtoProxy | None]:
    """Connect via the first working proxy; fall back to direct if none work.

    Returns (client, proxy) where proxy is None when connected directly.
    """
    timeout = config_float(getattr(config, "PING_TIMEOUT", None), 8.0)
    skip = set(exclude_keys or ())
    candidates = _proxy_candidates(catalog, config, skip)

    if not candidates and skip:
        log.info(
            "All %d proxy candidate(s) excluded this session — retrying full list.",
            len(skip),
        )
        skip.clear()
        candidates = _proxy_candidates(catalog, config, skip)

    if not candidates:
        log.warning(
            "No MTProto proxies in database — falling back to direct connection."
        )
        return await connect_direct(config), None

    log.info("Looking for a working proxy (%d candidate(s))…", len(candidates))

    def _progress(done: int, total: int, result) -> None:
        if result.ok and result.latency is not None:
            log.info(
                "[%d/%d] OK  %s  %.0f ms",
                done,
                total,
                result.proxy.to_link(),
                result.latency * 1000,
            )
        else:
            log.info("[%d/%d] FAIL %s", done, total, result.proxy.to_link())

    found = await find_first_working_proxy(
        candidates,
        timeout=timeout,
        on_result=_progress,
    )
    if found is None:
        log.warning("None of the stored MTProto proxies responded — trying direct.")
        return await connect_direct(config), None

    proxy, latency = found
    log.info("Using first working proxy (%.0f ms): %s", latency * 1000, proxy.to_link())

    client = make_client(config, config.SESSION_NAME, proxy=proxy)
    ok, err = await try_connect(
        client, f"proxy {proxy.server}:{proxy.port}", timeout=timeout
    )
    if not ok:
        if isinstance(err, AuthKeyDuplicatedError):
            raise RuntimeError(
                "Telegram session invalidated (used from two IPs at once). "
                f"Delete sessions/{config.SESSION_NAME}.session and log in again."
            ) from err
        log.warning("Chosen proxy failed to connect — trying direct.")
        return await connect_direct(config), None
    return client, proxy
