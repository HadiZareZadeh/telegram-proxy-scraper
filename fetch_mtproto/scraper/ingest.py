"""Extract and store MTProto / V2Ray links from Telegram messages."""

from __future__ import annotations

import logging

from telethon import TelegramClient
from telethon.tl.custom.message import Message
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

from fetch_mtproto.mtproto.store import ProxyCatalog, extract_proxies_from_text
from fetch_mtproto.scraper.attachments import download_napsternet_attachments
from fetch_mtproto.v2ray.napsternet import extract_v2ray_from_napsternet_file
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer, extract_v2ray_from_text
from fetch_mtproto.v2ray.subscription_import import expand_subscriptions_from_text

log = logging.getLogger("mtproto-scraper")


def message_text_parts(message: Message) -> list[str]:
    parts: list[str] = []
    if message.message:
        parts.append(message.message)
    if message.entities:
        for ent in message.entities:
            if isinstance(ent, MessageEntityTextUrl) and ent.url:
                parts.append(ent.url)
            elif isinstance(ent, MessageEntityUrl) and message.message:
                url = message.message[ent.offset : ent.offset + ent.length]
                parts.append(url)
    markup = getattr(message, "reply_markup", None)
    if markup and getattr(markup, "rows", None):
        for row in markup.rows:
            for button in row.buttons:
                url = getattr(button, "url", None)
                if url:
                    parts.append(url)
    return parts


def _merge_v2ray_servers(*groups: list[V2RayServer]) -> list[V2RayServer]:
    merged: dict[str, V2RayServer] = {}
    for group in groups:
        for server in group:
            merged[server.key] = server
    return list(merged.values())


async def ingest_message(
    message: Message,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    *,
    client: TelegramClient | None = None,
    label: str | None = None,
    expand_subscriptions: bool = True,
    subscription_fetch_timeout: float = 15.0,
    subscription_max_urls: int = 5,
    parse_napsternet_attachments: bool = True,
) -> tuple[int, int]:
    blob = "\n".join(message_text_parts(message))
    where = f" ({label})" if label else ""

    mt_added = mt_catalog.add(extract_proxies_from_text(blob))
    if mt_added:
        log.info("  +%d MTProto from message %s%s", mt_added, message.id, where)

    direct = extract_v2ray_from_text(blob)
    from_subs: list[V2RayServer] = []
    if expand_subscriptions:
        from_subs = await expand_subscriptions_from_text(
            blob,
            fetch_urls=True,
            timeout=subscription_fetch_timeout,
            max_urls=subscription_max_urls,
        )
        direct_keys = {server.key for server in direct}
        sub_new = sum(1 for server in from_subs if server.key not in direct_keys)
        if sub_new:
            log.info(
                "  +%d V2Ray from subscription content in message %s%s",
                sub_new,
                message.id,
                where,
            )

    from_napsternet: list[V2RayServer] = []
    if parse_napsternet_attachments and client is not None:
        for filename, raw in await download_napsternet_attachments(client, message):
            servers = extract_v2ray_from_napsternet_file(raw, filename=filename)
            if servers:
                from_napsternet.extend(servers)
                log.info(
                    "  +%d V2Ray from Napsternet file %s in message %s%s",
                    len(servers),
                    filename,
                    message.id,
                    where,
                )

    v2_added = v2_catalog.add(
        _merge_v2ray_servers(direct, from_subs, from_napsternet)
    )
    if v2_added:
        log.info("  +%d V2Ray from message %s%s", v2_added, message.id, where)

    return mt_added, v2_added
