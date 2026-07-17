"""Extract and store MTProto / V2Ray links from Telegram messages."""

from __future__ import annotations

import logging

from telethon.tl.custom.message import Message
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

from fetch_mtproto.mtproto.store import ProxyCatalog, extract_proxies_from_text
from fetch_mtproto.v2ray.store import V2RayCatalog, extract_v2ray_from_text

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


def ingest_message(
    message: Message,
    mt_catalog: ProxyCatalog,
    v2_catalog: V2RayCatalog,
    *,
    label: str | None = None,
) -> tuple[int, int]:
    blob = "\n".join(message_text_parts(message))
    where = f" ({label})" if label else ""

    mt_added = mt_catalog.add(extract_proxies_from_text(blob))
    if mt_added:
        log.info("  +%d MTProto from message %s%s", mt_added, message.id, where)

    v2_added = v2_catalog.add(extract_v2ray_from_text(blob))
    if v2_added:
        log.info("  +%d V2Ray from message %s%s", v2_added, message.id, where)

    return mt_added, v2_added
