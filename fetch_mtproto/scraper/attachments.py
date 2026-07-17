"""Download Napsternet config attachments from Telegram messages."""

from __future__ import annotations

import logging
from io import BytesIO

from telethon import TelegramClient
from telethon.tl.custom.message import Message
from telethon.tl.types import DocumentAttributeFilename

from fetch_mtproto.v2ray.napsternet import NAPSTERNET_EXTENSIONS, is_napsternet_filename

log = logging.getLogger("mtproto-scraper")

_MAX_ATTACHMENT_BYTES = 2_000_000


def napsternet_attachment_name(message: Message) -> str | None:
    document = message.document
    if document is None:
        return None
    for attribute in document.attributes:
        if isinstance(attribute, DocumentAttributeFilename):
            name = (attribute.file_name or "").strip()
            if name:
                return name
    mime = (document.mime_type or "").lower()
    if "napsternet" in mime or mime.endswith("npv4"):
        return f"config.npv4"
    return None


def message_has_napsternet_attachment(message: Message) -> bool:
    name = napsternet_attachment_name(message)
    return bool(name and is_napsternet_filename(name))


async def download_napsternet_attachments(
    client: TelegramClient,
    message: Message,
) -> list[tuple[str, bytes]]:
    """Return (filename, raw bytes) for Napsternet documents on a message."""
    name = napsternet_attachment_name(message)
    if not name or not is_napsternet_filename(name):
        return []

    document = message.document
    if document is None:
        return []
    size = int(getattr(document, "size", 0) or 0)
    if size > _MAX_ATTACHMENT_BYTES:
        log.debug("Skipping large Napsternet attachment %s (%d bytes)", name, size)
        return []

    buffer = BytesIO()
    try:
        result = await client.download_media(message, file=buffer)
    except Exception as exc:
        log.debug("Failed to download Napsternet attachment %s: %s", name, exc)
        return []
    if result is None:
        return []

    data = buffer.getvalue()
    if not data or len(data) > _MAX_ATTACHMENT_BYTES:
        return []
    return [(name, data)]
