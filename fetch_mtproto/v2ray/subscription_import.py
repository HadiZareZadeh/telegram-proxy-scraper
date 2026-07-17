"""Expand V2Ray subscription URLs and inline Base64 blobs into share links."""

from __future__ import annotations

import asyncio
import logging
import re
import urllib.error
import urllib.request
from urllib.parse import urlparse

from fetch_mtproto.v2ray.store import (
    V2RAY_SCHEMES,
    V2RayServer,
    _b64decode,
    _V2RAY_URL_RE,
    extract_v2ray_from_text,
)

log = logging.getLogger("mtproto-scraper")

_HTTP_URL_RE = re.compile(r"https?://[^\s<>\"'`]+", re.IGNORECASE)
_B64_BLOB_RE = re.compile(r"[A-Za-z0-9+/_=-]{40,}")
_USER_AGENT = "telegram-proxy-scraper/1.0"

_SKIP_FETCH_HOSTS = frozenset(
    {
        "t.me",
        "telegram.me",
        "telegra.ph",
        "youtube.com",
        "www.youtube.com",
        "youtu.be",
        "twitter.com",
        "www.twitter.com",
        "x.com",
        "instagram.com",
        "www.instagram.com",
        "facebook.com",
        "www.facebook.com",
        "play.google.com",
        "apps.apple.com",
    }
)

_MAX_BLOB_CHARS = 500_000
_MAX_DECODED_BYTES = 2_000_000


def _trim_token(raw: str) -> str:
    return raw.strip().rstrip(").,>;']\"`}")


def _text_has_share_links(text: str) -> bool:
    lower = text.lower()
    return any(f"{scheme}://" in lower for scheme in V2RAY_SCHEMES)


def _should_fetch_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    return host not in _SKIP_FETCH_HOSTS


def normalize_subscription_payload(raw: str) -> str:
    """Turn a subscription HTTP body or decoded blob into plain text for link extraction."""
    text = raw.strip()
    if not text:
        return ""
    if _text_has_share_links(text):
        return text

    compact = re.sub(r"\s+", "", text)
    if len(compact) >= 40:
        try:
            decoded = _b64decode(compact).decode("utf-8", errors="strict")
        except (UnicodeDecodeError, ValueError):
            decoded = ""
        else:
            if decoded and _text_has_share_links(decoded):
                return decoded
            if decoded and decoded.strip():
                return decoded
    return text


def find_http_subscription_urls(text: str) -> list[str]:
    """HTTP(S) URLs that may point at a V2Ray subscription (not Telegram / social links)."""
    seen: set[str] = set()
    urls: list[str] = []
    for match in _HTTP_URL_RE.finditer(text):
        url = _trim_token(match.group(0))
        if not url or url in seen or not _should_fetch_url(url):
            continue
        seen.add(url)
        urls.append(url)
    return urls


def find_inline_base64_blobs(text: str) -> list[str]:
    """Long Base64 tokens in message text, excluding bodies of share-link URIs."""
    scrubbed = _V2RAY_URL_RE.sub(" ", text)
    seen: set[str] = set()
    blobs: list[str] = []
    for match in _B64_BLOB_RE.finditer(scrubbed):
        blob = _trim_token(match.group(0))
        if len(blob) < 40 or len(blob) > _MAX_BLOB_CHARS or blob in seen:
            continue
        seen.add(blob)
        blobs.append(blob)
    return blobs


def expand_subscription_payload(payload: str) -> list[V2RayServer]:
    """Extract individual V2Ray servers from subscription body or decoded blob text."""
    normalized = normalize_subscription_payload(payload)
    if not normalized:
        return []
    return extract_v2ray_from_text(normalized)


def _decode_inline_blob(blob: str) -> str | None:
    try:
        raw = _b64decode(blob)
    except ValueError:
        return None
    if len(raw) > _MAX_DECODED_BYTES:
        return None
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None


def expand_inline_base64_blobs(text: str) -> list[V2RayServer]:
    found: dict[str, V2RayServer] = {}
    for blob in find_inline_base64_blobs(text):
        decoded = _decode_inline_blob(blob)
        if decoded is None:
            continue
        for server in expand_subscription_payload(decoded):
            found[server.key] = server
    return list(found.values())


def _fetch_subscription_url_sync(url: str, *, timeout: float) -> str | None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": _USER_AGENT, "Accept": "*/*"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(_MAX_DECODED_BYTES + 1)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise exc
    if len(body) > _MAX_DECODED_BYTES:
        log.debug("Subscription body too large from %s", url)
        return None
    return body.decode("utf-8", errors="replace")


async def fetch_subscription_url(url: str, *, timeout: float = 15.0) -> str | None:
    try:
        return await asyncio.to_thread(
            _fetch_subscription_url_sync, url, timeout=timeout
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        log.debug("Subscription fetch failed %s: %s", url, exc)
        return None


async def expand_subscriptions_from_text(
    text: str,
    *,
    fetch_urls: bool = True,
    timeout: float = 15.0,
    max_urls: int = 5,
) -> list[V2RayServer]:
    """Fetch HTTP subs and decode inline Base64; return servers only (not sub URLs)."""
    found: dict[str, V2RayServer] = {}

    for server in expand_inline_base64_blobs(text):
        found[server.key] = server

    if fetch_urls and max_urls > 0:
        for url in find_http_subscription_urls(text)[:max_urls]:
            body = await fetch_subscription_url(url, timeout=timeout)
            if not body:
                continue
            for server in expand_subscription_payload(body):
                found[server.key] = server

    return list(found.values())
