"""Fetch V2Ray share links from a list of HTTP(S) URLs (e.g. GitHub raw files)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from urllib.parse import urlparse

from fetch_mtproto.config_loader import config_bool, config_float, config_int
from fetch_mtproto.paths import PROJECT_ROOT
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer
from fetch_mtproto.v2ray.subscription_import import (
    expand_subscription_payload,
    fetch_subscription_url,
)

log = logging.getLogger("mtproto-scraper")

DEFAULT_URLS_FILE = "urls.txt"
DEFAULT_FETCH_INTERVAL = 3600.0
DEFAULT_FETCH_TIMEOUT = 30.0
DEFAULT_CONCURRENCY = 5


@dataclass(frozen=True, slots=True)
class UrlSourceSettings:
    enabled: bool = True
    urls_file: Path = Path(DEFAULT_URLS_FILE)
    fetch_interval: float = DEFAULT_FETCH_INTERVAL
    fetch_timeout: float = DEFAULT_FETCH_TIMEOUT
    concurrency: int = DEFAULT_CONCURRENCY


@dataclass(frozen=True, slots=True)
class UrlFetchStats:
    urls_total: int
    urls_ok: int
    urls_failed: int
    servers_found: int
    servers_added: int


def url_source_settings(config: ModuleType | None = None) -> UrlSourceSettings:
    """Resolve url_sources.* settings from config (with safe defaults)."""
    enabled = True
    urls_file = DEFAULT_URLS_FILE
    interval = DEFAULT_FETCH_INTERVAL
    timeout = DEFAULT_FETCH_TIMEOUT
    concurrency = DEFAULT_CONCURRENCY
    if config is not None:
        enabled = config_bool(getattr(config, "URL_SOURCES_ENABLED", True), True)
        raw_file = getattr(config, "URL_SOURCES_FILE", None)
        if raw_file:
            urls_file = str(raw_file).strip() or DEFAULT_URLS_FILE
        interval = config_float(
            getattr(config, "URL_SOURCES_FETCH_INTERVAL", None),
            DEFAULT_FETCH_INTERVAL,
        )
        timeout = config_float(
            getattr(config, "URL_SOURCES_FETCH_TIMEOUT", None),
            DEFAULT_FETCH_TIMEOUT,
        )
        concurrency = config_int(
            getattr(config, "URL_SOURCES_CONCURRENCY", None),
            DEFAULT_CONCURRENCY,
            minimum=1,
            maximum=32,
        )
    path = Path(urls_file)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return UrlSourceSettings(
        enabled=enabled,
        urls_file=path,
        fetch_interval=max(0.0, interval),
        fetch_timeout=max(1.0, timeout),
        concurrency=concurrency,
    )


def load_url_list(path: Path) -> list[str]:
    """Load unique HTTP(S) URLs from a text file (one per line; # comments allowed)."""
    if not path.is_file():
        return []
    seen: set[str] = set()
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        # Allow optional "name = url" style; take the last http(s) token.
        if "://" not in text and "=" in text:
            text = text.split("=", 1)[1].strip()
        if not text.startswith(("http://", "https://")):
            continue
        # Strip trailing junk from copy-paste
        text = text.rstrip(").,>;']\"`}")
        parsed = urlparse(text)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        if text in seen:
            continue
        seen.add(text)
        urls.append(text)
    return urls


async def _fetch_one(
    url: str,
    *,
    timeout: float,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[V2RayServer] | None, str | None]:
    async with semaphore:
        try:
            body = await fetch_subscription_url(url, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 — per-URL isolation
            return url, None, str(exc)
        if not body:
            return url, None, "empty or unreachable"
        servers = expand_subscription_payload(body)
        return url, servers, None


async def fetch_servers_from_urls(
    urls: list[str],
    *,
    timeout: float = DEFAULT_FETCH_TIMEOUT,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> tuple[dict[str, V2RayServer], int, int]:
    """
    Fetch many subscription URLs and merge unique V2Ray servers.

    Returns (servers_by_key, urls_ok, urls_failed).
    """
    if not urls:
        return {}, 0, 0
    semaphore = asyncio.Semaphore(max(1, concurrency))
    tasks = [
        asyncio.create_task(_fetch_one(url, timeout=timeout, semaphore=semaphore))
        for url in urls
    ]
    found: dict[str, V2RayServer] = {}
    ok = 0
    failed = 0
    for coro in asyncio.as_completed(tasks):
        url, servers, error = await coro
        if servers is None:
            failed += 1
            log.warning("URL source failed %s: %s", url, error or "unknown")
            continue
        ok += 1
        for server in servers:
            found[server.key] = server
        log.info("URL source OK %s — %d importable server(s)", url, len(servers))
    return found, ok, failed


async def ingest_url_sources(
    catalog: V2RayCatalog,
    settings: UrlSourceSettings | None = None,
    *,
    config: ModuleType | None = None,
) -> UrlFetchStats:
    """Load urls file, fetch all sources, upsert into the V2Ray catalog."""
    cfg = settings or url_source_settings(config)
    urls = load_url_list(cfg.urls_file)
    if not urls:
        log.warning(
            "No URL sources found in %s — add one HTTP(S) URL per line",
            cfg.urls_file,
        )
        return UrlFetchStats(0, 0, 0, 0, 0)

    log.info(
        "Fetching %d URL source(s) from %s (concurrency=%d)…",
        len(urls),
        cfg.urls_file.name,
        cfg.concurrency,
    )
    found, urls_ok, urls_failed = await fetch_servers_from_urls(
        urls,
        timeout=cfg.fetch_timeout,
        concurrency=cfg.concurrency,
    )
    # Insert only unknown keys; do not revive known-failed or trim max_working yet.
    added = catalog.add_new(found.values()) if found else 0
    v2_ok, v2_fail = catalog.counts()
    log.info(
        "URL sources done: %d/%d URLs OK, %d unique server(s), +%d new; "
        "catalog %d working / %d failed",
        urls_ok,
        len(urls),
        len(found),
        added,
        v2_ok,
        v2_fail,
    )
    return UrlFetchStats(
        urls_total=len(urls),
        urls_ok=urls_ok,
        urls_failed=urls_failed,
        servers_found=len(found),
        servers_added=added,
    )


async def periodic_url_source_fetch(
    catalog: V2RayCatalog,
    catalog_lock: asyncio.Lock,
    settings: UrlSourceSettings,
) -> None:
    """Fetch URL sources on an interval (0 = disabled after the caller’s initial run)."""
    interval = settings.fetch_interval
    if interval <= 0:
        return
    while True:
        await asyncio.sleep(interval)
        try:
            async with catalog_lock:
                await ingest_url_sources(catalog, settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Scheduled URL source fetch failed: %s", exc)
