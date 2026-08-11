"""Fetch V2Ray servers from configured HTTP(S) URL sources (CLI)."""

from __future__ import annotations

import asyncio
import logging

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.logging_setup import setup_logging
from fetch_mtproto.v2ray.url_sources import (
    ingest_url_sources,
    url_source_settings,
)

log = logging.getLogger("mtproto-scraper")


async def _run() -> None:
    config = load_config()
    settings = url_source_settings(config)
    if not settings.enabled:
        log.info("url_sources.enabled is false — nothing to do.")
        return

    db, _mt, v2_catalog = open_catalogs(config)
    try:
        while True:
            await ingest_url_sources(v2_catalog, settings)
            if settings.fetch_interval <= 0:
                break
            log.info(
                "Next URL source fetch in %.0f seconds (Ctrl+C to stop)",
                settings.fetch_interval,
            )
            await asyncio.sleep(settings.fetch_interval)
    finally:
        db.close()


def main() -> None:
    setup_logging()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("Interrupted.")


if __name__ == "__main__":
    main()
