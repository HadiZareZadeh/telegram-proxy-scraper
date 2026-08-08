"""Scrape Telegram sources and keep catalogs fresh (CLI)."""

from __future__ import annotations

import asyncio

from fetch_mtproto.config_loader import load_config
from fetch_mtproto.logging_setup import setup_logging
from fetch_mtproto.scraper.app import run_scraper


def main() -> None:
    log = setup_logging()
    config = load_config()
    try:
        asyncio.run(run_scraper(config))
    except KeyboardInterrupt:
        log.info("Interrupted.")


if __name__ == "__main__":
    main()
