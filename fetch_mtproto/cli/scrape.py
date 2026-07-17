"""Scrape Telegram sources and keep catalogs fresh (CLI)."""

from __future__ import annotations

import asyncio
import logging

from fetch_mtproto.config_loader import load_config
from fetch_mtproto.scraper.app import run_scraper


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    config = load_config()
    try:
        asyncio.run(run_scraper(config))
    except KeyboardInterrupt:
        logging.getLogger("mtproto-scraper").info("Interrupted.")


if __name__ == "__main__":
    main()
