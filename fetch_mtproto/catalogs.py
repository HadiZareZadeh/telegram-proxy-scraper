"""Open the shared catalog DB and optionally import legacy text files once."""

from __future__ import annotations

import logging
from pathlib import Path

from fetch_mtproto.config_loader import resolve_max_working, resolve_subscription_limit
from fetch_mtproto.db import CatalogDB
from fetch_mtproto.mtproto.store import ProxyCatalog, load_mtproto_from_text_file
from fetch_mtproto.paths import PROJECT_ROOT
from fetch_mtproto.v2ray.store import V2RAY_SCHEMES, V2RayCatalog, load_v2ray_from_text_file

log = logging.getLogger("mtproto-scraper")

LEGACY_MIGRATED_KEY = "legacy_txt_migrated"


def database_path(config_module=None) -> Path:
    configured = "data/catalog.db"
    if config_module is not None:
        configured = getattr(config_module, "DATABASE_FILE", configured)
    return PROJECT_ROOT / configured


def subscription_path(config_module=None) -> Path:
    configured = "data/subscription.txt"
    if config_module is not None:
        configured = getattr(config_module, "SUBSCRIPTION_FILE", configured)
    return PROJECT_ROOT / configured


def _legacy_paths(config_module=None) -> tuple[Path, Path, Path]:
    mt_working = "data/mtproto/proxies.txt"
    mt_failed = "data/mtproto/proxies_failed.txt"
    v2_dir = "data/v2ray"
    if config_module is not None:
        mt_working = getattr(config_module, "PROXIES_FILE", mt_working)
        mt_failed = getattr(config_module, "FAILED_PROXIES_FILE", mt_failed)
        v2_dir = getattr(config_module, "V2RAY_DIR", v2_dir)
    return (
        PROJECT_ROOT / mt_working,
        PROJECT_ROOT / mt_failed,
        PROJECT_ROOT / v2_dir,
    )


def migrate_legacy_text_files(db: CatalogDB, config_module=None) -> None:
    """Import old *.txt catalogs into SQLite once (if the DB tables are empty)."""
    if db.get_meta(LEGACY_MIGRATED_KEY) == "1":
        return
    if db.mtproto_count() > 0 or db.v2ray_count() > 0:
        db.set_meta(LEGACY_MIGRATED_KEY, "1")
        return

    mt_working_path, mt_failed_path, v2_dir = _legacy_paths(config_module)
    mt_ok = load_mtproto_from_text_file(mt_working_path)
    mt_fail = load_mtproto_from_text_file(mt_failed_path)
    # Drop failed keys that also appear as working
    ok_keys = {p.key for p in mt_ok}
    mt_fail = [p for p in mt_fail if p.key not in ok_keys]
    if mt_ok or mt_fail:
        db.mtproto_reorganize(
            [p.as_db_row() for p in mt_ok],
            [p.as_db_row() for p in mt_fail],
        )
        log.info(
            "Migrated MTProto from text files: %d working / %d failed",
            len(mt_ok),
            len(mt_fail),
        )

    v2_ok: list = []
    v2_fail: list = []
    seen_ok: set[str] = set()
    seen_fail: set[str] = set()
    for scheme in V2RAY_SCHEMES:
        for server in load_v2ray_from_text_file(v2_dir / f"{scheme}.txt"):
            if server.key not in seen_ok:
                seen_ok.add(server.key)
                v2_ok.append(server)
        for server in load_v2ray_from_text_file(v2_dir / f"{scheme}_failed.txt"):
            if server.key in seen_ok or server.key in seen_fail:
                continue
            seen_fail.add(server.key)
            v2_fail.append(server)
    if v2_ok or v2_fail:
        db.v2ray_reorganize(
            [s.as_db_row() for s in v2_ok],
            [s.as_db_row() for s in v2_fail],
        )
        log.info(
            "Migrated V2Ray from text files: %d working / %d failed",
            len(v2_ok),
            len(v2_fail),
        )

    db.set_meta(LEGACY_MIGRATED_KEY, "1")


def open_catalogs(config_module=None) -> tuple[CatalogDB, ProxyCatalog, V2RayCatalog]:
    db = CatalogDB(database_path(config_module))
    migrate_legacy_text_files(db, config_module)
    mt_max = None
    v2_max = None
    v2_sub_limit = 100
    if config_module is not None:
        mt_max = resolve_max_working(getattr(config_module, "MTPROTO_MAX_WORKING", 0))
        v2_max = resolve_max_working(getattr(config_module, "V2RAY_MAX_WORKING", 0))
        v2_sub_limit = resolve_subscription_limit(
            getattr(config_module, "V2RAY_SUBSCRIPTION_LIMIT", None)
        )
    mt_catalog = ProxyCatalog(db, max_working=mt_max)
    v2_catalog = V2RayCatalog(
        db,
        subscription_path=subscription_path(config_module),
        max_working=v2_max,
        subscription_limit=v2_sub_limit,
    )
    return db, mt_catalog, v2_catalog
