"""Prune dead catalog entries and cap per-run failed probes."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from types import ModuleType

from fetch_mtproto.config_loader import config_bool, config_int
from fetch_mtproto.db import CatalogDB

log = logging.getLogger("mtproto-scraper")


@dataclass(frozen=True, slots=True)
class PruneSettings:
    """When to delete failed servers and how many to re-probe each run."""

    after_failures: int = 8
    min_checks: int = 5
    stale_days: int = 14
    max_failed: int = 2000
    probe_failed_limit: int = 500
    prune_incompatible_v2ray: bool = True

    @property
    def enabled(self) -> bool:
        return (
            (self.after_failures > 0 and self.min_checks > 0)
            or self.stale_days > 0
            or self.max_failed > 0
            or self.prune_incompatible_v2ray
        )


def prune_settings_from_config(config: ModuleType | None) -> PruneSettings:
    if config is None:
        return PruneSettings()
    return PruneSettings(
        after_failures=config_int(
            getattr(config, "PROBE_PRUNE_AFTER_FAILURES", None), 8, minimum=0
        ),
        min_checks=config_int(
            getattr(config, "PROBE_PRUNE_MIN_CHECKS", None), 5, minimum=0
        ),
        stale_days=config_int(
            getattr(config, "PROBE_PRUNE_STALE_DAYS", None), 14, minimum=0
        ),
        max_failed=config_int(
            getattr(config, "PROBE_MAX_FAILED", None), 2000, minimum=0
        ),
        probe_failed_limit=config_int(
            getattr(config, "PROBE_FAILED_LIMIT", None), 500, minimum=0
        ),
        prune_incompatible_v2ray=config_bool(
            getattr(config, "PROBE_PRUNE_INCOMPATIBLE_V2RAY", None), True
        ),
    )


def probe_kwargs_from_config(config: ModuleType | None) -> dict:
    settings = prune_settings_from_config(config)
    return {
        "respect_backoff": config_bool(
            getattr(config, "PROBE_RESPECT_BACKOFF", None), True
        ),
        "failed_limit": settings.probe_failed_limit or None,
    }


def prune_mtproto(db: CatalogDB, settings: PruneSettings) -> dict[str, int]:
    if not settings.enabled:
        return {"chronic": 0, "stale": 0, "cap": 0, "total": 0}
    stats = db.mtproto_prune(
        after_failures=settings.after_failures,
        min_checks=settings.min_checks,
        stale_days=settings.stale_days,
        max_failed=settings.max_failed,
    )
    total = int(stats["total"])
    if total:
        log.info(
            "Pruned %d MTProto server(s): %d chronic, %d stale, %d over cap",
            total,
            stats["chronic"],
            stats["stale"],
            stats["cap"],
        )
    return stats


def prune_v2ray(db: CatalogDB, settings: PruneSettings) -> dict[str, int]:
    if not settings.enabled and not settings.prune_incompatible_v2ray:
        return {
            "chronic": 0,
            "stale": 0,
            "cap": 0,
            "incompatible": 0,
            "total": 0,
        }
    incompatible = ()
    if settings.prune_incompatible_v2ray:
        from fetch_mtproto.v2ray.store import NEKORAY_INCOMPATIBLE_NETWORKS

        incompatible = tuple(sorted(NEKORAY_INCOMPATIBLE_NETWORKS))
    stats = db.v2ray_prune(
        after_failures=settings.after_failures,
        min_checks=settings.min_checks,
        stale_days=settings.stale_days,
        max_failed=settings.max_failed,
        incompatible_networks=incompatible,
    )
    total = int(stats["total"])
    if total:
        log.info(
            "Pruned %d V2Ray server(s): %d chronic, %d stale, %d over cap, "
            "%d incompatible",
            total,
            stats["chronic"],
            stats["stale"],
            stats["cap"],
            stats["incompatible"],
        )
    return stats
