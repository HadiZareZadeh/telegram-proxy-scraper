"""In-memory hot-set scoring for the proxy pool ring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from fetch_mtproto.health import hours_since, parse_iso, utc_now


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def success_rate(row: Mapping[str, Any]) -> float:
    checks = _as_int(row.get("check_count") if hasattr(row, "get") else None)
    if not checks:
        try:
            checks = int(row["check_count"] or 0)
        except Exception:
            checks = 0
    successes = 0
    try:
        successes = int(row["success_count"] or 0)
    except Exception:
        successes = 0
    return (successes + 1.0) / (max(checks, 0) + 2.0)


def hot_score(
    row: Mapping[str, Any],
    *,
    now: datetime | None = None,
    reuse_penalty: float = 0.0,
) -> float:
    """Higher is better. Not persisted — computed when selecting the hot set."""
    now = now or utc_now()
    rate = success_rate(row)
    try:
        latency = row["last_latency_ms"]
        latency_ms = float(latency) if latency is not None else 3000.0
    except Exception:
        latency_ms = 3000.0
    try:
        consec_fail = int(row["consecutive_failures"] or 0)
    except Exception:
        consec_fail = 0
    try:
        checked = row["last_checked_at"]
    except Exception:
        checked = None
    freshness = min(80.0, hours_since(str(checked) if checked else None, now=now) * -8.0)
    # Recent checks are better for the hot ring.
    if parse_iso(str(checked) if checked else None) is not None:
        age_h = hours_since(str(checked), now=now)
        freshness = max(-80.0, 40.0 - age_h * 25.0)
    return (
        500.0 * rate
        - 0.5 * latency_ms
        + freshness
        - reuse_penalty
        - float(consec_fail) * 40.0
    )


@dataclass(slots=True)
class HotSet:
    active_keys: list[str]
    standby_keys: list[str]
    reserve_keys: list[str]

    @property
    def all_keys(self) -> list[str]:
        return [*self.active_keys, *self.standby_keys, *self.reserve_keys]


def select_hot_set(
    rows: list[Mapping[str, Any]],
    *,
    active: int,
    standby: int,
    reserve: int,
    assigned: set[str] | None = None,
    cooling: set[str] | None = None,
    now: datetime | None = None,
) -> HotSet:
    """Pick active/standby/reserve from already-filtered eligible rows."""
    now = now or utc_now()
    assigned = assigned or set()
    cooling = cooling or set()
    scored: list[tuple[float, str]] = []
    for row in rows:
        try:
            key = str(row["key"])
        except Exception:
            continue
        penalty = 0.0
        if key in cooling:
            penalty += 200.0
        if key in assigned:
            penalty -= 10.0  # slight stickiness for current assignments
        scored.append((hot_score(row, now=now, reuse_penalty=penalty), key))
    scored.sort(key=lambda item: item[0], reverse=True)

    keys: list[str] = []
    seen: set[str] = set()
    for _score, key in scored:
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)

    n_active = max(0, int(active))
    n_standby = max(0, int(standby))
    n_reserve = max(0, int(reserve))
    active_keys = keys[:n_active]
    standby_keys = keys[n_active : n_active + n_standby]
    reserve_keys = keys[n_active + n_standby : n_active + n_standby + n_reserve]
    return HotSet(active_keys, standby_keys, reserve_keys)


def spread_keys(
    slot_count: int,
    keys: list[str],
    *,
    keep: list[str | None] | None = None,
) -> tuple[list[str | None], int]:
    """Assign slots: at most one unique node each, then round-robin reuse.

    Returns ``(slot_keys, extra_reused)``. Extra reuse happens only when
    unique keys are fewer than slots (hot set exhausted).
    """
    n = max(0, int(slot_count))
    result: list[str | None] = [None] * n
    unique_order: list[str] = []
    seen: set[str] = set()
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        unique_order.append(key)
    used: set[str] = set()
    if keep is not None:
        for index in range(min(n, len(keep))):
            key = keep[index]
            if key and key in seen and key not in used:
                result[index] = key
                used.add(key)
    for index in range(n):
        if result[index] is not None:
            continue
        for key in unique_order:
            if key not in used:
                result[index] = key
                used.add(key)
                break
    extra = 0
    if unique_order:
        rr = 0
        for index in range(n):
            if result[index] is not None:
                continue
            result[index] = unique_order[rr % len(unique_order)]
            rr += 1
            extra += 1
    return result, extra
