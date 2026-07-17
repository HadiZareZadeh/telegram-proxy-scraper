"""Adaptive probe priority: explore / exploit / recover scoring for catalogs."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def hours_since(iso: str | None, *, now: datetime | None = None) -> float:
    dt = parse_iso(iso)
    if dt is None:
        return 1e6  # never checked → treat as extremely stale
    return max(0.0, ((now or utc_now()) - dt).total_seconds() / 3600.0)


def ema(previous: float | None, sample: float, *, alpha: float = 0.35) -> float:
    if previous is None or previous <= 0:
        return sample
    return alpha * sample + (1.0 - alpha) * previous


def backoff_seconds(consecutive_failures: int) -> int:
    """Exponential backoff between probes for chronic failures (capped)."""
    if consecutive_failures <= 0:
        return 0
    # 1 fail → 0s (retry next cycle), 2 → 15m, 3 → 30m, 4 → 1h, … cap 12h
    if consecutive_failures == 1:
        return 0
    minutes = 15 * (2 ** min(consecutive_failures - 2, 6))
    return int(min(minutes, 12 * 60) * 60)


def skip_until_iso(consecutive_failures: int, *, now: datetime | None = None) -> str | None:
    delay = backoff_seconds(consecutive_failures)
    if delay <= 0:
        return None
    return ((now or utc_now()) + timedelta(seconds=delay)).replace(microsecond=0).isoformat()


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    success_count: int = 0
    failure_count: int = 0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    check_count: int = 0
    last_latency_ms: float | None = None
    avg_latency_ms: float | None = None
    last_error: str | None = None
    last_checked_at: str | None = None
    skip_until: str | None = None
    priority_score: float = 1000.0

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> HealthSnapshot:
        keys = set(row.keys()) if hasattr(row, "keys") else set()

        def get(name: str, default=None):
            if keys and name not in keys:
                return default
            try:
                val = row[name]
            except (KeyError, IndexError, TypeError):
                return default
            return default if val is None else val

        lat = get("last_latency_ms")
        avg = get("avg_latency_ms")
        err = get("last_error")
        return cls(
            success_count=int(get("success_count", 0) or 0),
            failure_count=int(get("failure_count", 0) or 0),
            consecutive_successes=int(get("consecutive_successes", 0) or 0),
            consecutive_failures=int(get("consecutive_failures", 0) or 0),
            check_count=int(get("check_count", 0) or 0),
            last_latency_ms=float(lat) if lat is not None else None,
            avg_latency_ms=float(avg) if avg is not None else None,
            last_error=str(err) if err else None,
            last_checked_at=str(get("last_checked_at")) if get("last_checked_at") else None,
            skip_until=str(get("skip_until")) if get("skip_until") else None,
            priority_score=float(get("priority_score", 1000.0) or 1000.0),
        )


def compute_priority_score(
    *,
    success_count: int,
    failure_count: int,
    consecutive_successes: int,
    consecutive_failures: int,
    check_count: int,
    avg_latency_ms: float | None,
    last_latency_ms: float | None,
    last_checked_at: str | None,
    now: datetime | None = None,
) -> float:
    """
    Explore / Exploit / Recover score — higher means probe sooner.

    - Brand-new servers jump the queue (explore).
    - Proven successes with low latency stay near the front (exploit).
    - Chronic failures drop down but get sparse recovery probes (recover).
    - Stale entries get a freshness boost so we don't ignore them forever.
    """
    now = now or utc_now()

    # Never probed → top of the queue
    if check_count <= 0:
        return 1000.0

    # Bayesian-smoothed success rate (avoids 0/0 and overconfidenceing tiny samples)
    success_rate = (success_count + 1.0) / (check_count + 2.0)

    latency = avg_latency_ms if avg_latency_ms is not None else last_latency_ms
    if latency is None or latency <= 0:
        latency_term = 10.0
    else:
        # Faster ≈ higher; ~0ms → +40, ~2000ms → ~0
        latency_term = max(0.0, 40.0 - math.log1p(latency) * 5.0)

    age_h = hours_since(last_checked_at, now=now)
    freshness = min(50.0, age_h * 2.5)  # up to +50 for very stale

    streak = min(consecutive_successes, 20) * 4.0
    fail_penalty = (consecutive_failures**1.6) * 8.0

    # Soft recovery lane so dead servers still get occasional chances
    recovery = 0.0
    if consecutive_failures >= 2:
        recovery = 25.0 / consecutive_failures

    score = (
        40.0
        + success_rate * 120.0
        + latency_term
        + freshness
        + streak
        + recovery
        - fail_penalty
    )
    return round(score, 3)


def apply_success(
    snap: HealthSnapshot,
    latency_s: float,
    *,
    now: datetime | None = None,
) -> HealthSnapshot:
    now = now or utc_now()
    latency_ms = max(0.0, latency_s * 1000.0)
    success = snap.success_count + 1
    checks = snap.check_count + 1
    streak = snap.consecutive_successes + 1
    avg = ema(snap.avg_latency_ms, latency_ms)
    checked = now.replace(microsecond=0).isoformat()
    score = compute_priority_score(
        success_count=success,
        failure_count=snap.failure_count,
        consecutive_successes=streak,
        consecutive_failures=0,
        check_count=checks,
        avg_latency_ms=avg,
        last_latency_ms=latency_ms,
        last_checked_at=checked,
        now=now,
    )
    return HealthSnapshot(
        success_count=success,
        failure_count=snap.failure_count,
        consecutive_successes=streak,
        consecutive_failures=0,
        check_count=checks,
        last_latency_ms=latency_ms,
        avg_latency_ms=avg,
        last_error=None,
        last_checked_at=checked,
        skip_until=None,
        priority_score=score,
    )


def apply_failure(
    snap: HealthSnapshot,
    error: str | None,
    *,
    now: datetime | None = None,
) -> HealthSnapshot:
    now = now or utc_now()
    failures = snap.failure_count + 1
    checks = snap.check_count + 1
    consec = snap.consecutive_failures + 1
    checked = now.replace(microsecond=0).isoformat()
    skip = skip_until_iso(consec, now=now)
    score = compute_priority_score(
        success_count=snap.success_count,
        failure_count=failures,
        consecutive_successes=0,
        consecutive_failures=consec,
        check_count=checks,
        avg_latency_ms=snap.avg_latency_ms,
        last_latency_ms=None,
        last_checked_at=checked,
        now=now,
    )
    return HealthSnapshot(
        success_count=snap.success_count,
        failure_count=failures,
        consecutive_successes=0,
        consecutive_failures=consec,
        check_count=checks,
        last_latency_ms=None,
        avg_latency_ms=snap.avg_latency_ms,
        last_error=(error or "error")[:240],
        last_checked_at=checked,
        skip_until=skip,
        priority_score=score,
    )


def is_probe_eligible(snap: HealthSnapshot, *, now: datetime | None = None) -> bool:
    """True if the server is not in failure backoff."""
    until = parse_iso(snap.skip_until)
    if until is None:
        return True
    return (now or utc_now()) >= until
