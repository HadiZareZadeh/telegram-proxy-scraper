"""V2Ray health state mapping and probe_due_at backoff."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from fetch_mtproto.health import (
    derive_v2ray_state,
    next_probe_due_iso,
    parse_iso,
    state_to_status,
)


class StateTests(unittest.TestCase):
    def test_unknown_until_probed(self) -> None:
        self.assertEqual(
            derive_v2ray_state(
                ok=None, check_count=0, consecutive_failures=0, last_latency_ms=None
            ),
            "unknown",
        )

    def test_healthy_and_degraded_and_dead(self) -> None:
        self.assertEqual(
            derive_v2ray_state(
                ok=True, check_count=3, consecutive_failures=0, last_latency_ms=120
            ),
            "healthy",
        )
        self.assertEqual(
            derive_v2ray_state(
                ok=True, check_count=3, consecutive_failures=0, last_latency_ms=2500
            ),
            "degraded",
        )
        self.assertEqual(
            derive_v2ray_state(
                ok=False, check_count=5, consecutive_failures=3, last_latency_ms=None
            ),
            "dead",
        )

    def test_legacy_status_mapping(self) -> None:
        self.assertEqual(state_to_status("healthy"), "working")
        self.assertEqual(state_to_status("degraded"), "working")
        self.assertEqual(state_to_status("dead"), "failed")
        self.assertEqual(state_to_status("quarantined"), "failed")

    def test_probe_due_backoff(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        unknown = parse_iso(
            next_probe_due_iso("unknown", check_count=0, now=now)
        )
        self.assertEqual(unknown, now)
        degraded = parse_iso(
            next_probe_due_iso("degraded", check_count=4, now=now)
        )
        self.assertEqual(degraded, now + timedelta(minutes=2))
        healthy = parse_iso(
            next_probe_due_iso(
                "healthy",
                consecutive_successes=1,
                success_count=2,
                check_count=3,
                now=now,
            )
        )
        self.assertEqual(healthy, now + timedelta(minutes=10))
        very = parse_iso(
            next_probe_due_iso(
                "healthy",
                consecutive_successes=6,
                success_count=20,
                check_count=20,
                now=now,
            )
        )
        self.assertEqual(very, now + timedelta(minutes=15))
        dead = parse_iso(
            next_probe_due_iso(
                "dead", consecutive_failures=3, check_count=5, now=now
            )
        )
        self.assertEqual(dead, now + timedelta(minutes=10))


if __name__ == "__main__":
    unittest.main()
