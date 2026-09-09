"""Hot-set scoring and ring selection."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from fetch_mtproto.v2ray.hot_set import hot_score, select_hot_set, spread_keys, success_rate


def _row(**kwargs):
    base = {
        "key": "a",
        "check_count": 10,
        "success_count": 8,
        "consecutive_failures": 0,
        "last_latency_ms": 200,
        "last_checked_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    base.update(kwargs)
    return base


class HotSetTests(unittest.TestCase):
    def test_success_rate_smoothed(self) -> None:
        self.assertAlmostEqual(success_rate(_row(check_count=0, success_count=0)), 0.5)
        self.assertGreater(
            success_rate(_row(check_count=10, success_count=10)),
            success_rate(_row(check_count=10, success_count=1)),
        )

    def test_lower_latency_scores_higher(self) -> None:
        now = datetime.now(timezone.utc)
        fast = hot_score(_row(key="fast", last_latency_ms=80), now=now)
        slow = hot_score(_row(key="slow", last_latency_ms=2500), now=now)
        self.assertGreater(fast, slow)

    def test_select_splits_active_standby_reserve(self) -> None:
        rows = [
            _row(key=f"n{i}", last_latency_ms=100 + i, success_count=20 - i, check_count=20)
            for i in range(12)
        ]
        hot = select_hot_set(rows, active=3, standby=3, reserve=2)
        self.assertEqual(len(hot.active_keys), 3)
        self.assertEqual(len(hot.standby_keys), 3)
        self.assertEqual(len(hot.reserve_keys), 2)
        self.assertEqual(len(set(hot.all_keys)), 8)

    def test_cooling_keys_rank_lower(self) -> None:
        rows = [_row(key="cool", last_latency_ms=50), _row(key="fresh", last_latency_ms=60)]
        hot = select_hot_set(rows, active=1, standby=1, reserve=0, cooling={"cool"})
        self.assertEqual(hot.active_keys[0], "fresh")

    def test_spread_unique_when_enough_keys(self) -> None:
        mapped, extra = spread_keys(3, ["a", "b", "c", "d"])
        self.assertEqual(mapped, ["a", "b", "c"])
        self.assertEqual(extra, 0)
        self.assertEqual(len(set(mapped)), 3)

    def test_spread_reuses_when_hot_set_exhausted(self) -> None:
        mapped, extra = spread_keys(5, ["a", "b"])
        self.assertEqual(mapped, ["a", "b", "a", "b", "a"])
        self.assertEqual(extra, 3)

    def test_spread_keeps_sticky_slots(self) -> None:
        mapped, extra = spread_keys(4, ["a", "b", "c"], keep=[None, "c", None, None])
        self.assertEqual(mapped[1], "c")
        self.assertEqual(extra, 1)
        self.assertNotIn("c", [mapped[0], mapped[2]])


if __name__ == "__main__":
    unittest.main()
