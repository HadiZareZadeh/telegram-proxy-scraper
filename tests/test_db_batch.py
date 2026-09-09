"""Batched CatalogDB writer and additive V2Ray schema."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fetch_mtproto.db import CatalogDB


def _server(key: str = "vless:example.com:443:abc") -> tuple:
    return (
        key,
        "vless",
        "vless://abc@example.com:443",
        "example.com",
        443,
        "abc",
        "tcp",
        "none",
        "",
    )


class DbBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = CatalogDB(Path(self.tmp.name) / "catalog.db")

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def test_additive_columns_exist(self) -> None:
        cols = {
            row[1]
            for row in self.db.conn.execute("PRAGMA table_info(v2ray)").fetchall()
        }
        for name in (
            "state",
            "probe_due_at",
            "tcp_reachable",
            "proxy_verified",
            "first_seen_at",
            "last_seen_at",
        ):
            self.assertIn(name, cols)
        tables = {
            row[0]
            for row in self.db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertIn("v2ray_probe_history", tables)
        self.assertIn("pool_assignments", tables)

    def test_trim_working_is_noop(self) -> None:
        self.db.v2ray_insert_new([_server(f"k{i}") for i in range(5)])
        self.assertEqual(self.db.v2ray_trim_working(1), 0)
        self.assertEqual(self.db.v2ray_count(), 5)

    def test_record_results_sets_state_and_due(self) -> None:
        identity = _server()
        self.db.v2ray_insert_new([identity])
        self.db.v2ray_record_results(
            [(identity[0], True, 0.12, None, identity)], probe_type="xray"
        )
        row = self.db.conn.execute("SELECT * FROM v2ray").fetchone()
        self.assertEqual(row["state"], "healthy")
        self.assertEqual(row["status"], "working")
        self.assertEqual(row["proxy_verified"], 1)
        self.assertIsNotNone(row["probe_due_at"])

    def test_enqueue_flushes_at_100(self) -> None:
        rows = [_server(f"n{i}:host:443:{i}") for i in range(120)]
        self.db.v2ray_insert_new(rows)
        outcomes = [(r[0], False, None, "tcp unreachable", r) for r in rows]
        self.db.enqueue_v2ray_results(outcomes[:99], probe_type="tcp")
        pending = len(self.db._pending_v2ray)
        self.assertEqual(pending, 99)
        self.db.enqueue_v2ray_results(outcomes[99:100], probe_type="tcp")
        self.assertEqual(len(self.db._pending_v2ray), 0)

    def test_catalog_max_prunes_dead_only(self) -> None:
        good = _server("good")
        dead = _server("dead")
        self.db.v2ray_insert_new([good, dead])
        self.db.v2ray_record_results([(good[0], True, 0.1, None, good)])
        for _ in range(4):
            self.db.v2ray_record_results(
                [(dead[0], False, None, "fail", dead)], probe_type="tcp"
            )
        removed = self.db.v2ray_enforce_catalog_max(1)
        self.assertGreaterEqual(removed, 1)
        keys = {row["key"] for row in self.db.conn.execute("SELECT key FROM v2ray")}
        self.assertIn("good", keys)

    def test_due_unknown_is_now(self) -> None:
        self.db.v2ray_insert_new([_server()])
        due = self.db.v2ray_due_probe_rows()
        self.assertEqual(len(due), 1)

    def test_sort_order_refresh_is_noop(self) -> None:
        self.assertIsNone(self.db._v2ray_refresh_sort_orders())


if __name__ == "__main__":
    unittest.main()
