"""Single SQLite catalog for MTProto proxies and V2Ray servers."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Iterable

from fetch_mtproto.health import (
    HealthSnapshot,
    apply_failure,
    apply_success,
    derive_v2ray_state,
    is_probe_eligible,
    next_probe_due_iso,
    state_to_status,
    utc_now,
    utc_now_iso,
)

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mtproto (
    key TEXT PRIMARY KEY,
    link TEXT NOT NULL,
    server TEXT NOT NULL,
    port INTEGER NOT NULL,
    secret TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('working', 'failed')),
    sort_order INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    check_count INTEGER NOT NULL DEFAULT 0,
    last_latency_ms REAL,
    avg_latency_ms REAL,
    last_error TEXT,
    last_checked_at TEXT,
    skip_until TEXT,
    priority_score REAL NOT NULL DEFAULT 1000,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_mtproto_status ON mtproto(status, sort_order);

CREATE TABLE IF NOT EXISTS v2ray (
    key TEXT PRIMARY KEY,
    scheme TEXT NOT NULL,
    link TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    identity TEXT NOT NULL DEFAULT '',
    network TEXT NOT NULL DEFAULT '',
    security TEXT NOT NULL DEFAULT '',
    sni TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('working', 'failed')),
    sort_order INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    check_count INTEGER NOT NULL DEFAULT 0,
    last_latency_ms REAL,
    avg_latency_ms REAL,
    last_error TEXT,
    last_checked_at TEXT,
    skip_until TEXT,
    priority_score REAL NOT NULL DEFAULT 1000,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_v2ray_status ON v2ray(status, scheme, sort_order);
CREATE INDEX IF NOT EXISTS idx_v2ray_scheme_status ON v2ray(scheme, status);

CREATE TABLE IF NOT EXISTS v2ray_probe_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_key TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    success INTEGER NOT NULL,
    latency_ms REAL,
    error_code TEXT,
    error_text TEXT,
    probe_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_v2ray_history_checked
    ON v2ray_probe_history(checked_at);
CREATE INDEX IF NOT EXISTS idx_v2ray_history_key
    ON v2ray_probe_history(node_key, checked_at);

CREATE TABLE IF NOT EXISTS pool_assignments (
    slot_id INTEGER PRIMARY KEY,
    node_key TEXT,
    assigned_at TEXT,
    generation INTEGER NOT NULL DEFAULT 0
);
"""

HEALTH_COLUMNS: tuple[tuple[str, str], ...] = (
    ("success_count", "INTEGER NOT NULL DEFAULT 0"),
    ("failure_count", "INTEGER NOT NULL DEFAULT 0"),
    ("consecutive_successes", "INTEGER NOT NULL DEFAULT 0"),
    ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
    ("check_count", "INTEGER NOT NULL DEFAULT 0"),
    ("last_latency_ms", "REAL"),
    ("avg_latency_ms", "REAL"),
    ("last_error", "TEXT"),
    ("last_checked_at", "TEXT"),
    ("skip_until", "TEXT"),
    ("priority_score", "REAL NOT NULL DEFAULT 1000"),
)

V2RAY_STATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("state", "TEXT NOT NULL DEFAULT 'unknown'"),
    ("probe_due_at", "TEXT"),
    ("tcp_reachable", "INTEGER"),
    ("proxy_verified", "INTEGER NOT NULL DEFAULT 0"),
    ("first_seen_at", "TEXT"),
    ("last_seen_at", "TEXT"),
    ("last_success_at", "TEXT"),
    ("last_failure_at", "TEXT"),
)


class CatalogDB:
    """Shared SQLite connection for MTProto + V2Ray catalogs."""

    def __init__(self, path: str | Path, *, shared: bool = False) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._shared = shared
        self._write_lock = threading.Lock()
        self._pending_v2ray: list[tuple] = []
        self._closed = False
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self._migrate_health_columns()
        self._migrate_v2ray_state()
        self.conn.commit()

    def close(self) -> None:
        if self._shared:
            self.flush_v2ray_writer()
            return
        with self._write_lock:
            self._flush_v2ray_pending_locked()
            if not self._closed:
                self.conn.close()
                self._closed = True

    def __enter__(self) -> CatalogDB:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _migrate_health_columns(self) -> None:
        for table in ("mtproto", "v2ray"):
            existing = {
                row[1]
                for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for name, decl in HEALTH_COLUMNS:
                if name not in existing:
                    self.conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {decl}"
                    )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mtproto_probe "
            "ON mtproto(priority_score DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_v2ray_probe "
            "ON v2ray(priority_score DESC)"
        )
        self.conn.commit()

    def _migrate_v2ray_state(self) -> None:
        existing = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(v2ray)").fetchall()
        }
        for name, decl in V2RAY_STATE_COLUMNS:
            if name not in existing:
                self.conn.execute(f"ALTER TABLE v2ray ADD COLUMN {name} {decl}")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_v2ray_hot "
            "ON v2ray(state, skip_until, last_checked_at, last_latency_ms)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_v2ray_pool "
            "ON v2ray(state, last_checked_at, last_latency_ms, consecutive_failures)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_v2ray_probe_due ON v2ray(probe_due_at)"
        )
        self.conn.execute(
            """
            UPDATE v2ray SET state = 'unknown'
            WHERE (state IS NULL OR state = '') AND check_count = 0
            """
        )
        self.conn.execute(
            """
            UPDATE v2ray SET state = 'healthy'
            WHERE (state IS NULL OR state = '' OR state = 'unknown')
              AND status = 'working' AND check_count > 0
              AND consecutive_failures = 0
            """
        )
        self.conn.execute(
            """
            UPDATE v2ray SET state = 'dead'
            WHERE (state IS NULL OR state = '')
              AND status = 'failed' AND consecutive_failures >= 3
            """
        )
        self.conn.execute(
            """
            UPDATE v2ray SET probe_due_at = datetime('now')
            WHERE probe_due_at IS NULL
            """
        )
        self.conn.execute(
            """
            UPDATE v2ray SET proxy_verified = 1
            WHERE status = 'working'
              AND last_latency_ms IS NOT NULL
              AND (proxy_verified IS NULL OR proxy_verified = 0)
            """
        )
        self.conn.commit()

    # --- MTProto ---------------------------------------------------------

    def mtproto_count(self, status: str | None = None) -> int:
        if status is None:
            row = self.conn.execute("SELECT COUNT(*) AS n FROM mtproto").fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM mtproto WHERE status = ?", (status,)
            ).fetchone()
        return int(row["n"])

    def mtproto_has(self, key: str, status: str | None = None) -> bool:
        if status is None:
            row = self.conn.execute(
                "SELECT 1 FROM mtproto WHERE key = ? LIMIT 1", (key,)
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT 1 FROM mtproto WHERE key = ? AND status = ? LIMIT 1",
                (key, status),
            ).fetchone()
        return row is not None

    def mtproto_list(self, status: str) -> list[sqlite3.Row]:
        # Working: fastest first; failed: highest probe priority first
        if status == "working":
            order = (
                "CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END, "
                "last_latency_ms ASC, priority_score DESC, key"
            )
        else:
            order = "priority_score DESC, key"
        return list(
            self.conn.execute(
                f"SELECT * FROM mtproto WHERE status = ? ORDER BY {order}",
                (status,),
            )
        )

    def mtproto_all(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM mtproto ORDER BY priority_score DESC, key"
            )
        )

    def mtproto_probe_queue(
        self,
        *,
        respect_backoff: bool = True,
        limit: int | None = None,
        failed_limit: int | None = None,
    ) -> list[sqlite3.Row]:
        """Servers to probe, most promising first (explore / exploit / recover)."""
        rows = list(
            self.conn.execute(
                "SELECT * FROM mtproto ORDER BY priority_score DESC, key"
            )
        )
        now = utc_now()
        if respect_backoff:
            eligible = [
                row
                for row in rows
                if is_probe_eligible(HealthSnapshot.from_row(row), now=now)
            ]
            # If backoff would empty the queue, fall back to full ordered list
            rows = eligible or rows
        rows = self._apply_probe_failed_limit(rows, failed_limit)
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

    @staticmethod
    def _apply_probe_failed_limit(
        rows: list[sqlite3.Row], failed_limit: int | None
    ) -> list[sqlite3.Row]:
        """Always probe working servers; cap how many failed entries are retried."""
        if failed_limit is None or failed_limit <= 0:
            return rows
        working = [row for row in rows if row["status"] == "working"]
        failed = [row for row in rows if row["status"] == "failed"]
        return working + failed[:failed_limit]

    def mtproto_prune(
        self,
        *,
        after_failures: int = 8,
        min_checks: int = 5,
        stale_days: int = 14,
        max_failed: int = 0,
    ) -> dict[str, int]:
        """Delete chronic / stale failed MTProto proxies; trim failed over max_failed."""
        chronic = stale = cap = 0

        if after_failures > 0 and min_checks > 0:
            cur = self.conn.execute(
                """
                DELETE FROM mtproto
                WHERE status = 'failed'
                  AND consecutive_failures >= ?
                  AND check_count >= ?
                  AND success_count = 0
                """,
                (after_failures, min_checks),
            )
            chronic = cur.rowcount

        if stale_days > 0:
            cur = self.conn.execute(
                """
                DELETE FROM mtproto
                WHERE status = 'failed'
                  AND consecutive_failures >= 3
                  AND last_checked_at IS NOT NULL
                  AND datetime(last_checked_at) < datetime('now', ?)
                """,
                (f"-{int(stale_days)} days",),
            )
            stale = cur.rowcount

        if max_failed > 0:
            cap = self._trim_failed_rows("mtproto", max_failed)

        self.conn.commit()
        if chronic or stale or cap:
            self._mtproto_refresh_sort_orders()
        total = chronic + stale + cap
        return {"chronic": chronic, "stale": stale, "cap": cap, "total": total}

    def mtproto_upsert_working(self, rows: Iterable[tuple]) -> int:
        """Insert proxies as working; promote from failed. Returns newly added count."""
        added = 0
        cur = self.conn.cursor()
        for key, link, server, port, secret in rows:
            existing = cur.execute(
                "SELECT status FROM mtproto WHERE key = ?", (key,)
            ).fetchone()
            if existing and existing["status"] == "working":
                continue
            score = 1000.0  # brand-new / revived → explore first
            cur.execute(
                """
                INSERT INTO mtproto (
                    key, link, server, port, secret, status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, 'working', 0, ?)
                ON CONFLICT(key) DO UPDATE SET
                    link = excluded.link,
                    server = excluded.server,
                    port = excluded.port,
                    secret = excluded.secret,
                    status = 'working',
                    sort_order = 0,
                    priority_score = CASE
                        WHEN mtproto.check_count = 0 THEN 1000
                        ELSE mtproto.priority_score
                    END,
                    updated_at = datetime('now')
                """,
                (key, link, server, port, secret, score),
            )
            added += 1
        self.conn.commit()
        return added

    def mtproto_record_results(
        self,
        outcomes: list[tuple[str, bool, float | None, str | None, tuple | None]],
    ) -> None:
        """
        Batch-update health for many ping results in one transaction.

        Each outcome: (key, ok, latency_s, error, identity_or_none)
        identity_or_none = (link, server, port, secret) when the row may be missing.
        """
        cur = self.conn.cursor()
        for key, ok, latency_s, error, identity in outcomes:
            row = cur.execute("SELECT * FROM mtproto WHERE key = ?", (key,)).fetchone()
            if row is None:
                if identity is None:
                    continue
                link, server, port, secret = identity
                cur.execute(
                    """
                    INSERT INTO mtproto (
                        key, link, server, port, secret, status, sort_order, priority_score
                    ) VALUES (?, ?, ?, ?, ?, 'failed', 0, 1000)
                    """,
                    (key, link, server, port, secret),
                )
                row = cur.execute(
                    "SELECT * FROM mtproto WHERE key = ?", (key,)
                ).fetchone()

            snap = HealthSnapshot.from_row(row)
            if ok and latency_s is not None:
                new = apply_success(snap, latency_s)
                status = "working"
            else:
                new = apply_failure(snap, error)
                status = "failed"

            cur.execute(
                """
                UPDATE mtproto SET
                    status = ?,
                    success_count = ?,
                    failure_count = ?,
                    consecutive_successes = ?,
                    consecutive_failures = ?,
                    check_count = ?,
                    last_latency_ms = ?,
                    avg_latency_ms = ?,
                    last_error = ?,
                    last_checked_at = ?,
                    skip_until = ?,
                    priority_score = ?,
                    updated_at = datetime('now')
                WHERE key = ?
                """,
                (
                    status,
                    new.success_count,
                    new.failure_count,
                    new.consecutive_successes,
                    new.consecutive_failures,
                    new.check_count,
                    new.last_latency_ms,
                    new.avg_latency_ms,
                    new.last_error,
                    new.last_checked_at,
                    new.skip_until,
                    new.priority_score,
                    key,
                ),
            )
        self.conn.commit()
        self._mtproto_refresh_sort_orders()

    def mtproto_record_result(
        self,
        key: str,
        *,
        ok: bool,
        latency_s: float | None = None,
        error: str | None = None,
        identity: tuple[str, str, int, str] | None = None,
    ) -> None:
        self.mtproto_record_results([(key, ok, latency_s, error, identity)])

    def mtproto_reorganize(
        self,
        ok: list[tuple],
        failed: list[tuple],
    ) -> None:
        """Upsert status lists (used by legacy import). Prefer record_result for pings."""
        cur = self.conn.cursor()
        seen: set[str] = set()
        for i, row in enumerate(ok):
            key, link, server, port, secret = row[:5]
            seen.add(key)
            cur.execute(
                """
                INSERT INTO mtproto (
                    key, link, server, port, secret, status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, 'working', ?, 1000)
                ON CONFLICT(key) DO UPDATE SET
                    link = excluded.link,
                    server = excluded.server,
                    port = excluded.port,
                    secret = excluded.secret,
                    status = 'working',
                    sort_order = excluded.sort_order,
                    updated_at = datetime('now')
                """,
                (key, link, server, port, secret, i),
            )
        for i, row in enumerate(failed):
            key, link, server, port, secret = row[:5]
            seen.add(key)
            cur.execute(
                """
                INSERT INTO mtproto (
                    key, link, server, port, secret, status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, 'failed', ?, 100)
                ON CONFLICT(key) DO UPDATE SET
                    link = excluded.link,
                    server = excluded.server,
                    port = excluded.port,
                    secret = excluded.secret,
                    status = 'failed',
                    sort_order = excluded.sort_order,
                    updated_at = datetime('now')
                """,
                (key, link, server, port, secret, i),
            )
        if seen:
            placeholders = ",".join("?" * len(seen))
            cur.execute(
                f"DELETE FROM mtproto WHERE key NOT IN ({placeholders})",
                tuple(seen),
            )
        self.conn.commit()
        self._mtproto_refresh_sort_orders()

    def _mtproto_refresh_sort_orders(self) -> None:
        cur = self.conn.cursor()
        working = cur.execute(
            """
            SELECT key FROM mtproto WHERE status = 'working'
            ORDER BY
                CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END,
                last_latency_ms ASC,
                priority_score DESC,
                key
            """
        ).fetchall()
        for i, row in enumerate(working):
            cur.execute(
                "UPDATE mtproto SET sort_order = ? WHERE key = ?",
                (i, row["key"]),
            )
        failed = cur.execute(
            """
            SELECT key FROM mtproto WHERE status = 'failed'
            ORDER BY priority_score DESC, key
            """
        ).fetchall()
        for i, row in enumerate(failed):
            cur.execute(
                "UPDATE mtproto SET sort_order = ? WHERE key = ?",
                (i, row["key"]),
            )
        self.conn.commit()

    def mtproto_trim_working(self, max_working: int) -> int:
        """Demote working proxies beyond the top max_working (by latency) to failed."""
        if max_working <= 0:
            return 0
        working = self.mtproto_list("working")
        if len(working) <= max_working:
            return 0
        cur = self.conn.cursor()
        for row in working[max_working:]:
            cur.execute(
                """
                UPDATE mtproto SET
                    status = 'failed',
                    updated_at = datetime('now')
                WHERE key = ?
                """,
                (row["key"],),
            )
        self.conn.commit()
        self._mtproto_refresh_sort_orders()
        return len(working) - max_working

    def mtproto_health_summary(self) -> dict[str, float | int]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'working' THEN 1 ELSE 0 END) AS working,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(success_count) AS successes,
                SUM(failure_count) AS failures,
                AVG(CASE WHEN status = 'working' THEN last_latency_ms END) AS avg_ok_ms
            FROM mtproto
            """
        ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "working": int(row["working"] or 0),
            "failed": int(row["failed"] or 0),
            "successes": int(row["successes"] or 0),
            "failures": int(row["failures"] or 0),
            "avg_ok_ms": float(row["avg_ok_ms"] or 0),
        }

    # --- V2Ray -----------------------------------------------------------

    def v2ray_count(self, status: str | None = None, scheme: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if scheme is not None:
            clauses.append("scheme = ?")
            params.append(scheme)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM v2ray {where}", params
        ).fetchone()
        return int(row["n"])

    def v2ray_has(
        self, key: str, status: str | None = None, scheme: str | None = None
    ) -> bool:
        clauses = ["key = ?"]
        params: list[object] = [key]
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if scheme is not None:
            clauses.append("scheme = ?")
            params.append(scheme)
        row = self.conn.execute(
            f"SELECT 1 FROM v2ray WHERE {' AND '.join(clauses)} LIMIT 1",
            params,
        ).fetchone()
        return row is not None

    def v2ray_subscription_list(self, limit: int | None = None) -> list[sqlite3.Row]:
        """Working servers for subscription export: fastest first, then most recently checked."""
        order = (
            "CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END, "
            "last_latency_ms ASC, "
            "CASE WHEN last_checked_at IS NULL THEN 1 ELSE 0 END, "
            "last_checked_at DESC, "
            "updated_at DESC, "
            "priority_score DESC, "
            "key"
        )
        query = f"SELECT * FROM v2ray WHERE status = 'working' ORDER BY {order}"
        if limit is not None and limit > 0:
            query += f" LIMIT {int(limit)}"
        return list(self.conn.execute(query))

    def v2ray_list(
        self, status: str, scheme: str | None = None
    ) -> list[sqlite3.Row]:
        if status == "working":
            order = (
                "CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END, "
                "last_latency_ms ASC, priority_score DESC, key"
            )
        else:
            order = "priority_score DESC, key"
        if scheme is None:
            return list(
                self.conn.execute(
                    f"SELECT * FROM v2ray WHERE status = ? ORDER BY {order}",
                    (status,),
                )
            )
        return list(
            self.conn.execute(
                f"""
                SELECT * FROM v2ray
                WHERE status = ? AND scheme = ?
                ORDER BY {order}
                """,
                (status, scheme),
            )
        )

    def v2ray_all(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM v2ray ORDER BY priority_score DESC, key"
            )
        )

    def v2ray_probe_queue(
        self,
        *,
        respect_backoff: bool = True,
        limit: int | None = None,
        failed_limit: int | None = None,
    ) -> list[sqlite3.Row]:
        rows = list(
            self.conn.execute(
                "SELECT * FROM v2ray ORDER BY priority_score DESC, key"
            )
        )
        now = utc_now()
        if respect_backoff:
            eligible = [
                row
                for row in rows
                if is_probe_eligible(HealthSnapshot.from_row(row), now=now)
            ]
            rows = eligible or rows
        rows = self._apply_probe_failed_limit(rows, failed_limit)
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

    def _trim_failed_rows(self, table: str, max_failed: int) -> int:
        count_row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM {table} WHERE status = 'failed'"
        ).fetchone()
        failed_count = int(count_row["n"])
        if failed_count <= max_failed:
            return 0
        excess = failed_count - max_failed
        # Prefer deleting probed chronic failures; only drop never-checked candidates
        # after those are exhausted (URL-source imports rely on this).
        keys = self.conn.execute(
            f"""
            SELECT key FROM {table}
            WHERE status = 'failed'
              AND check_count > 0
            ORDER BY priority_score ASC,
                     CASE WHEN last_checked_at IS NULL THEN 1 ELSE 0 END,
                     last_checked_at ASC,
                     key
            LIMIT ?
            """,
            (excess,),
        ).fetchall()
        if len(keys) < excess:
            remaining = excess - len(keys)
            more = self.conn.execute(
                f"""
                SELECT key FROM {table}
                WHERE status = 'failed'
                  AND check_count = 0
                ORDER BY priority_score ASC, key
                LIMIT ?
                """,
                (remaining,),
            ).fetchall()
            keys = list(keys) + list(more)
        if not keys:
            return 0
        placeholders = ",".join("?" * len(keys))
        cur = self.conn.execute(
            f"DELETE FROM {table} WHERE key IN ({placeholders})",
            tuple(row["key"] for row in keys),
        )
        return cur.rowcount

    def v2ray_prune(
        self,
        *,
        after_failures: int = 8,
        min_checks: int = 5,
        stale_days: int = 14,
        max_failed: int = 0,
        incompatible_networks: tuple[str, ...] = (),
    ) -> dict[str, int]:
        """Delete chronic / stale / incompatible failed V2Ray servers."""
        chronic = stale = cap = incompatible = 0

        if incompatible_networks:
            placeholders = ",".join("?" * len(incompatible_networks))
            cur = self.conn.execute(
                f"""
                DELETE FROM v2ray
                WHERE lower(network) IN ({placeholders})
                """,
                tuple(net.lower() for net in incompatible_networks),
            )
            incompatible = cur.rowcount

        if after_failures > 0 and min_checks > 0:
            cur = self.conn.execute(
                """
                DELETE FROM v2ray
                WHERE status = 'failed'
                  AND consecutive_failures >= ?
                  AND check_count >= ?
                  AND success_count = 0
                """,
                (after_failures, min_checks),
            )
            chronic = cur.rowcount

        if stale_days > 0:
            cur = self.conn.execute(
                """
                DELETE FROM v2ray
                WHERE status = 'failed'
                  AND consecutive_failures >= 3
                  AND last_checked_at IS NOT NULL
                  AND datetime(last_checked_at) < datetime('now', ?)
                """,
                (f"-{int(stale_days)} days",),
            )
            stale = cur.rowcount

        if max_failed > 0:
            # Inventory cap is catalog_max (dead-only). Do not demote healthy
            # servers just because they are not in a working-set ranking.
            cap = 0

        self.conn.commit()
        if chronic or stale or cap or incompatible:
            self._v2ray_refresh_sort_orders()
        total = chronic + stale + cap + incompatible
        return {
            "chronic": chronic,
            "stale": stale,
            "cap": cap,
            "incompatible": incompatible,
            "total": total,
        }

    def v2ray_upsert_working(self, rows: Iterable[tuple]) -> int:
        added = 0
        cur = self.conn.cursor()
        for (
            key,
            scheme,
            link,
            host,
            port,
            identity,
            network,
            security,
            sni,
        ) in rows:
            existing = cur.execute(
                "SELECT status FROM v2ray WHERE key = ?", (key,)
            ).fetchone()
            if existing and existing["status"] == "working":
                continue
            cur.execute(
                """
                INSERT INTO v2ray (
                    key, scheme, link, host, port, identity, network, security, sni,
                    status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'working', 0, 1000)
                ON CONFLICT(key) DO UPDATE SET
                    scheme = excluded.scheme,
                    link = excluded.link,
                    host = excluded.host,
                    port = excluded.port,
                    identity = excluded.identity,
                    network = excluded.network,
                    security = excluded.security,
                    sni = excluded.sni,
                    status = 'working',
                    sort_order = 0,
                    priority_score = CASE
                        WHEN v2ray.check_count = 0 THEN 1000
                        ELSE v2ray.priority_score
                    END,
                    updated_at = datetime('now')
                """,
                (key, scheme, link, host, port, identity, network, security, sni),
            )
            added += 1
        self.conn.commit()
        return added

    def v2ray_insert_new(self, rows: Iterable[tuple]) -> int:
        """Insert only unknown keys as working explore candidates (never revive known rows)."""
        added = 0
        cur = self.conn.cursor()
        for (
            key,
            scheme,
            link,
            host,
            port,
            identity,
            network,
            security,
            sni,
        ) in rows:
            cur.execute(
                """
                INSERT OR IGNORE INTO v2ray (
                    key, scheme, link, host, port, identity, network, security, sni,
                    status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'working', 0, 1000)
                """,
                (key, scheme, link, host, port, identity, network, security, sni),
            )
            added += cur.rowcount
        self.conn.commit()
        return added

    def v2ray_record_results(
        self,
        outcomes: list[tuple[str, bool, float | None, str | None, tuple | None]],
        *,
        probe_type: str = "xray",
        tcp_reachable: bool | None = None,
    ) -> None:
        """Batch-update V2Ray health. Each outcome: (key, ok, latency_s, error, identity)."""
        if not outcomes:
            return
        with self._write_lock:
            self._v2ray_record_results_locked(
                outcomes, probe_type=probe_type, tcp_reachable=tcp_reachable
            )

    def enqueue_v2ray_results(
        self,
        outcomes: list[tuple[str, bool, float | None, str | None, tuple | None]],
        *,
        probe_type: str = "xray",
    ) -> None:
        """Queue probe results; flushed at 100 rows or via flush_v2ray_writer()."""
        if not outcomes:
            return
        with self._write_lock:
            self._pending_v2ray.extend(
                (item + (probe_type,) for item in outcomes)  # type: ignore[operator]
            )
            if len(self._pending_v2ray) >= 100:
                self._flush_v2ray_pending_locked()

    def flush_v2ray_writer(self) -> None:
        with self._write_lock:
            self._flush_v2ray_pending_locked()

    def _flush_v2ray_pending_locked(self) -> None:
        if not self._pending_v2ray:
            return
        grouped: dict[str, list] = {}
        for item in self._pending_v2ray:
            if len(item) == 6:
                key, ok, latency_s, error, identity, probe_type = item
            else:
                key, ok, latency_s, error, identity = item[:5]
                probe_type = "xray"
            grouped.setdefault(str(probe_type), []).append(
                (key, ok, latency_s, error, identity)
            )
        self._pending_v2ray.clear()
        for probe_type, batch in grouped.items():
            self._v2ray_record_results_locked(batch, probe_type=probe_type)

    def _v2ray_record_results_locked(
        self,
        outcomes: list[tuple[str, bool, float | None, str | None, tuple | None]],
        *,
        probe_type: str = "xray",
        tcp_reachable: bool | None = None,
    ) -> None:
        cur = self.conn.cursor()
        history_cutoff = "datetime('now', '-30 days')"
        for key, ok, latency_s, error, identity in outcomes:
            row = cur.execute("SELECT * FROM v2ray WHERE key = ?", (key,)).fetchone()
            if row is None:
                if identity is None:
                    continue
                (
                    _key,
                    scheme,
                    link,
                    host,
                    port,
                    ident,
                    network,
                    security,
                    sni,
                ) = identity[:9]
                cur.execute(
                    """
                    INSERT INTO v2ray (
                        key, scheme, link, host, port, identity, network, security, sni,
                        status, sort_order, priority_score, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'working', 0, 1000, 'unknown')
                    """,
                    (key, scheme, link, host, port, ident, network, security, sni),
                )
                row = cur.execute(
                    "SELECT * FROM v2ray WHERE key = ?", (key,)
                ).fetchone()

            snap = HealthSnapshot.from_row(row)
            if ok and latency_s is not None:
                new = apply_success(snap, latency_s)
            else:
                new = apply_failure(snap, error)

            state = derive_v2ray_state(
                ok=bool(ok and latency_s is not None),
                check_count=new.check_count,
                consecutive_failures=new.consecutive_failures,
                last_latency_ms=new.last_latency_ms,
            )
            status = state_to_status(state)
            due = next_probe_due_iso(
                state,
                consecutive_failures=new.consecutive_failures,
                consecutive_successes=new.consecutive_successes,
                success_count=new.success_count,
                check_count=new.check_count,
            )
            tcp_val = None
            if tcp_reachable is not None:
                tcp_val = 1 if tcp_reachable else 0
            elif probe_type == "tcp":
                tcp_val = 1 if ok else 0
            proxy_verified = 1 if (ok and probe_type != "tcp") else 0
            now_iso = utc_now_iso()
            success_at = now_iso if ok else None
            failure_at = None if ok else now_iso

            cur.execute(
                """
                UPDATE v2ray SET
                    status = ?,
                    state = ?,
                    success_count = ?,
                    failure_count = ?,
                    consecutive_successes = ?,
                    consecutive_failures = ?,
                    check_count = ?,
                    last_latency_ms = ?,
                    avg_latency_ms = ?,
                    last_error = ?,
                    last_checked_at = ?,
                    skip_until = ?,
                    priority_score = ?,
                    probe_due_at = ?,
                    tcp_reachable = COALESCE(?, tcp_reachable),
                    proxy_verified = CASE WHEN ? = 1 THEN 1 ELSE proxy_verified END,
                    last_success_at = COALESCE(?, last_success_at),
                    last_failure_at = COALESCE(?, last_failure_at),
                    last_seen_at = ?,
                    updated_at = datetime('now')
                WHERE key = ?
                """,
                (
                    status,
                    state,
                    new.success_count,
                    new.failure_count,
                    new.consecutive_successes,
                    new.consecutive_failures,
                    new.check_count,
                    new.last_latency_ms,
                    new.avg_latency_ms,
                    new.last_error,
                    new.last_checked_at,
                    new.skip_until,
                    new.priority_score,
                    due,
                    tcp_val,
                    proxy_verified,
                    success_at,
                    failure_at,
                    now_iso,
                    key,
                ),
            )
            latency_ms = None if latency_s is None else latency_s * 1000.0
            cur.execute(
                """
                INSERT INTO v2ray_probe_history (
                    node_key, checked_at, success, latency_ms, error_code, error_text, probe_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    new.last_checked_at or now_iso,
                    1 if ok else 0,
                    latency_ms,
                    None if ok else (error or "error")[:80],
                    None if ok else (error or "")[:240],
                    probe_type,
                ),
            )
        cur.execute(
            f"DELETE FROM v2ray_probe_history WHERE datetime(checked_at) < {history_cutoff}"
        )
        self.conn.commit()

    def v2ray_record_result(
        self,
        key: str,
        *,
        ok: bool,
        latency_s: float | None = None,
        error: str | None = None,
        identity: tuple | None = None,
    ) -> None:
        self.v2ray_record_results([(key, ok, latency_s, error, identity)])

    def v2ray_reorganize(
        self,
        ok: list[tuple],
        failed: list[tuple],
    ) -> None:
        cur = self.conn.cursor()
        seen: set[str] = set()
        for i, row in enumerate(ok):
            (
                key,
                scheme,
                link,
                host,
                port,
                identity,
                network,
                security,
                sni,
            ) = row[:9]
            seen.add(key)
            cur.execute(
                """
                INSERT INTO v2ray (
                    key, scheme, link, host, port, identity, network, security, sni,
                    status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'working', ?, 1000)
                ON CONFLICT(key) DO UPDATE SET
                    scheme = excluded.scheme,
                    link = excluded.link,
                    host = excluded.host,
                    port = excluded.port,
                    identity = excluded.identity,
                    network = excluded.network,
                    security = excluded.security,
                    sni = excluded.sni,
                    status = 'working',
                    sort_order = excluded.sort_order,
                    updated_at = datetime('now')
                """,
                (key, scheme, link, host, port, identity, network, security, sni, i),
            )
        for i, row in enumerate(failed):
            (
                key,
                scheme,
                link,
                host,
                port,
                identity,
                network,
                security,
                sni,
            ) = row[:9]
            seen.add(key)
            cur.execute(
                """
                INSERT INTO v2ray (
                    key, scheme, link, host, port, identity, network, security, sni,
                    status, sort_order, priority_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, 100)
                ON CONFLICT(key) DO UPDATE SET
                    scheme = excluded.scheme,
                    link = excluded.link,
                    host = excluded.host,
                    port = excluded.port,
                    identity = excluded.identity,
                    network = excluded.network,
                    security = excluded.security,
                    sni = excluded.sni,
                    status = 'failed',
                    sort_order = excluded.sort_order,
                    updated_at = datetime('now')
                """,
                (key, scheme, link, host, port, identity, network, security, sni, i),
            )
        if seen:
            placeholders = ",".join("?" * len(seen))
            cur.execute(
                f"DELETE FROM v2ray WHERE key NOT IN ({placeholders})",
                tuple(seen),
            )
        self.conn.commit()
        self._v2ray_refresh_sort_orders()

    def _v2ray_refresh_sort_orders(self) -> None:
        """No-op: ranking is computed from indexed columns, not materialized."""
        return

    def v2ray_trim_working(self, max_working: int) -> int:
        """Removed: never demote healthy servers for being outside a top-N set."""
        del max_working
        return 0

    def v2ray_enforce_catalog_max(self, catalog_max: int) -> int:
        """Delete lowest-priority dead rows when inventory exceeds catalog_max."""
        if catalog_max <= 0:
            return 0
        total = self.v2ray_count()
        if total <= catalog_max:
            return 0
        excess = total - catalog_max
        keys = self.conn.execute(
            """
            SELECT key FROM v2ray
            WHERE state IN ('dead', 'quarantined')
            ORDER BY priority_score ASC,
                     CASE WHEN last_checked_at IS NULL THEN 1 ELSE 0 END,
                     last_checked_at ASC,
                     key
            LIMIT ?
            """,
            (excess,),
        ).fetchall()
        if not keys:
            return 0
        placeholders = ",".join("?" * len(keys))
        cur = self.conn.execute(
            f"DELETE FROM v2ray WHERE key IN ({placeholders})",
            tuple(row["key"] for row in keys),
        )
        self.conn.commit()
        return cur.rowcount

    def v2ray_due_probe_rows(
        self,
        *,
        limit: int | None = None,
        now_iso: str | None = None,
    ) -> list[sqlite3.Row]:
        due = now_iso or utc_now_iso()
        query = """
            SELECT * FROM v2ray
            WHERE probe_due_at IS NULL OR probe_due_at <= ?
            ORDER BY
                CASE state WHEN 'unknown' THEN 0 WHEN 'degraded' THEN 1
                           WHEN 'healthy' THEN 2 ELSE 3 END,
                probe_due_at ASC,
                priority_score DESC,
                key
        """
        if limit is not None and limit > 0:
            query += f" LIMIT {int(limit)}"
        return list(self.conn.execute(query, (due,)))

    def v2ray_hot_candidates(
        self,
        *,
        max_latency_ms: float,
        freshness_sec: float,
        limit: int = 2000,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT * FROM v2ray
                WHERE state IN ('healthy', 'degraded')
                  AND proxy_verified = 1
                  AND last_latency_ms IS NOT NULL
                  AND last_latency_ms <= ?
                  AND last_checked_at IS NOT NULL
                  AND (julianday('now') - julianday(last_checked_at)) * 86400.0 <= ?
                ORDER BY last_latency_ms ASC, consecutive_failures ASC, key
                LIMIT ?
                """,
                (max_latency_ms, max(60.0, float(freshness_sec)), int(limit)),
            )
        )

    def v2ray_by_keys(self, keys: list[str]) -> list[sqlite3.Row]:
        if not keys:
            return []
        placeholders = ",".join("?" * len(keys))
        rows = list(
            self.conn.execute(
                f"SELECT * FROM v2ray WHERE key IN ({placeholders})",
                tuple(keys),
            )
        )
        order = {key: i for i, key in enumerate(keys)}
        rows.sort(key=lambda row: order.get(row["key"], 10_000))
        return rows

    def replace_pool_assignments(
        self, assignments: list[tuple[int, str | None]], *, generation: int
    ) -> None:
        now = utc_now_iso()
        with self._write_lock:
            cur = self.conn.cursor()
            for slot_id, node_key in assignments:
                cur.execute(
                    """
                    INSERT INTO pool_assignments (slot_id, node_key, assigned_at, generation)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(slot_id) DO UPDATE SET
                        node_key = excluded.node_key,
                        assigned_at = excluded.assigned_at,
                        generation = excluded.generation
                    """,
                    (int(slot_id), node_key, now, int(generation)),
                )
            self.conn.commit()

    def v2ray_health_summary(self) -> dict[str, float | int]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN state IN ('healthy', 'degraded')
                          OR (state = 'unknown' AND status = 'working')
                         THEN 1 ELSE 0 END) AS working,
                SUM(CASE WHEN state IN ('dead', 'quarantined')
                          OR status = 'failed'
                         THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN state = 'healthy' THEN 1 ELSE 0 END) AS healthy,
                SUM(CASE WHEN state = 'degraded' THEN 1 ELSE 0 END) AS degraded,
                SUM(CASE WHEN state = 'unknown' THEN 1 ELSE 0 END) AS unknown,
                SUM(CASE WHEN state = 'dead' THEN 1 ELSE 0 END) AS dead,
                SUM(success_count) AS successes,
                SUM(failure_count) AS failures,
                AVG(CASE WHEN state IN ('healthy', 'degraded')
                         THEN last_latency_ms END) AS avg_ok_ms,
                SUM(CASE WHEN state IN ('healthy', 'degraded')
                          AND proxy_verified = 1
                          AND last_latency_ms IS NOT NULL
                          AND last_latency_ms <= 3000
                         THEN 1 ELSE 0 END) AS hot
            FROM v2ray
            """
        ).fetchone()
        assigned_row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM pool_assignments WHERE node_key IS NOT NULL"
        ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "working": int(row["working"] or 0),
            "failed": int(row["failed"] or 0),
            "healthy": int(row["healthy"] or 0),
            "degraded": int(row["degraded"] or 0),
            "unknown": int(row["unknown"] or 0),
            "dead": int(row["dead"] or 0),
            "hot": int(row["hot"] or 0),
            "assigned": int(assigned_row["n"] or 0),
            "successes": int(row["successes"] or 0),
            "failures": int(row["failures"] or 0),
            "avg_ok_ms": float(row["avg_ok_ms"] or 0),
        }

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO meta (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        self.conn.commit()
