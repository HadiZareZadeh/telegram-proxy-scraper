"""Single SQLite catalog for MTProto proxies and V2Ray servers."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from fetch_mtproto.health import (
    HealthSnapshot,
    apply_failure,
    apply_success,
    is_probe_eligible,
    utc_now,
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


class CatalogDB:
    """Shared SQLite connection for MTProto + V2Ray catalogs."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self._migrate_health_columns()
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

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
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

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
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return rows

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

    def v2ray_record_results(
        self,
        outcomes: list[tuple[str, bool, float | None, str | None, tuple | None]],
    ) -> None:
        """Batch-update V2Ray health. Each outcome: (key, ok, latency_s, error, identity)."""
        cur = self.conn.cursor()
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
                        status, sort_order, priority_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', 0, 1000)
                    """,
                    (key, scheme, link, host, port, ident, network, security, sni),
                )
                row = cur.execute(
                    "SELECT * FROM v2ray WHERE key = ?", (key,)
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
                UPDATE v2ray SET
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
        self._v2ray_refresh_sort_orders()

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
        cur = self.conn.cursor()
        working = cur.execute(
            """
            SELECT key FROM v2ray WHERE status = 'working'
            ORDER BY
                CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END,
                last_latency_ms ASC,
                priority_score DESC,
                key
            """
        ).fetchall()
        for i, row in enumerate(working):
            cur.execute(
                "UPDATE v2ray SET sort_order = ? WHERE key = ?",
                (i, row["key"]),
            )
        failed = cur.execute(
            """
            SELECT key FROM v2ray WHERE status = 'failed'
            ORDER BY priority_score DESC, key
            """
        ).fetchall()
        for i, row in enumerate(failed):
            cur.execute(
                "UPDATE v2ray SET sort_order = ? WHERE key = ?",
                (i, row["key"]),
            )
        self.conn.commit()

    def v2ray_trim_working(self, max_working: int) -> int:
        """Demote working V2Ray servers beyond the top max_working (by latency) to failed."""
        if max_working <= 0:
            return 0
        working = self.v2ray_list("working")
        if len(working) <= max_working:
            return 0
        cur = self.conn.cursor()
        for row in working[max_working:]:
            cur.execute(
                """
                UPDATE v2ray SET
                    status = 'failed',
                    updated_at = datetime('now')
                WHERE key = ?
                """,
                (row["key"],),
            )
        self.conn.commit()
        self._v2ray_refresh_sort_orders()
        return len(working) - max_working

    def v2ray_health_summary(self) -> dict[str, float | int]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'working' THEN 1 ELSE 0 END) AS working,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(success_count) AS successes,
                SUM(failure_count) AS failures,
                AVG(CASE WHEN status = 'working' THEN last_latency_ms END) AS avg_ok_ms
            FROM v2ray
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
