"""Run persistent local SOCKS5 + HTTP proxies backed by catalog V2Ray servers."""

from __future__ import annotations

import json
import os
import random
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.health import hours_since
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import (
    DEFAULT_TEST_TIMEOUT,
    DEFAULT_TEST_URL,
    resolve_xray_bin,
)
from fetch_mtproto.v2ray.port_cleanup import (
    DEFAULT_POOL_TEST_BASE_PORT,
    cleanup_pool_xray,
    pool_test_ports,
)
from fetch_mtproto.v2ray.store import V2RayServer, _server_from_row, is_nekoray_compatible
from fetch_mtproto.v2ray.xray import (
    XRAY_SCHEMES,
    XrayRouteEntry,
    build_xray_multi_pool_config,
    build_xray_routed_config,
    format_traffic_bytes,
    link_to_xray_outbound,
)

LogFn = Callable[[str], None]
StatusFn = Callable[[list["ProxySlotStatus"]], None]
FinishedFn = Callable[[], None]
SlotStartResult = Literal["ok", "failed", "slow"]

PORTS_PER_SLOT = 2
# Stats API port offset from pool start_port (one API for the shared pool process).
API_PORT_OFFSET = 10000
TRAFFIC_POLL_SEC = 2.0
# How many upstream candidates to try per slot before giving up.
MAX_VALIDATE_ATTEMPTS_PER_SLOT = 16
# Wait when the catalog has no servers before retrying.
EMPTY_CATALOG_WAIT_SEC = 60.0
# Defaults; overridden by config.yaml proxy_pool.* when the runner is started.
DEFAULT_REUSE_AFTER_ROTATIONS = 5
DEFAULT_REUSE_AFTER_SEC = 20 * 60
DEFAULT_MAX_LATENCY_MS = 2000
# Trust catalog latency for pre-tested servers checked within this window (hours).
TRUST_CATALOG_MAX_AGE_H = 0.5


@dataclass(slots=True)
class ProxySlotStatus:
    socks_port: int
    http_port: int
    host: str
    scheme: str
    latency_ms: float | None
    running: bool
    error: str | None = None
    upload_bytes: int = 0
    download_bytes: int = 0

    @property
    def upload_text(self) -> str:
        return format_traffic_bytes(self.upload_bytes)

    @property
    def download_text(self) -> str:
        return format_traffic_bytes(self.download_bytes)


@dataclass
class _ProxySlot:
    socks_port: int
    http_port: int
    server: V2RayServer | None = None
    error: str | None = None
    # Lifetime totals for this pool session (survive upstream rotations).
    base_upload: int = 0
    base_download: int = 0
    upload_bytes: int = 0
    download_bytes: int = 0


@dataclass(slots=True)
class _UsageRecord:
    rotation: int
    used_at: float


class ProxyPoolRunner:
    """Manage N local proxy slots via two Xray processes (pool + validation)."""

    def __init__(
        self,
        *,
        start_port: int,
        count: int,
        switch_interval_sec: float,
        xray_bin: str | None = None,
        reuse_after_rotations: int = DEFAULT_REUSE_AFTER_ROTATIONS,
        reuse_after_sec: float = DEFAULT_REUSE_AFTER_SEC,
        max_latency_ms: float = DEFAULT_MAX_LATENCY_MS,
        random_pick: bool = True,
        test_base_port: int = DEFAULT_POOL_TEST_BASE_PORT,
        log: LogFn | None = None,
        on_status: StatusFn | None = None,
        on_finished: FinishedFn | None = None,
    ) -> None:
        self.start_port = start_port
        self.count = count
        self.switch_interval_sec = switch_interval_sec
        self.xray_bin = xray_bin
        self.reuse_after_rotations = max(1, int(reuse_after_rotations))
        self.reuse_after_sec = max(1.0, float(reuse_after_sec))
        self.max_latency_ms = max(1.0, float(max_latency_ms))
        self.random_pick = bool(random_pick)
        self.test_base_port = int(test_base_port)
        self._log = log or (lambda _msg: None)
        self._on_status = on_status
        self._on_finished = on_finished
        self._latency_by_key: dict[str, float] = {}
        self._checked_at_by_key: dict[str, str] = {}

        self._thread: threading.Thread | None = None
        self._traffic_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._slots_lock = threading.Lock()
        self._assigning_slots = False
        self._slots: list[_ProxySlot] = []
        self._servers: list[V2RayServer] = []
        self._recovery_servers: list[V2RayServer] = []
        self._rotation_round = 0
        self._bin_path: str | None = None
        # key -> last use (rotation index + wall clock)
        self._usage: dict[str, _UsageRecord] = {}
        # Keys assigned on the previous (still-running) round.
        self._previous_keys: list[str] = []
        self._current_keys: list[str] = []

        # Shared pool process (all 10801+ slots) + dedicated validation process.
        self._pool_process: subprocess.Popen | None = None
        self._pool_cfg_path: Path | None = None
        self._api_port = self.start_port + API_PORT_OFFSET
        self._test_process: subprocess.Popen | None = None
        self._test_cfg_path: Path | None = None
        test_ports = pool_test_ports(self.test_base_port)
        self._test_socks_port = test_ports[0]
        self._test_http_port = test_ports[1]

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _pool_process_alive(self) -> bool:
        return self._pool_process is not None and self._pool_process.poll() is None

    @staticmethod
    def ports_for_slot(start_port: int, slot_index: int) -> tuple[int, int]:
        base = start_port + slot_index * PORTS_PER_SLOT
        return base, base + 1

    @staticmethod
    def api_port_for_start(start_port: int) -> int:
        return start_port + API_PORT_OFFSET

    @staticmethod
    def last_port(start_port: int, count: int) -> int:
        if count <= 0:
            return start_port
        _socks, http = ProxyPoolRunner.ports_for_slot(start_port, count - 1)
        return http

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="proxy-pool", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal stop and kill Xray processes immediately (non-blocking-friendly)."""
        self._stop_event.set()
        # Kill now so port waits abort and the worker can exit quickly.
        self._kill_all_processes()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
            if not thread.is_alive():
                self._thread = None

    def _run(self) -> None:
        try:
            self._bin_path = resolve_xray_bin(self.xray_bin)
            if not self._bin_path:
                self._log(
                    "[proxy pool] xray binary not found — run setup or set xray.bin in config.yaml"
                )
                return

            killed = cleanup_pool_xray(start_port=self.start_port, count=self.count)
            if killed:
                self._log(
                    f"[proxy pool] cleared {len(killed)} leftover xray process(es) "
                    f"on pool ports {self.start_port}+"
                )

            self._api_port = self.api_port_for_start(self.start_port)
            with self._slots_lock:
                self._slots = []
                for index in range(self.count):
                    socks_port, http_port = self.ports_for_slot(self.start_port, index)
                    self._slots.append(
                        _ProxySlot(socks_port=socks_port, http_port=http_port)
                    )
            self._rotation_round = 0
            self._usage.clear()
            self._previous_keys = []
            self._current_keys = []

            while not self._stop_event.is_set():
                self._refresh_servers()
                if not self._servers and not self._recovery_servers:
                    self._log(
                        "[proxy pool] no V2Ray servers in catalog — "
                        f"retrying in {int(EMPTY_CATALOG_WAIT_SEC)}s"
                    )
                    if self._stop_event.wait(EMPTY_CATALOG_WAIT_SEC):
                        return
                    continue

                if self._rotation_round == 0:
                    if not self._servers:
                        self._log(
                            f"[proxy pool] no pre-tested servers ≤ "
                            f"{self.max_latency_ms:.0f} ms — live-testing catalog"
                        )
                    elif len(self._servers) < self.count:
                        self._log(
                            f"[proxy pool] only {len(self._servers)} tested server(s) ≤ "
                            f"{self.max_latency_ms:.0f} ms for {self.count} slot(s); "
                            "will probe more from catalog when needed"
                        )

                self._start_all_slots(initial=self._rotation_round == 0)
                if self._stop_event.is_set():
                    return
                self._emit_status()

                if self._traffic_thread is None or not self._traffic_thread.is_alive():
                    self._traffic_thread = threading.Thread(
                        target=self._traffic_loop,
                        name="proxy-pool-traffic",
                        daemon=True,
                    )
                    self._traffic_thread.start()

                if self._stop_event.wait(self.switch_interval_sec):
                    return
                self._rotation_round += 1
                self._log(
                    f"[proxy pool] rotating upstream servers (round {self._rotation_round})"
                )

        finally:
            self._cleanup_all()
            self._emit_status()
            self._log("[proxy pool] stopped")
            if self._on_finished is not None:
                self._on_finished()

    def _refresh_servers(self) -> None:
        self._servers, primary_latency, primary_checked = self._load_primary_servers()
        self._recovery_servers, recovery_latency, recovery_checked = (
            self._load_recovery_servers()
        )
        self._latency_by_key = {**recovery_latency, **primary_latency}
        self._checked_at_by_key = {**recovery_checked, **primary_checked}

    def _trust_catalog_latency(
        self, server: V2RayServer, *, allow_slow: bool, initial: bool
    ) -> bool:
        """Skip a live HTTP probe when Ping V2Ray recently verified this server."""
        if initial or allow_slow:
            return False
        lat = self._latency_by_key.get(server.key)
        if lat is None or lat > self.max_latency_ms:
            return False
        checked = self._checked_at_by_key.get(server.key)
        if not checked:
            return False
        return hours_since(checked) <= TRUST_CATALOG_MAX_AGE_H

    def _filter_compatible_rows(
        self, rows: list
    ) -> tuple[list[V2RayServer], dict[str, float], dict[str, str]]:
        servers: list[V2RayServer] = []
        latency_by_key: dict[str, float] = {}
        checked_at_by_key: dict[str, str] = {}
        for row in rows:
            server = _server_from_row(row)
            if server.scheme not in XRAY_SCHEMES:
                continue
            if not is_nekoray_compatible(server):
                continue
            if link_to_xray_outbound(server) is None:
                continue
            servers.append(server)
            lat = row["last_latency_ms"]
            if lat is not None:
                latency_by_key[server.key] = float(lat)
            checked = row["last_checked_at"]
            if checked:
                checked_at_by_key[server.key] = str(checked)
        return servers, latency_by_key, checked_at_by_key

    def _load_primary_servers(
        self,
    ) -> tuple[list[V2RayServer], dict[str, float], dict[str, str]]:
        """Working servers with catalog latency ≤ max."""
        config = load_config(required=False)
        db, _mt, _v2 = open_catalogs(config)
        try:
            rows = db.conn.execute(
                """
                SELECT * FROM v2ray
                WHERE status = 'working'
                  AND last_latency_ms IS NOT NULL
                  AND last_latency_ms <= ?
                ORDER BY last_latency_ms ASC, key
                """,
                (self.max_latency_ms,),
            ).fetchall()
            return self._filter_compatible_rows(rows)
        finally:
            db.close()

    def _load_recovery_servers(
        self,
    ) -> tuple[list[V2RayServer], dict[str, float], dict[str, str]]:
        """All known catalog servers (working first, then history) for live re-test."""
        config = load_config(required=False)
        db, _mt, _v2 = open_catalogs(config)
        try:
            rows = db.conn.execute(
                """
                SELECT * FROM v2ray
                ORDER BY
                    CASE status WHEN 'working' THEN 0 ELSE 1 END,
                    CASE WHEN last_latency_ms IS NULL THEN 1 ELSE 0 END,
                    last_latency_ms ASC,
                    success_count DESC,
                    priority_score DESC,
                    key
                """
            ).fetchall()
            return self._filter_compatible_rows(rows)
        finally:
            db.close()

    def _is_reusable(self, key: str, *, now: float, rotation: int) -> bool:
        record = self._usage.get(key)
        if record is None:
            return True
        rotations_ago = rotation - record.rotation
        age_sec = now - record.used_at
        return (
            rotations_ago >= self.reuse_after_rotations
            or age_sec >= self.reuse_after_sec
        )

    def _server_latency(self, server: V2RayServer) -> float:
        return self._latency_by_key.get(server.key, self.max_latency_ms)

    def _ordered_candidates(self, pool: list[V2RayServer]) -> list[V2RayServer]:
        """Eligible servers ordered for assignment (off cooldown first)."""
        if not pool:
            return []

        now = time.monotonic()
        rotation = self._rotation_round
        previous_blocked = {
            key
            for key in self._previous_keys
            if not self._is_reusable(key, now=now, rotation=rotation)
        }

        available: list[V2RayServer] = []
        cooling: list[tuple[float, V2RayServer]] = []
        for server in pool:
            if server.key in previous_blocked:
                record = self._usage.get(server.key)
                score = record.used_at if record else 0.0
                cooling.append((score, server))
                continue
            if self._is_reusable(server.key, now=now, rotation=rotation):
                available.append(server)
            else:
                record = self._usage[server.key]
                cooling.append((record.used_at, server))

        if self.random_pick:
            random.shuffle(available)
            random.shuffle(cooling)
        else:
            available.sort(key=self._server_latency)
        cooling.sort(key=lambda item: item[0])

        ordered: list[V2RayServer] = []
        seen: set[str] = set()
        for server in available:
            if server.key in seen:
                continue
            ordered.append(server)
            seen.add(server.key)
        reused = 0
        for _score, server in cooling:
            if server.key in seen:
                continue
            ordered.append(server)
            seen.add(server.key)
            reused += 1
        if reused:
            self._log(
                f"[proxy pool] only {len(available)} server(s) off cooldown; "
                f"{reused} still cooling (may reuse if needed)"
            )
        return ordered

    def _candidate_pools(self) -> list[tuple[str, list[V2RayServer]]]:
        """Primary pre-tested pool, then full catalog for live re-test."""
        pools: list[tuple[str, list[V2RayServer]]] = []
        if self._servers:
            pools.append(("pre-tested", self._servers))
        if self._recovery_servers:
            pools.append(("catalog", self._recovery_servers))
        return pools

    def _candidates_for_slot(
        self,
        ordered: list[V2RayServer],
        *,
        blocked: set[str],
        used: set[str],
    ) -> list[V2RayServer]:
        preferred = [
            server
            for server in ordered
            if server.key not in blocked and server.key not in used
        ]
        fallback = [
            server
            for server in ordered
            if server.key not in blocked and server.key in used
        ]
        return preferred + fallback

    def _slot_is_healthy(self, slot: _ProxySlot) -> bool:
        return (
            self._pool_process_alive()
            and slot.error is None
            and slot.server is not None
        )

    def _test_settings(self) -> tuple[str, float]:
        config = load_config(required=False)
        test_url = DEFAULT_TEST_URL
        timeout = DEFAULT_TEST_TIMEOUT
        if config is not None:
            test_url = str(getattr(config, "V2RAY_TEST_URL", DEFAULT_TEST_URL))
            raw_timeout = getattr(config, "V2RAY_TEST_TIMEOUT", DEFAULT_TEST_TIMEOUT)
            try:
                timeout = float(raw_timeout)
            except (TypeError, ValueError):
                timeout = DEFAULT_TEST_TIMEOUT
        return test_url, max(1.0, timeout)

    def _validate_upstream(
        self, http_port: int
    ) -> tuple[bool, float | None, str | None]:
        """HTTP GET through a local HTTP proxy. Returns (ok, latency_s, error)."""
        test_url, timeout = self._test_settings()
        proxy_url = f"http://127.0.0.1:{http_port}"
        handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        opener = urllib.request.build_opener(handler)
        started = time.perf_counter()
        try:
            with opener.open(test_url, timeout=timeout) as resp:
                status = getattr(resp, "status", None)
                if status is None:
                    status = resp.getcode()
                resp.read(1024)
                if status is None or not (200 <= int(status) < 400):
                    return False, None, f"HTTP {status}"
                return True, time.perf_counter() - started, None
        except Exception as exc:
            detail = str(exc) or type(exc).__name__
            if isinstance(exc, urllib.error.URLError) and exc.reason:
                detail = str(exc.reason)
            return False, None, detail

    def _record_probe(
        self,
        server: V2RayServer,
        *,
        ok: bool,
        latency_s: float | None,
        error: str | None,
    ) -> None:
        config = load_config(required=False)
        db, _mt, _v2 = open_catalogs(config)
        try:
            db.v2ray_record_result(
                server.key,
                ok=ok,
                latency_s=latency_s,
                error=error,
                identity=server.as_db_row(),
            )
        finally:
            db.close()

    def _record_usage(self, servers: list[V2RayServer]) -> None:
        now = time.monotonic()
        self._previous_keys = list(self._current_keys)
        self._current_keys = [server.key for server in servers]
        for server in servers:
            self._usage[server.key] = _UsageRecord(
                rotation=self._rotation_round,
                used_at=now,
            )

    def _temp_cfg_path(self, prefix: str) -> Path:
        return Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / (
            f"{prefix}-{int(time.time() * 1000)}.json"
        )

    def _stop_process(
        self, proc: subprocess.Popen | None, cfg_path: Path | None
    ) -> None:
        if proc is not None and proc.poll() is None:
            kill_process_tree(proc)
        if cfg_path is not None:
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _stop_test_process(self) -> None:
        self._stop_process(self._test_process, self._test_cfg_path)
        self._test_process = None
        self._test_cfg_path = None

    def _stop_pool_process(self) -> None:
        self._commit_all_traffic()
        self._stop_process(self._pool_process, self._pool_cfg_path)
        self._pool_process = None
        self._pool_cfg_path = None

    def _start_xray(self, config: dict, *, prefix: str) -> tuple[subprocess.Popen, Path]:
        if not self._bin_path:
            raise RuntimeError("xray binary not resolved")
        cfg_path = self._temp_cfg_path(prefix)
        cfg_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
        try:
            proc = subprocess.Popen(
                [self._bin_path, "run", "-c", str(cfg_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **hide_console_kwargs(),
            )
        except OSError:
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return proc, cfg_path

    def _validate_on_test_process(
        self,
        server: V2RayServer,
        *,
        allow_slow: bool = False,
        skip_live_test: bool = False,
    ) -> tuple[SlotStartResult, str | None]:
        """Validate a candidate on the dedicated test Xray process (not pool ports)."""
        if self._stop_event.is_set():
            return "failed", "stopped"

        if skip_live_test:
            latency_ms = self._latency_by_key.get(server.key, self.max_latency_ms)
            if latency_ms > self.max_latency_ms and not allow_slow:
                return (
                    "slow",
                    f"latency {latency_ms:.0f} ms > {self.max_latency_ms:.0f} ms",
                )
            return "ok", None

        outbound = link_to_xray_outbound(server)
        if outbound is None:
            return "failed", f"unsupported scheme: {server.scheme}"

        self._stop_test_process()
        if self._stop_event.is_set():
            return "failed", "stopped"

        config = build_xray_routed_config(
            [
                XrayRouteEntry(
                    outbound=outbound,
                    tag="proxy-test",
                    socks_port=self._test_socks_port,
                    http_port=self._test_http_port,
                )
            ]
        )
        try:
            proc, cfg_path = self._start_xray(config, prefix="fetch-mtproto-pool-test")
        except OSError as exc:
            return "failed", f"xray start failed: {exc}"

        self._test_process = proc
        self._test_cfg_path = cfg_path

        if self._stop_event.is_set():
            self._stop_test_process()
            return "failed", "stopped"

        if not self._wait_port("127.0.0.1", self._test_http_port, timeout=8.0):
            self._stop_test_process()
            if self._stop_event.is_set():
                return "failed", "stopped"
            return "failed", "test HTTP port did not open"

        ok, latency_s, error = self._validate_upstream(self._test_http_port)
        # Keep test process only while validating; free ports between candidates.
        self._stop_test_process()

        if not ok or latency_s is None:
            detail = error or "upstream check failed"
            self._record_probe(server, ok=False, latency_s=None, error=detail)
            return "failed", detail

        latency_ms = latency_s * 1000.0
        self._record_probe(server, ok=True, latency_s=latency_s, error=None)
        self._latency_by_key[server.key] = latency_ms
        if latency_ms > self.max_latency_ms and not allow_slow:
            return (
                "slow",
                f"latency {latency_ms:.0f} ms > {self.max_latency_ms:.0f} ms",
            )
        return "ok", None

    def _apply_pool_assignments(self, assignments: list[V2RayServer | None]) -> None:
        """Restart the shared pool Xray once with the given per-slot upstreams."""
        if self._stop_event.is_set():
            return

        with self._slots_lock:
            slots = list(self._slots)
        if len(assignments) != len(slots):
            raise ValueError("assignment count must match slot count")

        outbounds: list[dict | None] = []
        for server in assignments:
            if server is None:
                outbounds.append(None)
                continue
            outbound = link_to_xray_outbound(server)
            outbounds.append(outbound)

        active = [ob for ob in outbounds if ob is not None]
        self._stop_pool_process()
        if self._stop_event.is_set():
            return

        if not active:
            for slot, server in zip(slots, assignments):
                slot.server = server
                slot.error = "no working upstream found"
            return

        config = build_xray_multi_pool_config(
            outbounds,
            start_port=self.start_port,
            api_port=self._api_port,
        )
        try:
            proc, cfg_path = self._start_xray(config, prefix="fetch-mtproto-pool")
        except OSError as exc:
            for slot in slots:
                slot.error = f"xray start failed: {exc}"
            return

        self._pool_process = proc
        self._pool_cfg_path = cfg_path

        for index, (slot, server) in enumerate(zip(slots, assignments)):
            slot.server = server
            if server is None:
                slot.error = "no working upstream found"
                continue
            if outbounds[index] is None:
                slot.error = f"unsupported scheme: {server.scheme}"
                slot.server = None
                continue
            if not self._wait_port("127.0.0.1", slot.socks_port, timeout=8.0):
                if self._stop_event.is_set():
                    return
                slot.error = "SOCKS5 port did not open"
                continue
            if not self._wait_port("127.0.0.1", slot.http_port, timeout=8.0):
                if self._stop_event.is_set():
                    return
                slot.error = "HTTP port did not open"
                continue
            slot.error = None

        if self._stop_event.is_set():
            self._stop_pool_process()

    def _pick_validated_server(
        self,
        index: int,
        slot: _ProxySlot,
        *,
        initial: bool,
        rotate: bool,
        blocked: set[str],
        used: set[str],
    ) -> V2RayServer | None:
        """Try candidates on the test process; return a validated upstream or None."""
        for label, pool in self._candidate_pools():
            ordered = self._ordered_candidates(pool)
            if not ordered:
                continue
            candidates = self._candidates_for_slot(
                ordered, blocked=blocked, used=used
            )
            if not candidates and ordered:
                candidates = [
                    server for server in ordered if server.key not in blocked
                ]
            attempts = 0
            for server in candidates:
                if self._stop_event.is_set():
                    return None
                if attempts >= MAX_VALIDATE_ATTEMPTS_PER_SLOT:
                    break
                attempts += 1
                if initial or rotate:
                    if initial:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"SOCKS5 127.0.0.1:{slot.socks_port}, "
                            f"HTTP 127.0.0.1:{slot.http_port} → "
                            f"testing {server.host}:{server.port} ({server.scheme}) "
                            f"[{label}]"
                        )
                    else:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"testing {server.host}:{server.port} ({server.scheme}) "
                            f"[{label}]"
                        )
                else:
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"retry {server.host}:{server.port} ({server.scheme}) "
                        f"[{label}]"
                    )
                allow_slow = label == "catalog"
                trust_catalog = self._trust_catalog_latency(
                    server, allow_slow=allow_slow, initial=initial
                )
                result, detail = self._validate_on_test_process(
                    server,
                    allow_slow=allow_slow,
                    skip_live_test=trust_catalog,
                )
                if result == "ok":
                    latency = self._latency_by_key.get(server.key)
                    latency_txt = (
                        f"{latency:.0f} ms" if latency is not None else "ok"
                    )
                    if trust_catalog:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"validated {server.host}:{server.port} "
                            f"({latency_txt}, catalog)"
                        )
                    elif allow_slow and latency is not None and latency > self.max_latency_ms:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"using slow upstream {server.host}:{server.port} "
                            f"({latency_txt}) — no faster servers available"
                        )
                    else:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"validated {server.host}:{server.port} ({latency_txt})"
                        )
                    used.add(server.key)
                    return server
                if result == "slow":
                    blocked.add(server.key)
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"skipped {server.host}:{server.port} "
                        f"(above {self.max_latency_ms:.0f} ms)"
                    )
                    continue
                blocked.add(server.key)
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    f"rejected {server.host}:{server.port} — "
                    f"{detail or 'upstream check failed'}"
                )
        return None

    def _start_all_slots(self, *, initial: bool) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        if not slots:
            return

        if not self._candidate_pools():
            self._log(
                "[proxy pool] no servers to assign — will retry on next cycle"
            )
            return

        cooling = sum(
            1
            for key, record in self._usage.items()
            if not self._is_reusable(
                key, now=time.monotonic(), rotation=self._rotation_round
            )
        )
        mode = "random" if self.random_pick else "fastest-first"
        primary_n = len(self._servers)
        recovery_n = len(self._recovery_servers)
        self._log(
            f"[proxy pool] validating upstreams ({mode}): "
            f"{primary_n} pre-tested, {recovery_n} in catalog; "
            f"{cooling} server(s) on cooldown "
            f"(reuse after {self.reuse_after_rotations} rotations or "
            f"{int(self.reuse_after_sec) // 60} min)"
        )

        self._assigning_slots = True
        try:
            blocked: set[str] = set()
            used: set[str] = set()
            assigned: list[V2RayServer] = []
            assignments: list[V2RayServer | None] = []

            for index, slot in enumerate(slots):
                if self._stop_event.is_set():
                    return
                # Rotate healthy slots on schedule; always try to fill dead ones.
                rotate = initial or self._slot_is_healthy(slot)
                server = self._pick_validated_server(
                    index,
                    slot,
                    initial=initial,
                    rotate=rotate,
                    blocked=blocked,
                    used=used,
                )
                if server is not None:
                    assignments.append(server)
                    assigned.append(server)
                else:
                    assignments.append(None)
                    if rotate:
                        self._log(
                            f"[proxy pool] slot {index + 1}/{self.count}: "
                            f"no valid upstream this cycle — will retry"
                        )

            self._apply_pool_assignments(assignments)
            if assigned:
                self._record_usage(assigned)
        finally:
            self._assigning_slots = False
            self._stop_test_process()

        running = sum(1 for slot in slots if self._slot_is_healthy(slot))
        self._log(
            f"[proxy pool] {running}/{len(slots)} slot(s) running with "
            f"validated upstreams (1 pool xray + 1 test xray)"
        )

    def _repair_dead_slots(self) -> None:
        """Immediately retry failed or crashed slots without waiting for rotation."""
        if self._assigning_slots:
            return
        with self._slots_lock:
            slots = list(self._slots)
        if not slots:
            return

        # Shared process crash: re-apply known assignments without re-validating.
        if not self._pool_process_alive() and any(slot.server for slot in slots):
            self._log("[proxy pool] pool xray exited — restarting with current upstreams")
            self._apply_pool_assignments([slot.server for slot in slots])
            self._emit_status()
            if self._pool_process_alive():
                return

        if not self._candidate_pools():
            return

        blocked: set[str] = set()
        used: set[str] = set()
        assigned: list[V2RayServer] = []
        assignments: list[V2RayServer | None] = []
        changed = False

        for index, slot in enumerate(slots):
            if self._stop_event.is_set():
                return
            if self._slot_is_healthy(slot):
                assignments.append(slot.server)
                if slot.server is not None:
                    used.add(slot.server.key)
                continue

            server = self._pick_validated_server(
                index,
                slot,
                initial=False,
                rotate=False,
                blocked=blocked,
                used=used,
            )
            if server is not None:
                assignments.append(server)
                assigned.append(server)
                changed = True
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    "recovered after failure"
                )
            else:
                assignments.append(None)
                changed = True

        if changed:
            self._apply_pool_assignments(assignments)
            if assigned:
                self._record_usage(assigned)
            self._emit_status()
        self._stop_test_process()

    def _commit_all_traffic(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        for slot in slots:
            self._commit_slot_traffic(slot)

    def _commit_slot_traffic(self, slot: _ProxySlot) -> None:
        """Fold current Xray counters into lifetime totals before process restart."""
        self._poll_slot_traffic(slot)
        slot.base_upload = slot.upload_bytes
        slot.base_download = slot.download_bytes

    def _traffic_loop(self) -> None:
        while not self._stop_event.wait(TRAFFIC_POLL_SEC):
            self._refresh_all_traffic()
            self._repair_dead_slots()
            self._emit_status()

    def _refresh_all_traffic(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        if not self._pool_process_alive():
            return
        stats = self._query_all_outbound_traffic()
        for index, slot in enumerate(slots):
            if self._stop_event.is_set():
                return
            up, down = stats.get(index, (0, 0))
            slot.upload_bytes = slot.base_upload + up
            slot.download_bytes = slot.base_download + down

    def _poll_slot_traffic(self, slot: _ProxySlot) -> None:
        if not self._pool_process_alive():
            return
        with self._slots_lock:
            try:
                index = self._slots.index(slot)
            except ValueError:
                return
        stats = self._query_all_outbound_traffic()
        up, down = stats.get(index, (0, 0))
        slot.upload_bytes = slot.base_upload + up
        slot.download_bytes = slot.base_download + down

    def _query_all_outbound_traffic(self) -> dict[int, tuple[int, int]]:
        """Return slot_index -> (uplink, downlink) from the shared Stats API."""
        if not self._bin_path or self._api_port <= 0:
            return {}
        try:
            result = subprocess.run(
                [
                    self._bin_path,
                    "api",
                    "statsquery",
                    f"--server=127.0.0.1:{self._api_port}",
                ],
                capture_output=True,
                text=True,
                timeout=2.0,
                **hide_console_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired):
            return {}
        if result.returncode != 0 or not result.stdout.strip():
            return {}
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}

        by_index: dict[int, list[int]] = {}
        for item in payload.get("stat") or []:
            name = str(item.get("name") or "")
            try:
                value = int(item.get("value") or 0)
            except (TypeError, ValueError):
                value = 0
            # outbound>>>proxy-3>>>traffic>>>uplink
            if not name.startswith("outbound>>>proxy-"):
                continue
            parts = name.split(">>>")
            if len(parts) != 4 or parts[2] != "traffic":
                continue
            tag = parts[1]
            direction = parts[3]
            if not tag.startswith("proxy-"):
                continue
            try:
                index = int(tag.split("-", 1)[1])
            except (TypeError, ValueError):
                continue
            pair = by_index.setdefault(index, [0, 0])
            if direction == "uplink":
                pair[0] = value
            elif direction == "downlink":
                pair[1] = value
        return {index: (pair[0], pair[1]) for index, pair in by_index.items()}

    def _kill_all_processes(self) -> None:
        self._stop_test_process()
        self._stop_pool_process()

    def _cleanup_all(self) -> None:
        self._kill_all_processes()
        with self._slots_lock:
            self._slots = []

    def _wait_port(self, host: str, port: int, *, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._stop_event.is_set():
                return False
            try:
                with socket.create_connection((host, port), timeout=0.2):
                    return True
            except OSError:
                time.sleep(0.05)
        return False

    def snapshot_statuses(self) -> list[ProxySlotStatus]:
        """Return current slot statuses for UI tests / display."""
        latency_by_key = dict(self._latency_by_key)
        pool_alive = self._pool_process_alive()
        with self._slots_lock:
            slots = list(self._slots)
        statuses: list[ProxySlotStatus] = []
        for slot in slots:
            server = slot.server
            statuses.append(
                ProxySlotStatus(
                    socks_port=slot.socks_port,
                    http_port=slot.http_port,
                    host=server.host if server else "—",
                    scheme=server.scheme if server else "—",
                    latency_ms=latency_by_key.get(server.key) if server else None,
                    running=pool_alive and server is not None and slot.error is None,
                    error=slot.error,
                    upload_bytes=slot.upload_bytes,
                    download_bytes=slot.download_bytes,
                )
            )
        return statuses

    def _emit_status(self) -> None:
        if self._on_status is None:
            return
        self._on_status(self.snapshot_statuses())
