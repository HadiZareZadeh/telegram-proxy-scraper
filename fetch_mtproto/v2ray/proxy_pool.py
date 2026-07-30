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
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import (
    DEFAULT_TEST_TIMEOUT,
    DEFAULT_TEST_URL,
    resolve_xray_bin,
)
from fetch_mtproto.v2ray.port_cleanup import cleanup_pool_xray
from fetch_mtproto.v2ray.store import V2RayServer, _server_from_row, is_nekoray_compatible
from fetch_mtproto.v2ray.xray import (
    XRAY_SCHEMES,
    build_xray_pool_config,
    format_traffic_bytes,
    link_to_xray_outbound,
)

LogFn = Callable[[str], None]
StatusFn = Callable[[list["ProxySlotStatus"]], None]
FinishedFn = Callable[[], None]
SlotStartResult = Literal["ok", "failed", "slow"]

PORTS_PER_SLOT = 2
# Stats API port offset from SOCKS port (keeps user-facing SOCKS/HTTP layout).
API_PORT_OFFSET = 10000
TRAFFIC_POLL_SEC = 2.0
# How many upstream candidates to try per slot before giving up.
MAX_VALIDATE_ATTEMPTS_PER_SLOT = 8
# Defaults; overridden by config.yaml proxy_pool.* when the runner is started.
DEFAULT_REUSE_AFTER_ROTATIONS = 5
DEFAULT_REUSE_AFTER_SEC = 20 * 60
DEFAULT_MAX_LATENCY_MS = 2000


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
    api_port: int = 0
    process: subprocess.Popen | None = None
    cfg_path: Path | None = None
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
    """Manage N local Xray instances (SOCKS5 + HTTP each) with upstream rotation."""

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
        self._log = log or (lambda _msg: None)
        self._on_status = on_status
        self._on_finished = on_finished
        self._latency_by_key: dict[str, float] = {}

        self._thread: threading.Thread | None = None
        self._traffic_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._slots_lock = threading.Lock()
        self._slots: list[_ProxySlot] = []
        self._servers: list[V2RayServer] = []
        self._rotation_round = 0
        self._bin_path: str | None = None
        # key -> last use (rotation index + wall clock)
        self._usage: dict[str, _UsageRecord] = {}
        # Keys assigned on the previous (still-running) round.
        self._previous_keys: list[str] = []
        self._current_keys: list[str] = []

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @staticmethod
    def ports_for_slot(start_port: int, slot_index: int) -> tuple[int, int]:
        base = start_port + slot_index * PORTS_PER_SLOT
        return base, base + 1

    @staticmethod
    def api_port_for_socks(socks_port: int) -> int:
        return socks_port + API_PORT_OFFSET

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
        # Keep slot objects — the worker may still hold them and will finish cleanup.
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

            self._refresh_servers()
            if not self._servers:
                self._log(
                    f"[proxy pool] no working Xray-compatible V2Ray servers with "
                    f"latency ≤ {self.max_latency_ms:.0f} ms — run Ping V2Ray first "
                    f"(pool only uses tested servers)"
                )
                return

            if len(self._servers) < self.count:
                self._log(
                    f"[proxy pool] only {len(self._servers)} tested server(s) ≤ "
                    f"{self.max_latency_ms:.0f} ms for {self.count} slot(s); "
                    f"some upstreams may be reused after live re-check"
                )

            with self._slots_lock:
                self._slots = []
                for index in range(self.count):
                    socks_port, http_port = self.ports_for_slot(self.start_port, index)
                    self._slots.append(
                        _ProxySlot(
                            socks_port=socks_port,
                            http_port=http_port,
                            api_port=self.api_port_for_socks(socks_port),
                        )
                    )
            self._rotation_round = 0
            self._usage.clear()
            self._previous_keys = []
            self._current_keys = []
            self._start_all_slots(initial=True)
            if self._stop_event.is_set():
                return
            self._emit_status()
            self._traffic_thread = threading.Thread(
                target=self._traffic_loop,
                name="proxy-pool-traffic",
                daemon=True,
            )
            self._traffic_thread.start()

            while not self._stop_event.wait(self.switch_interval_sec):
                self._rotation_round += 1
                self._log(
                    f"[proxy pool] rotating upstream servers (round {self._rotation_round})"
                )
                self._refresh_servers()
                self._start_all_slots(initial=False)
                if self._stop_event.is_set():
                    return
                self._emit_status()

        finally:
            self._cleanup_all()
            self._emit_status()
            self._log("[proxy pool] stopped")
            if self._on_finished is not None:
                self._on_finished()

    def _refresh_servers(self) -> None:
        self._servers = self._load_servers()

    def _load_servers(self) -> list[V2RayServer]:
        """Load working servers with latency ≤ max — independent of subscription ranking."""
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
            servers: list[V2RayServer] = []
            latency_by_key: dict[str, float] = {}
            for row in rows:
                server = _server_from_row(row)
                if server.scheme not in XRAY_SCHEMES:
                    continue
                if not is_nekoray_compatible(server):
                    continue
                if link_to_xray_outbound(server) is None:
                    continue
                servers.append(server)
                latency_by_key[server.key] = float(row["last_latency_ms"])
            self._latency_by_key = latency_by_key
            return servers
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

    def _ordered_candidates(self) -> list[V2RayServer]:
        """Eligible servers ordered for assignment (off cooldown first)."""
        if not self._servers:
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
        for server in self._servers:
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

    def _drop_server(self, key: str) -> None:
        self._servers = [server for server in self._servers if server.key != key]
        self._latency_by_key.pop(key, None)

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
        """HTTP GET through the slot's local HTTP proxy. Returns (ok, latency_s, error)."""
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

    def _start_all_slots(self, *, initial: bool) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        if not slots:
            return

        ordered = self._ordered_candidates()
        if not ordered:
            self._log("[proxy pool] no servers to assign")
            return

        cooling = sum(
            1
            for key, record in self._usage.items()
            if key not in {server.key for server in ordered[: len(slots)]}
            and not self._is_reusable(
                key, now=time.monotonic(), rotation=self._rotation_round
            )
        )
        mode = "random" if self.random_pick else "fastest-first"
        self._log(
            f"[proxy pool] validating upstreams ({mode}) from "
            f"{len(ordered)} candidate(s) ≤ {self.max_latency_ms:.0f} ms; "
            f"{cooling} server(s) on cooldown "
            f"(reuse after {self.reuse_after_rotations} rotations or "
            f"{int(self.reuse_after_sec) // 60} min)"
        )

        blocked: set[str] = set()
        used: set[str] = set()
        assigned: list[V2RayServer] = []

        for index, slot in enumerate(slots):
            if self._stop_event.is_set():
                return
            candidates = self._candidates_for_slot(
                ordered, blocked=blocked, used=used
            )
            if not candidates and ordered:
                # Allow duplicates when the catalog is smaller than the slot count.
                candidates = [
                    server for server in ordered if server.key not in blocked
                ]
            placed = False
            attempts = 0
            for server in candidates:
                if self._stop_event.is_set():
                    return
                if attempts >= MAX_VALIDATE_ATTEMPTS_PER_SLOT:
                    break
                attempts += 1
                if initial:
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"SOCKS5 127.0.0.1:{slot.socks_port}, "
                        f"HTTP 127.0.0.1:{slot.http_port} → "
                        f"testing {server.host}:{server.port} ({server.scheme})"
                    )
                else:
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"testing {server.host}:{server.port} ({server.scheme})"
                    )
                result = self._restart_slot(slot, server)
                if result == "ok":
                    latency = self._latency_by_key.get(server.key)
                    latency_txt = (
                        f"{latency:.0f} ms" if latency is not None else "ok"
                    )
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"validated {server.host}:{server.port} ({latency_txt})"
                    )
                    assigned.append(server)
                    used.add(server.key)
                    placed = True
                    break
                if result == "slow":
                    blocked.add(server.key)
                    self._drop_server(server.key)
                    self._log(
                        f"[proxy pool] slot {index + 1}/{self.count}: "
                        f"skipped {server.host}:{server.port} "
                        f"(above {self.max_latency_ms:.0f} ms)"
                    )
                    continue
                blocked.add(server.key)
                self._drop_server(server.key)
                detail = slot.error or "upstream check failed"
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    f"rejected {server.host}:{server.port} — {detail}"
                )

            if not placed:
                slot.server = None
                slot.error = "no working upstream found"
                self._stop_slot(slot)
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    f"no valid upstream after {attempts} attempt(s)"
                )

        if assigned:
            self._record_usage(assigned)
        self._log(
            f"[proxy pool] {len(assigned)}/{len(slots)} slot(s) running with "
            f"validated upstreams"
        )

    def _restart_slot(self, slot: _ProxySlot, server: V2RayServer) -> SlotStartResult:
        if self._stop_event.is_set():
            return "failed"
        self._commit_slot_traffic(slot)
        self._stop_slot(slot)
        if self._stop_event.is_set():
            return "failed"
        slot.error = None
        slot.server = server
        if not slot.api_port:
            slot.api_port = self.api_port_for_socks(slot.socks_port)

        outbound = link_to_xray_outbound(server)
        if outbound is None:
            slot.error = f"unsupported scheme: {server.scheme}"
            return "failed"

        config = build_xray_pool_config(
            outbound,
            slot.socks_port,
            slot.http_port,
            api_port=slot.api_port,
        )
        cfg_path = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / (
            f"fetch-mtproto-pool-{slot.socks_port}-{int(time.time() * 1000)}.json"
        )
        try:
            cfg_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            slot.error = f"config write failed: {exc}"
            return "failed"

        if self._stop_event.is_set():
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            return "failed"

        try:
            proc = subprocess.Popen(
                [self._bin_path, "run", "-c", str(cfg_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **hide_console_kwargs(),
            )
        except OSError as exc:
            slot.error = f"xray start failed: {exc}"
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            return "failed"

        if self._stop_event.is_set():
            kill_process_tree(proc)
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            return "failed"

        slot.process = proc
        slot.cfg_path = cfg_path
        if not self._wait_port("127.0.0.1", slot.socks_port, timeout=8.0):
            if not self._stop_event.is_set():
                slot.error = "SOCKS5 port did not open"
            self._stop_slot(slot)
            return "failed"
        if not self._wait_port("127.0.0.1", slot.http_port, timeout=8.0):
            if not self._stop_event.is_set():
                slot.error = "HTTP port did not open"
            self._stop_slot(slot)
            return "failed"

        if self._stop_event.is_set():
            self._stop_slot(slot)
            return "failed"

        ok, latency_s, error = self._validate_upstream(slot.http_port)
        if not ok or latency_s is None:
            slot.error = error or "upstream check failed"
            self._record_probe(server, ok=False, latency_s=None, error=slot.error)
            self._stop_slot(slot)
            return "failed"

        latency_ms = latency_s * 1000.0
        self._record_probe(server, ok=True, latency_s=latency_s, error=None)
        self._latency_by_key[server.key] = latency_ms
        if latency_ms > self.max_latency_ms:
            slot.error = (
                f"latency {latency_ms:.0f} ms > {self.max_latency_ms:.0f} ms"
            )
            self._stop_slot(slot)
            return "slow"
        return "ok"

    def _stop_slot(self, slot: _ProxySlot) -> None:
        if slot.process is not None and slot.process.poll() is None:
            kill_process_tree(slot.process)
        slot.process = None
        if slot.cfg_path is not None:
            try:
                slot.cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            slot.cfg_path = None

    def _commit_slot_traffic(self, slot: _ProxySlot) -> None:
        """Fold current Xray counters into lifetime totals before process restart."""
        self._poll_slot_traffic(slot)
        slot.base_upload = slot.upload_bytes
        slot.base_download = slot.download_bytes

    def _traffic_loop(self) -> None:
        while not self._stop_event.wait(TRAFFIC_POLL_SEC):
            self._refresh_all_traffic()
            self._emit_status()

    def _refresh_all_traffic(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        for slot in slots:
            if self._stop_event.is_set():
                return
            self._poll_slot_traffic(slot)

    def _poll_slot_traffic(self, slot: _ProxySlot) -> None:
        if slot.process is None or slot.process.poll() is not None:
            return
        up, down = self._query_outbound_traffic(slot.api_port)
        slot.upload_bytes = slot.base_upload + up
        slot.download_bytes = slot.base_download + down

    def _query_outbound_traffic(self, api_port: int) -> tuple[int, int]:
        """Return (uplink, downlink) bytes for outbound tag 'proxy' via xray API."""
        if not self._bin_path or api_port <= 0:
            return 0, 0
        try:
            result = subprocess.run(
                [
                    self._bin_path,
                    "api",
                    "statsquery",
                    f"--server=127.0.0.1:{api_port}",
                ],
                capture_output=True,
                text=True,
                timeout=2.0,
                **hide_console_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired):
            return 0, 0
        if result.returncode != 0 or not result.stdout.strip():
            return 0, 0
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return 0, 0
        uplink = 0
        downlink = 0
        for item in payload.get("stat") or []:
            name = str(item.get("name") or "")
            try:
                value = int(item.get("value") or 0)
            except (TypeError, ValueError):
                value = 0
            if name == "outbound>>>proxy>>>traffic>>>uplink":
                uplink = value
            elif name == "outbound>>>proxy>>>traffic>>>downlink":
                downlink = value
        return uplink, downlink

    def _kill_all_processes(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        for slot in slots:
            self._commit_slot_traffic(slot)
            self._stop_slot(slot)

    def _cleanup_all(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
            self._slots = []
        for slot in slots:
            self._commit_slot_traffic(slot)
            self._stop_slot(slot)

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
        with self._slots_lock:
            slots = list(self._slots)
        statuses: list[ProxySlotStatus] = []
        for slot in slots:
            server = slot.server
            running = slot.process is not None and slot.process.poll() is None
            statuses.append(
                ProxySlotStatus(
                    socks_port=slot.socks_port,
                    http_port=slot.http_port,
                    host=server.host if server else "—",
                    scheme=server.scheme if server else "—",
                    latency_ms=latency_by_key.get(server.key) if server else None,
                    running=running and slot.error is None,
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
