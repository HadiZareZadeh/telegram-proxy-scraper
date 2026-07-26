"""Run persistent local SOCKS5 + HTTP proxies backed by catalog V2Ray servers."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.store import V2RayServer, _server_from_row, is_nekoray_compatible
from fetch_mtproto.v2ray.xray import (
    XRAY_SCHEMES,
    build_xray_pool_config,
    link_to_xray_outbound,
)

LogFn = Callable[[str], None]
StatusFn = Callable[[list["ProxySlotStatus"]], None]
FinishedFn = Callable[[], None]

PORTS_PER_SLOT = 2
# A used upstream may be picked again once either threshold is met.
REUSE_AFTER_ROTATIONS = 5
REUSE_AFTER_SEC = 20 * 60


@dataclass(slots=True)
class ProxySlotStatus:
    socks_port: int
    http_port: int
    host: str
    scheme: str
    latency_ms: float | None
    running: bool
    error: str | None = None


@dataclass
class _ProxySlot:
    socks_port: int
    http_port: int
    process: subprocess.Popen | None = None
    cfg_path: Path | None = None
    server: V2RayServer | None = None
    error: str | None = None


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
        log: LogFn | None = None,
        on_status: StatusFn | None = None,
        on_finished: FinishedFn | None = None,
    ) -> None:
        self.start_port = start_port
        self.count = count
        self.switch_interval_sec = switch_interval_sec
        self.xray_bin = xray_bin
        self._log = log or (lambda _msg: None)
        self._on_status = on_status
        self._on_finished = on_finished

        self._thread: threading.Thread | None = None
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

            self._refresh_servers()
            if not self._servers:
                self._log(
                    "[proxy pool] no working Xray-compatible V2Ray servers — run Ping V2Ray first"
                )
                return

            if len(self._servers) < self.count:
                self._log(
                    f"[proxy pool] only {len(self._servers)} server(s) available "
                    f"for {self.count} proxy slot(s); some upstreams will be reused"
                )

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
            self._start_all_slots(initial=True)
            if self._stop_event.is_set():
                return
            self._emit_status()

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
        config = load_config(required=False)
        db, _mt, _v2 = open_catalogs(config)
        try:
            rows = db.v2ray_subscription_list(limit=None)
            servers: list[V2RayServer] = []
            for row in rows:
                server = _server_from_row(row)
                if server.scheme not in XRAY_SCHEMES:
                    continue
                if not is_nekoray_compatible(server):
                    continue
                if link_to_xray_outbound(server) is None:
                    continue
                servers.append(server)
            return servers
        finally:
            db.close()

    def _is_reusable(self, key: str, *, now: float, rotation: int) -> bool:
        record = self._usage.get(key)
        if record is None:
            return True
        rotations_ago = rotation - record.rotation
        age_sec = now - record.used_at
        return rotations_ago >= REUSE_AFTER_ROTATIONS or age_sec >= REUSE_AFTER_SEC

    def _pick_servers(self, count: int) -> list[V2RayServer]:
        """Pick distinct upstreams, preferring ones off cooldown."""
        if not self._servers:
            return []

        now = time.monotonic()
        rotation = self._rotation_round
        # Never re-assign the immediately previous set unless they are already reusable
        # (they usually are not — still on cooldown).
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

        picked: list[V2RayServer] = []
        seen: set[str] = set()

        for server in available:
            if len(picked) >= count:
                break
            if server.key in seen:
                continue
            picked.append(server)
            seen.add(server.key)

        if len(picked) < count:
            # Oldest last-used first (most cooled). Last resort when catalog is thin.
            cooling.sort(key=lambda item: item[0])
            reused = 0
            for _score, server in cooling:
                if len(picked) >= count:
                    break
                if server.key in seen:
                    continue
                picked.append(server)
                seen.add(server.key)
                reused += 1
            if reused:
                self._log(
                    f"[proxy pool] only {len(available)} server(s) off cooldown; "
                    f"reused {reused} early (need {count})"
                )

        # Still short? Cycle available catalog (same server on multiple slots).
        if len(picked) < count and self._servers:
            index = 0
            while len(picked) < count:
                picked.append(self._servers[index % len(self._servers)])
                index += 1
            self._log(
                f"[proxy pool] catalog smaller than slot count — "
                f"duplicating upstreams for {count - len(seen)} slot(s)"
            )

        return picked[:count]

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

        selected = self._pick_servers(len(slots))
        if not selected:
            self._log("[proxy pool] no servers to assign")
            return

        self._record_usage(selected)
        cooling = sum(
            1
            for key, record in self._usage.items()
            if key not in self._current_keys
            and not self._is_reusable(
                key, now=time.monotonic(), rotation=self._rotation_round
            )
        )
        self._log(
            f"[proxy pool] assigned {len(selected)} upstream(s); "
            f"{cooling} server(s) on cooldown "
            f"(reuse after {REUSE_AFTER_ROTATIONS} rotations or {REUSE_AFTER_SEC // 60} min)"
        )

        for index, slot in enumerate(slots):
            if self._stop_event.is_set():
                return
            server = selected[index]
            if initial:
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    f"SOCKS5 127.0.0.1:{slot.socks_port}, "
                    f"HTTP 127.0.0.1:{slot.http_port} → "
                    f"{server.host}:{server.port} ({server.scheme})"
                )
            else:
                self._log(
                    f"[proxy pool] slot {index + 1}/{self.count}: "
                    f"switching to {server.host}:{server.port} ({server.scheme})"
                )
            self._restart_slot(slot, server)

    def _restart_slot(self, slot: _ProxySlot, server: V2RayServer) -> None:
        if self._stop_event.is_set():
            return
        self._stop_slot(slot)
        if self._stop_event.is_set():
            return
        slot.error = None
        slot.server = server

        outbound = link_to_xray_outbound(server)
        if outbound is None:
            slot.error = f"unsupported scheme: {server.scheme}"
            return

        config = build_xray_pool_config(outbound, slot.socks_port, slot.http_port)
        cfg_path = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / (
            f"fetch-mtproto-pool-{slot.socks_port}-{int(time.time() * 1000)}.json"
        )
        try:
            cfg_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            slot.error = f"config write failed: {exc}"
            return

        if self._stop_event.is_set():
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            return

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
            return

        if self._stop_event.is_set():
            kill_process_tree(proc)
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            return

        slot.process = proc
        slot.cfg_path = cfg_path
        if not self._wait_port("127.0.0.1", slot.socks_port, timeout=8.0):
            if not self._stop_event.is_set():
                slot.error = "SOCKS5 port did not open"
            self._stop_slot(slot)
            return
        if not self._wait_port("127.0.0.1", slot.http_port, timeout=8.0):
            if not self._stop_event.is_set():
                slot.error = "HTTP port did not open"
            self._stop_slot(slot)

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

    def _kill_all_processes(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        for slot in slots:
            self._stop_slot(slot)

    def _cleanup_all(self) -> None:
        with self._slots_lock:
            slots = list(self._slots)
            self._slots = []
        for slot in slots:
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

    def _emit_status(self) -> None:
        if self._on_status is None:
            return
        latency_by_key: dict[str, float | None] = {}
        if self._servers:
            config = load_config(required=False)
            db, _mt, _v2 = open_catalogs(config)
            try:
                for row in db.v2ray_subscription_list(limit=None):
                    key = str(row["key"])
                    raw = row["last_latency_ms"]
                    latency_by_key[key] = float(raw) if raw is not None else None
            finally:
                db.close()

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
                )
            )
        self._on_status(statuses)
