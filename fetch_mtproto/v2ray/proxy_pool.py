"""Persistent local SOCKS5+HTTP slots: hot ring + routing-only rotation."""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import config_bool, config_float, config_int, load_config
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.hot_set import select_hot_set, spread_keys
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.pool_ports import (
    DEFAULT_HTTP_START_PORT,
    DEFAULT_POOL_API_PORT,
    DEFAULT_POOL_COUNT,
    DEFAULT_SOCKS_START_PORT,
    balancer_tag,
    clamp_pool_count,
    fallback_outbound_tag,
    last_pool_ports,
    node_outbound_tag,
    slot_ports,
)
from fetch_mtproto.v2ray.port_cleanup import cleanup_pool_xray
from fetch_mtproto.v2ray.store import V2RayServer, _server_from_row, is_nekoray_compatible
from fetch_mtproto.v2ray.win_ports import (
    colliding_ports,
    fatal_bind_issues,
    format_port_diagnostics,
    query_dynamic_tcp_ports,
    query_excluded_tcp_ranges,
    unbindable_localhost_ports,
)
from fetch_mtproto.v2ray.xray import (
    blackhole_json,
    build_xray_slot_balancer_config,
    format_traffic_bytes,
    link_to_xray_outbound,
)
from fetch_mtproto.v2ray.xray_control import (
    HandlerClient,
    RoutingClient,
    StatsClient,
    XrayControlChannel,
)
from fetch_mtproto.v2ray.xray_version import xray_version_warning

LogFn = Callable[[str], None]
StatusFn = Callable[[list["ProxySlotStatus"]], None]
SnapshotFn = Callable[["PoolSnapshot"], None]
FinishedFn = Callable[[], None]

TRAFFIC_POLL_SEC = 2.0
EMPTY_CATALOG_WAIT_SEC = 30.0
TRUST_FRESHNESS_SEC = 900.0
XRAY_RESTART_BACKOFF_SEC = 2.0
XRAY_RESTART_BACKOFF_MAX_SEC = 30.0
XRAY_STABLE_UPTIME_SEC = 60.0


def xray_restart_delay(attempt: int, *, base: float = XRAY_RESTART_BACKOFF_SEC, max_sec: float = XRAY_RESTART_BACKOFF_MAX_SEC) -> float:
    """Exponential backoff between in-process Xray respawns."""
    return min(float(max_sec), float(base) * (2 ** max(0, int(attempt))))


def _looks_like_control_error(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    if "rpc" in name:
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "unavailable",
            "goaway",
            "not connected",
            "socket closed",
            "statuscode",
            "grpc",
            "control channel",
        )
    )


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
    node_key: str | None = None
    ring: str = ""

    @property
    def upload_text(self) -> str:
        return format_traffic_bytes(self.upload_bytes)

    @property
    def download_text(self) -> str:
        return format_traffic_bytes(self.download_bytes)


@dataclass(slots=True)
class PoolSnapshot:
    total: int
    healthy: int
    degraded: int
    dead: int
    average_latency: float | None
    slots: list[ProxySlotStatus] = field(default_factory=list)


@dataclass
class _ProxySlot:
    socks_port: int
    http_port: int
    server: V2RayServer | None = None
    error: str | None = None
    base_upload: int = 0
    base_download: int = 0
    upload_bytes: int = 0
    download_bytes: int = 0
    outbound_tag: str | None = None


@dataclass(slots=True)
class _Lease:
    slot: int
    used_at: float


class ProxyPoolRunner:
    """One pool Xray; rotate by OverrideBalancerTarget; respawn Xray if it dies."""

    def __init__(
        self,
        *,
        start_port: int,
        count: int,
        switch_interval_sec: float = 0.0,
        xray_bin: str | None = None,
        reuse_after_rotations: int = 1,
        reuse_after_sec: float = 600.0,
        max_latency_ms: float = 3000.0,
        random_pick: bool = True,
        http_start_port: int | None = None,
        api_port: int | None = None,
        standby_outbounds: int = 300,
        reserve_outbounds: int = 100,
        hot_refresh_sec: float = 10.0,
        freshness_sec: float = TRUST_FRESHNESS_SEC,
        diversity_rotate_sec: float = 0.0,
        log: LogFn | None = None,
        on_status: StatusFn | None = None,
        on_snapshot: SnapshotFn | None = None,
        on_finished: FinishedFn | None = None,
    ) -> None:
        self.start_port = int(start_port)
        self.http_start_port = int(http_start_port or DEFAULT_HTTP_START_PORT)
        self.count = clamp_pool_count(count)
        self.switch_interval_sec = float(switch_interval_sec)
        self.xray_bin = xray_bin
        self.min_reuse_sec = max(1.0, float(reuse_after_sec))
        self.max_latency_ms = max(1.0, float(max_latency_ms))
        self.random_pick = bool(random_pick)
        self.api_port = int(api_port or DEFAULT_POOL_API_PORT)
        self.standby_outbounds = max(0, int(standby_outbounds))
        self.reserve_outbounds = max(0, int(reserve_outbounds))
        self.hot_refresh_sec = max(2.0, float(hot_refresh_sec))
        self.freshness_sec = max(60.0, float(freshness_sec))
        # Either knob enables timed rotate-all (legacy switch_interval_sec still honored).
        self.diversity_rotate_sec = max(
            0.0, float(diversity_rotate_sec), float(switch_interval_sec)
        )
        del reuse_after_rotations
        self._log = log or (lambda _msg: None)
        self._on_status = on_status
        self._on_snapshot = on_snapshot
        self._on_finished = on_finished

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = threading.Event()
        self._slots_lock = threading.Lock()
        self._slots: list[_ProxySlot] = []
        self._leases: dict[str, _Lease] = {}
        self._loaded_tags: set[str] = set()
        self._generation = 0
        self._bin_path: str | None = None
        self._pool_process: subprocess.Popen | None = None
        self._pool_cfg_path: Path | None = None
        self._pool_err_path: Path | None = None
        self._pool_err_file: object | None = None
        self._xray_started_at: float | None = None
        self._control: XrayControlChannel | None = None
        self._handler: HandlerClient | None = None
        self._routing: RoutingClient | None = None
        self._stats: StatsClient | None = None
        self._latency_by_key: dict[str, float] = {}
        self._rotate_all_event = threading.Event()
        self._xray_restart_attempt = 0
        self._control_fail_streak = 0
        self._last_xray_stderr = ""

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _pool_process_alive(self) -> bool:
        return self._pool_process is not None and self._pool_process.poll() is None

    @staticmethod
    def ports_for_slot(
        start_port: int, slot_index: int, http_start: int | None = None
    ) -> tuple[int, int]:
        return slot_ports(
            start_port, http_start or DEFAULT_HTTP_START_PORT, slot_index
        )

    @staticmethod
    def api_port_for_start(start_port: int) -> int:
        del start_port
        return DEFAULT_POOL_API_PORT

    @staticmethod
    def last_port(start_port: int, count: int, http_start: int | None = None) -> int:
        _socks, http = last_pool_ports(
            start_port, http_start or DEFAULT_HTTP_START_PORT, count
        )
        return http

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._rotate_all_event.clear()
        self._thread = threading.Thread(target=self._run, name="proxy-pool", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(lambda: None)
        self._kill_process()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3.0)
            if not thread.is_alive():
                self._thread = None

    def request_rotate_all(self) -> None:
        self._rotate_all_event.set()

    def _run(self) -> None:
        try:
            asyncio.run(self._async_run())
        finally:
            self._cleanup_all()
            self._emit_status()
            self._log("[proxy pool] stopped")
            if self._on_finished is not None:
                self._on_finished()

    async def _async_run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._bin_path = resolve_xray_bin(self.xray_bin)
        if not self._bin_path:
            self._log(
                "[proxy pool] xray binary not found — run setup or set xray.bin in config.yaml"
            )
            return

        warning = xray_version_warning(self._bin_path)
        if warning:
            self._log(f"[proxy pool] {warning}")

        with self._slots_lock:
            self._slots = []
            for index in range(self.count):
                socks, http = slot_ports(self.start_port, self.http_start_port, index)
                self._slots.append(_ProxySlot(socks_port=socks, http_port=http))

        if not self._preflight_ports():
            return

        if not await self._launch_xray(reason="start"):
            return

        rotate_sec = self.diversity_rotate_sec
        rotate_note = (
            f", diversity rotate every {rotate_sec:.0f}s"
            if rotate_sec > 0
            else ", diversity rotate off"
        )
        self._log(
            f"[proxy pool] xray ready: {self.count} SOCKS "
            f"{self.start_port}–{self.start_port + self.count - 1}, "
            f"HTTP {self.http_start_port}–{self.http_start_port + self.count - 1}, "
            f"api {self.api_port} (respawn on crash, no restart on rotate{rotate_note})"
        )

        last_hot = 0.0
        last_diversity = time.monotonic()
        self._xray_restart_attempt = 0
        self._control_fail_streak = 0
        while not self._stop_event.is_set():
            try:
                if self._xray_needs_restart():
                    if not await self._recover_xray():
                        return
                    last_hot = time.monotonic()
                    last_diversity = last_hot
                    self._control_fail_streak = 0
                    continue
                now = time.monotonic()
                if (
                    self._xray_started_at is not None
                    and (now - self._xray_started_at) >= XRAY_STABLE_UPTIME_SEC
                ):
                    self._xray_restart_attempt = 0
                if now - last_hot >= self.hot_refresh_sec or last_hot == 0.0:
                    await self._refresh_hot_and_assign(initial=last_hot == 0.0)
                    last_hot = now
                if self._rotate_all_event.is_set():
                    self._rotate_all_event.clear()
                    await self._rotate_all()
                    last_diversity = time.monotonic()
                if (
                    self.diversity_rotate_sec > 0
                    and now - last_diversity >= self.diversity_rotate_sec
                ):
                    await self._rotate_all()
                    last_diversity = time.monotonic()
                if not await self._refresh_traffic():
                    if self._pool_process_alive() and not await self._reconnect_control():
                        self._control_fail_streak += 1
                    else:
                        self._control_fail_streak = 0
                    if self._control_fail_streak >= 3:
                        self._log(
                            "[proxy pool] control channel dead — restarting xray"
                        )
                        self._kill_process()
                        self._control_fail_streak = 0
                        continue
                else:
                    self._control_fail_streak = 0
                self._emit_status()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Keep listeners up; a single gRPC/catalog failure must not kill the pool.
                self._log(f"[proxy pool] loop error (continuing): {exc}")
                if not self._pool_process_alive():
                    await asyncio.sleep(0.5)
                    continue
                if _looks_like_control_error(exc):
                    await self._reconnect_control()
                await asyncio.sleep(2.0)
                continue
            await asyncio.sleep(TRAFFIC_POLL_SEC)

    def _xray_needs_restart(self) -> bool:
        if self._stop_event.is_set():
            return False
        return not self._pool_process_alive()

    def _preflight_ports(self) -> bool:
        ports = []
        with self._slots_lock:
            for slot in self._slots:
                ports.extend((slot.socks_port, slot.http_port))
        ports.append(self.api_port)
        dynamic = query_dynamic_tcp_ports()
        excluded = query_excluded_tcp_ranges()
        diag = format_port_diagnostics(dynamic=dynamic, excluded=excluded)
        if diag:
            self._log(f"[proxy pool] Windows ports: {diag}")
        issues = colliding_ports(ports, dynamic=dynamic, excluded=excluded)
        for issue in issues:
            self._log(f"[proxy pool] port check: {issue}")
        fatal = fatal_bind_issues(issues)
        if fatal:
            self._log(
                "[proxy pool] refusing to bind — change SOCKS/HTTP/API ports in Settings"
            )
            return False
        blocked = unbindable_localhost_ports(ports)
        if blocked:
            sample = ", ".join(str(p) for p in blocked[:8])
            extra = f" (+{len(blocked) - 8} more)" if len(blocked) > 8 else ""
            self._log(
                f"[proxy pool] cannot bind {len(blocked)} port(s) on 127.0.0.1 "
                f"({sample}{extra}) — pick SOCKS/HTTP ranges outside the "
                "Windows dynamic TCP pool"
            )
            return False
        return True

    async def _recover_xray(self) -> bool:
        if self._stop_event.is_set():
            return False
        code = None
        if self._pool_process is not None:
            code = self._pool_process.poll()
        tail = self._xray_error_tail() or self._last_xray_stderr
        detail = f"code {code}" if code is not None else "process gone"
        self._log(f"[proxy pool] xray died ({detail}) — will re-run")
        if tail:
            self._log(f"[proxy pool] xray stderr: {tail}")
        if (
            self._xray_started_at is not None
            and (time.monotonic() - self._xray_started_at) >= XRAY_STABLE_UPTIME_SEC
        ):
            self._xray_restart_attempt = 0
        delay = xray_restart_delay(self._xray_restart_attempt)
        self._xray_restart_attempt += 1
        self._log(
            f"[proxy pool] restarting xray in {delay:.0f}s "
            f"(attempt {self._xray_restart_attempt}, backoff)…"
        )
        await asyncio.sleep(delay)
        if self._stop_event.is_set():
            return False
        if not await self._launch_xray(reason="respawn"):
            self._log("[proxy pool] xray re-run failed — retrying")
            return True
        await self._refresh_hot_and_assign(initial=True)
        self._log("[proxy pool] xray re-run complete; slots reassigned")
        return True

    async def _launch_xray(self, *, reason: str) -> bool:
        await self._teardown_xray()
        killed = cleanup_pool_xray(
            start_port=self.start_port,
            count=self.count,
            http_start=self.http_start_port,
            api_port=self.api_port,
        )
        if killed:
            self._log(
                f"[proxy pool] cleared {len(killed)} leftover xray process(es) "
                f"on SOCKS {self.start_port}+ / HTTP {self.http_start_port}+"
            )
        config = build_xray_slot_balancer_config(
            slot_count=self.count,
            socks_start=self.start_port,
            http_start=self.http_start_port,
            api_port=self.api_port,
            hot_outbounds=[],
            fallback=blackhole_json(fallback_outbound_tag()),
        )
        try:
            self._pool_process, self._pool_cfg_path = self._start_xray(config)
        except OSError as exc:
            self._log(f"[proxy pool] xray {reason} failed: {exc}")
            return False

        if not await self._wait_port("127.0.0.1", self.api_port, timeout=15.0):
            self._log("[proxy pool] API port did not open")
            self._kill_process()
            return False
        for slot in self._slots:
            if self._stop_event.is_set():
                self._kill_process()
                return False
            await self._wait_port("127.0.0.1", slot.socks_port, timeout=8.0)

        if not await self._connect_control():
            self._kill_process()
            return False
        self._xray_started_at = time.monotonic()
        return True

    async def _connect_control(self) -> bool:
        await self._close_control()
        try:
            self._control = XrayControlChannel(port=self.api_port)
            await self._control.connect()
            self._handler = HandlerClient(self._control)
            self._routing = RoutingClient(self._control)
            self._stats = StatsClient(self._control)
            return True
        except Exception as exc:
            self._log(f"[proxy pool] control connect failed: {exc}")
            await self._close_control()
            return False

    async def _reconnect_control(self) -> bool:
        if self._stop_event.is_set() or not self._pool_process_alive():
            return False
        self._log("[proxy pool] reconnecting xray control channel")
        ok = await self._connect_control()
        if ok:
            self._log("[proxy pool] control channel reconnected")
        return ok

    async def _close_control(self) -> None:
        control = self._control
        self._control = None
        self._handler = None
        self._routing = None
        self._stats = None
        if control is None:
            return
        try:
            await asyncio.wait_for(control.close(), timeout=1.5)
        except Exception:
            pass

    async def _teardown_xray(self) -> None:
        await self._close_control()
        self._loaded_tags.clear()
        self._kill_process()

    def _xray_error_tail(self, limit: int = 1500) -> str:
        err_file = self._pool_err_file
        if err_file is not None:
            try:
                err_file.flush()
            except Exception:
                pass
        path = self._pool_err_path
        if path is None or not path.is_file():
            return ""
        try:
            data = path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            return ""
        if not data:
            return ""
        if len(data) > limit:
            data = data[-limit:]
        return " ".join(data.split())

    def _catalog_rows(self):
        config = load_config(required=False)
        db, _mt, _v2 = open_catalogs(config)
        rows = db.v2ray_hot_candidates(
            max_latency_ms=self.max_latency_ms,
            freshness_sec=self.freshness_sec,
        )
        # If the fresh hot set is thinner than the slot count, widen freshness so
        # recently-known working nodes can still fill the ring.
        if len(rows) < self.count:
            wider = db.v2ray_hot_candidates(
                max_latency_ms=self.max_latency_ms,
                freshness_sec=max(self.freshness_sec, 3600.0),
            )
            if len(wider) > len(rows):
                rows = wider
        # Freshly probed-but-not-yet-verified healthy rows: allow catalog latency.
        if not rows:
            rows = db.conn.execute(
                """
                SELECT * FROM v2ray
                WHERE state IN ('healthy', 'degraded', 'unknown')
                  AND last_latency_ms IS NOT NULL
                  AND last_latency_ms <= ?
                ORDER BY last_latency_ms ASC
                LIMIT 2000
                """,
                (self.max_latency_ms,),
            ).fetchall()
        servers: dict[str, V2RayServer] = {}
        for row in rows:
            server = _server_from_row(row)
            if server.scheme not in {"vmess", "vless", "trojan", "ss"}:
                continue
            if not is_nekoray_compatible(server):
                continue
            if link_to_xray_outbound(server) is None:
                continue
            servers[server.key] = server
            lat = row["last_latency_ms"]
            if lat is not None:
                self._latency_by_key[server.key] = float(lat)
        return rows, servers

    def _is_cooling(self, key: str, *, now: float) -> bool:
        lease = self._leases.get(key)
        if lease is None:
            return False
        return (now - lease.used_at) < self.min_reuse_sec

    async def _refresh_hot_and_assign(self, *, initial: bool) -> None:
        rows, servers = self._catalog_rows()
        if not servers:
            self._log(
                f"[proxy pool] no eligible V2Ray servers ≤ {self.max_latency_ms:.0f} ms "
                f"— retrying in {int(EMPTY_CATALOG_WAIT_SEC)}s"
            )
            return

        now = time.monotonic()
        cooling = {key for key in servers if self._is_cooling(key, now=now)}
        assigned = {
            slot.server.key for slot in self._slots if slot.server is not None
        }
        hot = select_hot_set(
            rows,
            active=self.count,
            standby=self.standby_outbounds,
            reserve=self.reserve_outbounds,
            assigned=assigned,
            cooling=cooling,
        )
        await self._sync_loaded_outbounds(hot.all_keys, servers)

        if initial or not any(slot.server for slot in self._slots):
            await self._assign_keys(hot.all_keys, servers, replace_all=True)
            self._log(
                f"[proxy pool] hot ring loaded: active={len(hot.active_keys)} "
                f"standby={len(hot.standby_keys)} reserve={len(hot.reserve_keys)} "
                f"xray_outbounds={len(self._loaded_tags)}"
            )
            return

        # Event-driven: replace only dead/empty slots from standby.
        standby = [key for key in hot.standby_keys if key in servers]
        await self._repair_slots(servers, standby)

    async def _sync_loaded_outbounds(
        self, keys: list[str], servers: dict[str, V2RayServer]
    ) -> None:
        if self._handler is None:
            return
        wanted = {node_outbound_tag(key): key for key in keys if key in servers}
        for tag in list(self._loaded_tags):
            if tag not in wanted and tag != fallback_outbound_tag():
                try:
                    await self._handler.remove_outbound(tag)
                except Exception:
                    pass
                self._loaded_tags.discard(tag)
        for tag, key in wanted.items():
            if tag in self._loaded_tags:
                continue
            outbound = link_to_xray_outbound(servers[key])
            if outbound is None:
                continue
            try:
                await self._handler.add_outbound(outbound, tag=tag)
                self._loaded_tags.add(tag)
            except Exception as exc:
                self._log(
                    f"[proxy pool] AddOutbound {tag} failed: {exc}"
                )

    async def _assign_keys(
        self,
        keys: list[str],
        servers: dict[str, V2RayServer],
        *,
        replace_all: bool,
    ) -> None:
        if self._routing is None:
            return
        ordered = [key for key in keys if key in servers]
        if self.random_pick:
            random.shuffle(ordered)
        now = time.monotonic()
        fresh = [key for key in ordered if not self._is_cooling(key, now=now)]
        cooling = [key for key in ordered if self._is_cooling(key, now=now)]
        preferred = fresh + cooling
        if replace_all:
            keep: list[str | None] = [None] * len(self._slots)
        else:
            keep = [
                slot.server.key
                if slot.server is not None and slot.server.key in servers
                else None
                for slot in self._slots
            ]
        mapped, extra = spread_keys(len(self._slots), preferred, keep=keep)
        assignments: dict[str, str] = {}
        slot_servers: list[V2RayServer | None] = []
        for index, key in enumerate(mapped):
            if key is None or key not in servers:
                slot_servers.append(None)
                continue
            tag = node_outbound_tag(key)
            assignments[balancer_tag(index)] = tag
            slot_servers.append(servers[key])
            self._leases[key] = _Lease(slot=index, used_at=now)

        if assignments:
            errors = await self._routing.override_many(assignments)
            failed = sum(1 for err in errors if err)
            if failed:
                self._log(f"[proxy pool] {failed} balancer override(s) failed")
        if extra:
            unique_n = len({key for key in mapped if key})
            self._log(
                f"[proxy pool] hot set exhausted — reused {unique_n} node(s) "
                f"on {extra} extra slot(s)"
            )

        with self._slots_lock:
            for slot, server in zip(self._slots, slot_servers):
                slot.server = server
                slot.outbound_tag = (
                    node_outbound_tag(server.key) if server else None
                )
                slot.error = None if server else "no working upstream found"

        self._generation += 1
        try:
            config = load_config(required=False)
            db, _mt, _v2 = open_catalogs(config)
            db.replace_pool_assignments(
                [
                    (i, srv.key if srv else None)
                    for i, srv in enumerate(slot_servers)
                ],
                generation=self._generation,
            )
        except Exception:
            pass

    async def _repair_slots(
        self, servers: dict[str, V2RayServer], standby: list[str]
    ) -> None:
        with self._slots_lock:
            slots = list(self._slots)
        need: list[int] = []
        for index, slot in enumerate(slots):
            if slot.server is None or slot.server.key not in servers:
                need.append(index)
        if not need:
            return
        unique_pool = [k for k in standby if k in servers]
        reuse_pool = unique_pool or [k for k in servers]
        assigned = {
            slot.server.key for slot in slots if slot.server is not None
        }
        replacements: dict[str, str] = {}
        now = time.monotonic()
        reused = 0
        rr = 0
        for index in need:
            key = next((k for k in unique_pool if k not in assigned), None)
            if key is None:
                if not reuse_pool:
                    continue
                key = reuse_pool[rr % len(reuse_pool)]
                rr += 1
                reused += 1
            else:
                assigned.add(key)
            replacements[balancer_tag(index)] = node_outbound_tag(key)
            with self._slots_lock:
                self._slots[index].server = servers[key]
                self._slots[index].outbound_tag = node_outbound_tag(key)
                self._slots[index].error = None
            self._leases[key] = _Lease(slot=index, used_at=now)
        if replacements and self._routing is not None:
            await self._routing.override_many(replacements)
            msg = f"[proxy pool] repaired {len(replacements)} dead slot(s)"
            if reused:
                msg += f" ({reused} by reusing nodes; hot set exhausted)"
            self._log(msg)

    async def _rotate_all(self) -> None:
        rows, servers = self._catalog_rows()
        if not servers:
            self._log("[proxy pool] rotate-all skipped — no eligible servers")
            return
        # Timed/manual shuffle should reassign freely; reuse cooldown is for repair only.
        self._leases.clear()
        hot = select_hot_set(
            rows,
            active=self.count,
            standby=self.standby_outbounds,
            reserve=self.reserve_outbounds,
        )
        await self._sync_loaded_outbounds(hot.all_keys, servers)
        await self._assign_keys(hot.all_keys, servers, replace_all=True)
        self._log(f"[proxy pool] rotate-all generation {self._generation}")

    async def _refresh_traffic(self) -> bool:
        if self._stats is None or not self._pool_process_alive():
            return False
        try:
            traffic = await self._stats.outbound_traffic()
        except Exception:
            return False
        with self._slots_lock:
            slots = list(self._slots)
        for slot in slots:
            tag = slot.outbound_tag
            if not tag:
                continue
            up, down = traffic.get(tag, (0, 0))
            slot.upload_bytes = slot.base_upload + up
            slot.download_bytes = slot.base_download + down
        return True

    def _start_xray(self, config: dict, *, prefix: str = "fetch-mtproto-pool"):
        if not self._bin_path:
            raise RuntimeError("xray binary not resolved")
        cfg_path = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / (
            f"{prefix}-{int(time.time() * 1000)}.json"
        )
        cfg_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
        err_path = cfg_path.with_suffix(".err")
        err_file = open(err_path, "w", encoding="utf-8")
        try:
            proc = subprocess.Popen(
                [self._bin_path, "run", "-c", str(cfg_path)],
                stdout=subprocess.DEVNULL,
                stderr=err_file,
                **hide_console_kwargs(),
            )
        except OSError:
            err_file.close()
            try:
                err_path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        self._pool_err_file = err_file
        self._pool_err_path = err_path
        return proc, cfg_path

    def _kill_process(self) -> None:
        tail = self._xray_error_tail()
        if tail:
            self._last_xray_stderr = tail
        if self._pool_process is not None and self._pool_process.poll() is None:
            kill_process_tree(self._pool_process)
        self._pool_process = None
        err_file = self._pool_err_file
        self._pool_err_file = None
        if err_file is not None:
            try:
                err_file.close()
            except Exception:
                pass
        if self._pool_cfg_path is not None:
            try:
                self._pool_cfg_path.unlink(missing_ok=True)
            except OSError:
                pass
            self._pool_cfg_path = None
        err_path = self._pool_err_path
        self._pool_err_path = None
        if err_path is not None:
            try:
                extra = err_path.read_text(encoding="utf-8", errors="replace").strip()
                if extra:
                    self._last_xray_stderr = " ".join(extra.split())[-1500:]
            except OSError:
                pass
            try:
                err_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _cleanup_all(self) -> None:
        control = self._control
        self._control = None
        self._handler = None
        self._routing = None
        self._stats = None
        if control is not None:
            try:
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(
                        asyncio.wait_for(control.close(), timeout=1.5)
                    )
                finally:
                    loop.close()
            except Exception:
                pass
        self._kill_process()
        with self._slots_lock:
            self._slots = []

    async def _wait_port(self, host: str, port: int, *, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._stop_event.is_set():
                return False
            try:
                with socket.create_connection((host, port), timeout=0.2):
                    return True
            except OSError:
                await asyncio.sleep(0.05)
        return False

    def snapshot(self) -> PoolSnapshot:
        statuses = self.snapshot_statuses()
        healthy = sum(1 for s in statuses if s.running)
        dead = sum(1 for s in statuses if s.error)
        lats = [s.latency_ms for s in statuses if s.latency_ms is not None]
        avg = sum(lats) / len(lats) if lats else None
        return PoolSnapshot(
            total=len(statuses),
            healthy=healthy,
            degraded=max(0, len(statuses) - healthy - dead),
            dead=dead,
            average_latency=avg,
            slots=statuses,
        )

    def snapshot_statuses(self) -> list[ProxySlotStatus]:
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
                    node_key=server.key if server else None,
                )
            )
        return statuses

    def _emit_status(self) -> None:
        snapshot = self.snapshot()
        if self._on_status is not None:
            self._on_status(snapshot.slots)
        if self._on_snapshot is not None:
            self._on_snapshot(snapshot)
