"""Run Xray in TUN mode, rotating through fastest working V2Ray servers."""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from fetch_mtproto.paths import LOGS_DIR
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer
from fetch_mtproto.v2ray.xray import (
    build_xray_tun_config,
    dumps_config,
    link_to_xray_outbound,
)


@dataclass(slots=True)
class TunSettings:
    xray_bin: str
    switch_interval: float
    server_limit: int
    reuse_cooldown: float
    tun_name: str
    tun_mtu: int
    config_path: Path


def tun_settings_from_config(config) -> TunSettings | None:
    """Build TUN runner settings; return None when Xray binary is missing."""
    bin_path = resolve_xray_bin(getattr(config, "XRAY_BIN", None))
    if not bin_path:
        return None

    from fetch_mtproto.config_loader import config_float, resolve_subscription_limit

    switch_interval = config_float(
        getattr(config, "XRAY_TUN_SWITCH_INTERVAL", None), 60.0
    )
    if switch_interval < 1.0:
        switch_interval = 1.0

    reuse_cooldown = config_float(
        getattr(config, "XRAY_TUN_REUSE_COOLDOWN", None), 900.0
    )
    if reuse_cooldown < 0:
        reuse_cooldown = 0.0

    raw_limit = getattr(config, "XRAY_TUN_SERVER_LIMIT", None)
    if raw_limit is None:
        server_limit = resolve_subscription_limit(
            getattr(config, "V2RAY_SUBSCRIPTION_LIMIT", None)
        )
        server_limit = server_limit if server_limit is not None else 20
    else:
        try:
            server_limit = int(raw_limit)
        except (TypeError, ValueError):
            server_limit = 20
        if server_limit < 1:
            server_limit = 1

    tun_name = str(getattr(config, "XRAY_TUN_NAME", None) or "xray0")
    try:
        tun_mtu = int(getattr(config, "XRAY_TUN_MTU", None) or 1500)
    except (TypeError, ValueError):
        tun_mtu = 1500
    if tun_mtu < 576:
        tun_mtu = 576

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return TunSettings(
        xray_bin=bin_path,
        switch_interval=switch_interval,
        server_limit=server_limit,
        reuse_cooldown=reuse_cooldown,
        tun_name=tun_name,
        tun_mtu=tun_mtu,
        config_path=LOGS_DIR / "xray-tun.json",
    )


def fastest_working_servers(
    catalog: V2RayCatalog, *, limit: int
) -> list[V2RayServer]:
    return catalog.fastest_working(limit)


def _server_label(server: V2RayServer) -> str:
    return f"{server.scheme}://{server.host}:{server.port}"


class TunRunner:
    """Start Xray TUN mode and rotate through fastest working servers."""

    def __init__(self, catalog: V2RayCatalog, settings: TunSettings) -> None:
        self.catalog = catalog
        self.settings = settings
        self._proc: subprocess.Popen | None = None
        self._stop = False
        self._current_key: str | None = None
        self._switched_from_at: dict[str, float] = {}
        self._skip_key: str | None = None

    def request_stop(self) -> None:
        self._stop = True

    def _is_eligible(self, key: str) -> bool:
        last = self._switched_from_at.get(key)
        if last is None:
            return True
        return (time.monotonic() - last) >= self.settings.reuse_cooldown

    def _mark_switched_from(self, key: str | None) -> None:
        if key:
            self._switched_from_at[key] = time.monotonic()

    def _seconds_until_next_eligible(self) -> float:
        now = time.monotonic()
        cooldown = self.settings.reuse_cooldown
        waits = [
            cooldown - (now - last)
            for last in self._switched_from_at.values()
            if (now - last) < cooldown
        ]
        return max(0.0, min(waits)) if waits else 0.0

    def _pick_eligible_server(self) -> V2RayServer | None:
        limit = self.settings.server_limit
        while True:
            batch = fastest_working_servers(self.catalog, limit=limit)
            if not batch:
                return None

            eligible = [s for s in batch if self._is_eligible(s.key)]
            if self._current_key:
                eligible = [s for s in eligible if s.key != self._current_key]
            if self._skip_key:
                eligible = [s for s in eligible if s.key != self._skip_key]
            if eligible:
                return eligible[0]

            if len(batch) < limit:
                break
            limit *= 2
            if limit > 2000:
                break

        all_servers = self.catalog.fastest_working(None)
        eligible = [s for s in all_servers if self._is_eligible(s.key)]
        if self._current_key:
            eligible = [s for s in eligible if s.key != self._current_key]
        if self._skip_key:
            eligible = [s for s in eligible if s.key != self._skip_key]
        return eligible[0] if eligible else None

    def _stop_xray(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            kill_process_tree(self._proc)
        self._proc = None

    def _start_xray(self, server: V2RayServer) -> tuple[bool, str]:
        outbound = link_to_xray_outbound(server)
        if outbound is None:
            return False, f"unsupported scheme for Xray TUN: {server.scheme}"

        config = build_xray_tun_config(
            outbound,
            tun_name=self.settings.tun_name,
            mtu=self.settings.tun_mtu,
        )
        self.settings.config_path.write_text(
            dumps_config(config), encoding="utf-8"
        )

        popen_kw: dict = {
            "cwd": str(self.settings.config_path.parent),
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            **hide_console_kwargs(),
        }
        if sys.platform != "win32":
            popen_kw["start_new_session"] = True

        try:
            self._proc = subprocess.Popen(
                [
                    self.settings.xray_bin,
                    "run",
                    "-c",
                    str(self.settings.config_path),
                ],
                **popen_kw,
            )
        except OSError as exc:
            self._proc = None
            return False, str(exc)

        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                detail = self._read_process_output()
                return False, detail or f"xray exited with code {self._proc.returncode}"
            time.sleep(0.15)
            if not self._stop:
                return True, ""
        return True, ""

    def _read_process_output(self) -> str:
        if self._proc is None or self._proc.stdout is None:
            return ""
        try:
            return self._proc.stdout.read(4000).strip()
        except OSError:
            return ""

    def _sleep_for(self, seconds: float) -> bool:
        """Sleep up to seconds unless stop is requested. Returns False if stopped."""
        deadline = time.monotonic() + seconds
        while True:
            if self._stop:
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(0.25, remaining))

    def _sleep_interval(self) -> bool:
        """Sleep until switch interval elapses or stop is requested. Returns False if stopped."""
        deadline = time.monotonic() + self.settings.switch_interval
        while time.monotonic() < deadline:
            if self._stop:
                return False
            if self._proc is not None and self._proc.poll() is not None:
                detail = self._read_process_output()
                print(
                    f"xray exited early (code {self._proc.returncode})"
                    + (f": {detail}" if detail else ""),
                    flush=True,
                )
                self._proc = None
                return True
            time.sleep(0.25)
        return True

    def run(self) -> int:
        cooldown_min = self.settings.reuse_cooldown / 60.0
        print(
            f"TUN mode: rotating fastest servers every "
            f"{self.settings.switch_interval:.0f}s "
            f"(top {self.settings.server_limit}, "
            f"reuse cooldown {cooldown_min:.0f}m, "
            f"config {self.settings.config_path})",
            flush=True,
        )
        if sys.platform == "win32":
            print(
                "Note: TUN mode on Windows may require running the app as Administrator.",
                flush=True,
            )

        consecutive_failures = 0
        while not self._stop:
            if not self.catalog.fastest_working(1):
                print(
                    "No working V2Ray servers — run Ping V2Ray first.",
                    flush=True,
                )
                return 1

            server = self._pick_eligible_server()
            if server is None:
                wait = self._seconds_until_next_eligible()
                if wait <= 0:
                    print("No eligible V2Ray servers available.", flush=True)
                    return 1
                print(
                    f"All recent servers on cooldown; waiting {wait:.0f}s…",
                    flush=True,
                )
                if not self._sleep_for(wait):
                    break
                continue

            if self._current_key and self._current_key != server.key:
                self._mark_switched_from(self._current_key)
            self._stop_xray()
            ok, err = self._start_xray(server)
            if not ok:
                consecutive_failures += 1
                print(
                    f"Skipping {_server_label(server)}: {err}",
                    flush=True,
                )
                self._skip_key = server.key
                if consecutive_failures >= len(self.catalog.fastest_working(None)):
                    print("All candidate servers failed to start TUN.", flush=True)
                    return 1
                continue

            consecutive_failures = 0
            self._skip_key = None
            self._current_key = server.key

            print(
                f"Using {_server_label(server)} (pid {self._proc.pid if self._proc else '?'})",
                flush=True,
            )
            if not self._sleep_interval():
                break

        if self._current_key:
            self._mark_switched_from(self._current_key)
        self._stop_xray()
        print("Stopped TUN mode.", flush=True)
        return 0


def run_tun_loop(catalog: V2RayCatalog, settings: TunSettings) -> int:
    runner = TunRunner(catalog, settings)

    def _handle_signal(_signum, _frame) -> None:
        runner.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    return runner.run()
