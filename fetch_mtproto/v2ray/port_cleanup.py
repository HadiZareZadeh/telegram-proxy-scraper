"""Kill leftover Xray listeners on dedicated Ping / Proxy-pool port ranges."""

from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import time
from typing import Iterable

from fetch_mtproto.process_tree import hide_console_kwargs, kill_pid_tree

# Proxy pool keeps 10801+ (Memu / emulators). Ping / pool-test use separate ranges.
DEFAULT_PING_BASE_PORT = 45001
DEFAULT_POOL_START_PORT = 10801
DEFAULT_POOL_COUNT = 10
DEFAULT_POOL_TEST_BASE_PORT = 44001
PORTS_PER_POOL_SLOT = 2
# Single Stats API for the shared pool Xray process.
POOL_API_PORT_OFFSET = 10000
PORTS_PER_POOL_TEST = 2

_XRAY_NAMES = frozenset({"xray", "xray.exe"})


def ping_ports(base_port: int, concurrency: int) -> list[int]:
    """Local SOCKS ports used by one Ping V2Ray batch (single Xray process)."""
    base = max(1024, int(base_port))
    count = max(1, int(concurrency))
    last = base + count - 1
    if last > 65535:
        raise ValueError(
            f"Ping port range {base}–{last} exceeds 65535 "
            f"(base={base}, concurrency={count})"
        )
    return list(range(base, base + count))


def pool_test_ports(base_port: int = DEFAULT_POOL_TEST_BASE_PORT) -> list[int]:
    """SOCKS + HTTP ports for the proxy-pool validation Xray process."""
    base = max(1024, int(base_port))
    last = base + PORTS_PER_POOL_TEST - 1
    if last > 65535:
        raise ValueError(f"Proxy pool test ports exceed 65535 (base={base})")
    return list(range(base, base + PORTS_PER_POOL_TEST))


def pool_ports(start_port: int, count: int) -> list[int]:
    """SOCKS + HTTP per slot + one shared stats API + pool-test ports."""
    start = max(1024, int(start_port))
    slots = max(1, int(count))
    ports: list[int] = []
    for index in range(slots):
        socks = start + index * PORTS_PER_POOL_SLOT
        http = socks + 1
        if http > 65535:
            raise ValueError(
                f"Proxy pool ports exceed 65535 (start={start}, count={slots})"
            )
        ports.extend((socks, http))
    api = start + POOL_API_PORT_OFFSET
    if api > 65535:
        raise ValueError(
            f"Proxy pool API port exceeds 65535 "
            f"(start={start}, offset={POOL_API_PORT_OFFSET})"
        )
    ports.append(api)
    ports.extend(pool_test_ports())
    return ports


def _listening_pids_windows(ports: set[int]) -> dict[int, set[int]]:
    """Map pid -> ports it is LISTENING on (subset of `ports`)."""
    try:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=15.0,
            **hide_console_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if result.returncode not in (0, None) and not result.stdout:
        return {}

    by_pid: dict[int, set[int]] = {}
    # TCP    127.0.0.1:10801    0.0.0.0:0    LISTENING    1234
    line_re = re.compile(
        r"^\s*TCP\s+(\S+):(\d+)\s+\S+\s+LISTENING\s+(\d+)\s*$",
        re.IGNORECASE,
    )
    for line in result.stdout.splitlines():
        match = line_re.match(line)
        if not match:
            continue
        port = int(match.group(2))
        if port not in ports:
            continue
        pid = int(match.group(3))
        if pid <= 0:
            continue
        by_pid.setdefault(pid, set()).add(port)
    return by_pid


def _listening_pids_unix(ports: set[int]) -> dict[int, set[int]]:
    by_pid: dict[int, set[int]] = {}
    try:
        result = subprocess.run(
            ["ss", "-lptn"],
            capture_output=True,
            text=True,
            timeout=15.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    # ... 127.0.0.1:45001 ... users:(("xray",pid=123,fd=8))
    for line in result.stdout.splitlines():
        port_match = re.search(r":(\d+)\s", line)
        if not port_match:
            continue
        port = int(port_match.group(1))
        if port not in ports:
            continue
        for pid_match in re.finditer(r"pid=(\d+)", line):
            pid = int(pid_match.group(1))
            if pid > 0:
                by_pid.setdefault(pid, set()).add(port)
    return by_pid


def listening_pids_on_ports(ports: Iterable[int]) -> dict[int, set[int]]:
    port_set = {int(p) for p in ports if 0 < int(p) <= 65535}
    if not port_set:
        return {}
    if sys.platform == "win32":
        return _listening_pids_windows(port_set)
    return _listening_pids_unix(port_set)


def _image_name(pid: int) -> str | None:
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                [
                    "tasklist",
                    "/FI",
                    f"PID eq {pid}",
                    "/FO",
                    "CSV",
                    "/NH",
                ],
                capture_output=True,
                text=True,
                timeout=5.0,
                **hide_console_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        line = (result.stdout or "").strip().splitlines()
        if not line:
            return None
        # "xray.exe","1234","Session","1","12 K"
        parts = next(iter(line)).split(",")
        if not parts:
            return None
        return parts[0].strip().strip('"').lower()
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "comm="],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    name = (result.stdout or "").strip().lower()
    return name.split("/")[-1] if name else None


def is_xray_pid(pid: int) -> bool:
    name = _image_name(pid)
    if not name:
        return False
    return name in _XRAY_NAMES or name.startswith("xray")


def kill_xray_on_ports(ports: Iterable[int]) -> list[int]:
    """
    Kill Xray processes listening on any of the given ports.
    Returns the list of PIDs that were targeted.
    """
    by_pid = listening_pids_on_ports(ports)
    killed: list[int] = []
    for pid in sorted(by_pid):
        if not is_xray_pid(pid):
            continue
        kill_pid_tree(pid, timeout=3.0)
        killed.append(pid)
    return killed


def kill_listeners_on_ports(ports: Iterable[int]) -> list[int]:
    """
    Kill any process listening on the given ports (not limited to Xray).
    Skips this process. Returns the list of PIDs that were targeted.
    """
    self_pid = os.getpid()
    by_pid = listening_pids_on_ports(ports)
    killed: list[int] = []
    for pid in sorted(by_pid):
        if pid == self_pid:
            continue
        kill_pid_tree(pid, timeout=3.0)
        killed.append(pid)
    return killed


def cleanup_subscription_port(port: int) -> list[int]:
    """Free the subscription HTTP bind port if a leftover process holds it."""
    port = int(port)
    if port <= 0 or port > 65535:
        return []
    return kill_listeners_on_ports([port])


def _port_is_listening(host: str, port: int, *, timeout: float = 0.15) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def wait_ping_ports_free(
    *,
    base_port: int,
    concurrency: int,
    timeout: float = 5.0,
) -> None:
    """Block until ping SOCKS ports are not held by a leftover Xray listener."""
    ports = ping_ports(base_port, concurrency)
    deadline = time.monotonic() + max(0.1, float(timeout))
    while time.monotonic() < deadline:
        occupied = [port for port in ports if _port_is_listening("127.0.0.1", port)]
        if not occupied:
            return
        kill_xray_on_ports(occupied)
        time.sleep(0.1)
    still = [port for port in ports if _port_is_listening("127.0.0.1", port)]
    if still:
        raise TimeoutError(
            f"Ping ports {ports[0]}–{ports[-1]} still in use "
            f"({len(still)} listener(s) remaining)"
        )


def cleanup_ping_xray(*, base_port: int, concurrency: int) -> list[int]:
    return kill_xray_on_ports(ping_ports(base_port, concurrency))


def cleanup_pool_xray(*, start_port: int, count: int) -> list[int]:
    return kill_xray_on_ports(pool_ports(start_port, count))


def cleanup_owned_xray_from_config(config) -> dict[str, list[int]]:
    """Clear both Ping and Proxy-pool port ranges (safe at app startup)."""
    ping_base = int(
        getattr(config, "V2RAY_PING_BASE_PORT", DEFAULT_PING_BASE_PORT)
        or DEFAULT_PING_BASE_PORT
    )
    concurrency = 64  # full allowed ping batch window

    pool_start = int(
        getattr(config, "PROXY_POOL_START_PORT", DEFAULT_POOL_START_PORT)
        or DEFAULT_POOL_START_PORT
    )
    pool_count = int(
        getattr(config, "PROXY_POOL_COUNT", DEFAULT_POOL_COUNT) or DEFAULT_POOL_COUNT
    )
    # Clear a slightly wider pool window in case count was raised before.
    pool_count = max(pool_count, 50)

    return {
        "ping": cleanup_ping_xray(base_port=ping_base, concurrency=concurrency),
        "pool": cleanup_pool_xray(start_port=pool_start, count=pool_count),
    }
