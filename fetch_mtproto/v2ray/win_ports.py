"""Windows TCP dynamic-port diagnostics for pool/probe bind ranges."""

from __future__ import annotations

import re
import socket
import subprocess
import sys
from dataclasses import dataclass

from fetch_mtproto.process_tree import hide_console_kwargs


@dataclass(frozen=True, slots=True)
class DynamicPortRange:
    start: int
    count: int

    @property
    def end(self) -> int:
        return self.start + self.count - 1


@dataclass(frozen=True, slots=True)
class ExcludedPortRange:
    start: int
    end: int


def _run_netsh(args: list[str]) -> str:
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=8.0,
            **hide_console_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout or ""


def query_dynamic_tcp_ports() -> DynamicPortRange | None:
    if sys.platform != "win32":
        return None
    text = _run_netsh(["netsh", "int", "ipv4", "show", "dynamicport", "tcp"])
    start = count = None
    for line in text.splitlines():
        lower = line.lower()
        if "start port" in lower:
            match = re.search(r"(\d+)", line)
            if match:
                start = int(match.group(1))
        elif "number of ports" in lower:
            match = re.search(r"(\d+)", line)
            if match:
                count = int(match.group(1))
    if start is None or count is None:
        return None
    return DynamicPortRange(start=start, count=count)


def query_excluded_tcp_ranges() -> list[ExcludedPortRange]:
    if sys.platform != "win32":
        return []
    text = _run_netsh(["netsh", "int", "ipv4", "show", "excludedportrange", "tcp"])
    ranges: list[ExcludedPortRange] = []
    for line in text.splitlines():
        match = re.match(r"^\s*(\d+)\s+(\d+)\s*$", line)
        if not match:
            continue
        start, end = int(match.group(1)), int(match.group(2))
        if start and end and end >= start:
            ranges.append(ExcludedPortRange(start=start, end=end))
    return ranges


def ranges_overlap(start: int, end: int, other_start: int, other_end: int) -> bool:
    return start <= other_end and other_start <= end


def unbindable_localhost_ports(
    ports: list[int], *, host: str = "127.0.0.1"
) -> list[int]:
    """Ports that fail a local bind (in use, excluded, or WSAEACCES holes)."""
    blocked: list[int] = []
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, int(port)))
        except OSError:
            blocked.append(int(port))
        finally:
            sock.close()
    return blocked


def colliding_ports(
    ports: list[int],
    *,
    dynamic: DynamicPortRange | None,
    excluded: list[ExcludedPortRange],
) -> list[str]:
    """Human-readable collisions. Excluded ranges cannot bind; dynamic overlap is a warning."""
    reasons: list[str] = []
    if not ports:
        return reasons
    lo, hi = min(ports), max(ports)
    if dynamic is not None and ranges_overlap(lo, hi, dynamic.start, dynamic.end):
        reasons.append(
            f"warning: ports {lo}–{hi} overlap Windows dynamic TCP "
            f"{dynamic.start}–{dynamic.end}"
        )
    for block in excluded:
        hit = [p for p in ports if block.start <= p <= block.end]
        if hit:
            reasons.append(
                f"fatal: {len(hit)} port(s) in excluded range {block.start}–{block.end} "
                f"(e.g. {hit[0]})"
            )
    return reasons


def fatal_bind_issues(issues: list[str]) -> list[str]:
    return [item for item in issues if item.startswith("fatal:")]


def format_port_diagnostics(
    *,
    dynamic: DynamicPortRange | None,
    excluded: list[ExcludedPortRange],
) -> str:
    parts: list[str] = []
    if dynamic is not None:
        parts.append(f"dynamic TCP {dynamic.start}–{dynamic.end}")
    if excluded:
        parts.append(f"{len(excluded)} excluded range(s)")
    return "; ".join(parts) if parts else "no Windows port diagnostics"
