"""Regression gate before bumping the pinned Xray version.

Reduced-N by default (CI-friendly). Pass --full for 300/300/1000/100.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.pool_ports import (
    PINNED_XRAY_VERSION,
    balancer_tag,
    fallback_outbound_tag,
    node_outbound_tag,
)
from fetch_mtproto.v2ray.xray import blackhole_json, build_xray_slot_balancer_config
from fetch_mtproto.v2ray.xray_control import RoutingClient, XrayControlChannel
from fetch_mtproto.v2ray.xray_version import read_xray_version


def _wait_port(port: int, proc: subprocess.Popen, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--api-port", type=int, default=27002)
    args = parser.parse_args()
    n_slots = 300 if args.full else 8
    n_hot = 1000 if args.full else 24
    n_rotations = 100 if args.full else 20

    bin_path = resolve_xray_bin()
    if not bin_path:
        print("xray binary not found", file=sys.stderr)
        return 1
    version = read_xray_version(bin_path)
    print(f"xray {version} (pin {PINNED_XRAY_VERSION}) at {bin_path}")
    if version != PINNED_XRAY_VERSION:
        print(
            f"WARNING: not the pinned release; balancer regressions are why we pin",
            file=sys.stderr,
        )

    hot = [blackhole_json(node_outbound_tag(f"node-{i}")) for i in range(n_hot)]
    cfg = build_xray_slot_balancer_config(
        slot_count=n_slots,
        socks_start=27001,
        http_start=27401,
        api_port=args.api_port,
        hot_outbounds=hot,
        fallback=blackhole_json(fallback_outbound_tag()),
    )
    tmp = tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, prefix="xray-reg-"
    )
    tmp.write(json.dumps(cfg))
    tmp.close()
    proc = subprocess.Popen(
        [bin_path, "run", "-c", tmp.name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        **hide_console_kwargs(),
    )
    try:
        if not _wait_port(args.api_port, proc):
            print("xray API did not open", file=sys.stderr)
            return 1
        pid = proc.pid

        async def _run() -> None:
            control = XrayControlChannel(port=args.api_port)
            await control.connect()
            routing = RoutingClient(control)
            tags = [ob["tag"] for ob in hot]
            for generation in range(n_rotations):
                assignments = {
                    balancer_tag(i): tags[(i + generation) % len(tags)]
                    for i in range(n_slots)
                }
                errors = await routing.override_many(assignments)
                failed = sum(1 for err in errors if err)
                if failed:
                    raise RuntimeError(
                        f"generation {generation}: {failed} override failures"
                    )
            await control.close()

        asyncio.run(_run())
        if proc.poll() is not None:
            print(f"xray died during rotations (code {proc.returncode})", file=sys.stderr)
            return 1
        if proc.pid != pid:
            print("xray pid changed — process was restarted", file=sys.stderr)
            return 1
        print(
            f"ok: {n_slots} slots, {n_hot} outbounds, {n_rotations} rotations, pid={pid}"
        )
        return 0
    finally:
        if proc.poll() is None:
            kill_process_tree(proc)
        try:
            Path(tmp.name).unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
