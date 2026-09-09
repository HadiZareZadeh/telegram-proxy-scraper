"""Microbench: parallel OverrideBalancerTarget p50/p95/p99 for 1/10/50/100/300."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.pool_ports import balancer_tag, fallback_outbound_tag
from fetch_mtproto.v2ray.xray import blackhole_json, build_xray_slot_balancer_config
from fetch_mtproto.v2ray.xray_control import RoutingClient, XrayControlChannel


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


async def _bench_batch(routing: RoutingClient, n: int, repeats: int) -> dict:
    times: list[float] = []
    for _ in range(repeats):
        assignments = {
            balancer_tag(i): "n-aaaa" if i % 2 == 0 else "n-bbbb" for i in range(n)
        }
        started = time.perf_counter()
        errors = await routing.override_many(assignments)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        failed = sum(1 for err in errors if err)
        if failed:
            print(f"  {n} RPCs: {failed} error(s)", file=sys.stderr)
        times.append(elapsed_ms)
    return {
        "n": n,
        "repeats": repeats,
        "p50_ms": round(_percentile(times, 50), 3),
        "p95_ms": round(_percentile(times, 95), 3),
        "p99_ms": round(_percentile(times, 99), 3),
        "mean_ms": round(statistics.fmean(times), 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-port", type=int, default=26002)
    parser.add_argument("--slots", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    bin_path = resolve_xray_bin()
    if not bin_path:
        print("xray binary not found", file=sys.stderr)
        return 1

    cfg = build_xray_slot_balancer_config(
        slot_count=args.slots,
        socks_start=26001,
        http_start=26401,
        api_port=args.api_port,
        hot_outbounds=[blackhole_json("n-aaaa"), blackhole_json("n-bbbb")],
        fallback=blackhole_json(fallback_outbound_tag()),
    )
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, prefix="bench-rpc-")
    tmp.write(json.dumps(cfg))
    tmp.close()
    proc = subprocess.Popen(
        [bin_path, "run", "-c", tmp.name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        **hide_console_kwargs(),
    )
    try:
        deadline = time.monotonic() + 20
        import socket

        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", args.api_port), timeout=0.2):
                    break
            except OSError:
                if proc.poll() is not None:
                    print("xray exited before API was ready", file=sys.stderr)
                    return 1
                time.sleep(0.05)
        else:
            print("API port did not open", file=sys.stderr)
            return 1

        async def _run() -> list[dict]:
            control = XrayControlChannel(port=args.api_port)
            await control.connect()
            routing = RoutingClient(control)
            out = []
            for n in (1, 10, 50, 100, min(300, args.slots)):
                result = await _bench_batch(routing, n, args.repeats)
                print(
                    f"n={result['n']:>3}  p50={result['p50_ms']:.3f}ms  "
                    f"p95={result['p95_ms']:.3f}ms  p99={result['p99_ms']:.3f}ms  "
                    f"mean={result['mean_ms']:.3f}ms"
                )
                out.append(result)
            await control.close()
            return out

        asyncio.run(_run())
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
