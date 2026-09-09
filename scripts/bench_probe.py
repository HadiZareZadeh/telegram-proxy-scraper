"""Probe pipeline throughput: TCP prefilter + Xray HTTP HEAD (probes/sec)."""

from __future__ import annotations

import argparse
import asyncio
import os
import time

from fetch_mtproto.v2ray.ping import (
    DEFAULT_PING_CONCURRENCY,
    DEFAULT_TCP_CONCURRENCY,
    ping_v2ray_servers,
    resolve_xray_bin,
    tcp_prefilter,
)
from fetch_mtproto.v2ray.store import V2RayServer


def _dummy_servers(count: int) -> list[V2RayServer]:
    servers = []
    for i in range(count):
        host = "127.0.0.1"
        port = 1 + (i % 64000)
        servers.append(
            V2RayServer(
                scheme="vless",
                link=f"vless://id-{i}@{host}:{port}?type=tcp&security=none#{i}",
                host=host,
                port=port,
                identity=f"id-{i}",
                network="tcp",
                security="none",
                sni="",
            )
        )
    return servers


async def _tcp_only(count: int, concurrency: int, timeout: float) -> None:
    servers = _dummy_servers(count)
    sem = asyncio.Semaphore(concurrency)
    started = time.perf_counter()

    async def _one(server: V2RayServer) -> bool:
        async with sem:
            ok, _err = await tcp_prefilter(server.host, server.port, timeout=timeout)
            return ok

    results = await asyncio.gather(*(_one(s) for s in servers))
    elapsed = time.perf_counter() - started
    print(
        f"TCP prefilter: {count} hosts, conc={concurrency}, "
        f"{elapsed:.2f}s, {count / max(elapsed, 1e-6):.1f}/s, "
        f"reachable={sum(1 for ok in results if ok)}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--tcp-concurrency", type=int, default=DEFAULT_TCP_CONCURRENCY)
    parser.add_argument("--tcp-timeout", type=float, default=0.4)
    parser.add_argument("--xray", action="store_true", help="also run Xray probe path")
    parser.add_argument("--xray-concurrency", type=int, default=DEFAULT_PING_CONCURRENCY)
    args = parser.parse_args()

    asyncio.run(_tcp_only(args.count, args.tcp_concurrency, args.tcp_timeout))
    if not args.xray:
        return 0
    bin_path = resolve_xray_bin()
    if not bin_path:
        print("xray binary not found; skip Xray stage")
        return 0
    servers = _dummy_servers(min(args.count, args.xray_concurrency))
    started = time.perf_counter()
    results = asyncio.run(
        ping_v2ray_servers(
            servers,
            concurrency=args.xray_concurrency,
            xray_bin=bin_path,
            tcp_concurrency=args.tcp_concurrency,
            tcp_timeout=args.tcp_timeout,
            timeout=2.5,
        )
    )
    elapsed = time.perf_counter() - started
    ok = sum(1 for r in results if r.ok)
    print(
        f"Xray probes: {len(results)} attempted, {ok} ok, "
        f"{elapsed:.2f}s, {len(results) / max(elapsed, 1e-6):.1f}/s"
    )
    print(f"pid={os.getpid()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
