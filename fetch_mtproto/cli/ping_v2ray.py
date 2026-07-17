"""Ping V2Ray servers in the SQLite catalog (CLI)."""

from __future__ import annotations

import asyncio
import sys

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.v2ray.ping import check_and_reorganize_v2ray
from fetch_mtproto.v2ray.settings import v2ray_test_kwargs


def _print_fastest(fastest) -> None:
    if fastest is None:
        print("No working V2Ray servers found yet.")
        return
    server, latency = fastest
    print("Fastest server:")
    print(f"  {server.to_link()}")
    print(f"  latency: {latency * 1000:.0f} ms")
    print(f"  endpoint: {server.scheme}://{server.host}:{server.port}")


def _probe_kwargs(config) -> dict:
    return {
        "respect_backoff": bool(getattr(config, "PROBE_RESPECT_BACKOFF", True)),
    }


async def run(config, best: list) -> None:
    db, _mt, catalog = open_catalogs(config)
    try:
        probe_kw = _probe_kwargs(config)
        queue = catalog.probe_queue(**probe_kw)
        working, failed = catalog.counts()
        total_unique = len(catalog.all_unique())
        if not queue:
            print(f"No V2Ray links found in {db.path}")
            return

        kwargs = v2ray_test_kwargs(config)
        if not kwargs["xray_bin"]:
            print(
                "Xray binary not found. Install Xray-core and set xray.bin in config.yaml "
                "(install xray on PATH, set xray.bin in config.yaml, or run setup to install it in xray/).",
                file=sys.stderr,
            )
            sys.exit(1)

        summary = db.v2ray_health_summary()
        print(
            f"Adaptive probe queue: {len(queue)}/{total_unique} "
            f"(working={working}, failed={failed}; "
            f"lifetime ok={summary['successes']} fail={summary['failures']})\n"
            f"via {kwargs['test_url']} through {kwargs['xray_bin']}\n"
            f"Order: highest priority_score first "
            f"(backoff={'on' if probe_kw['respect_backoff'] else 'off'})\n"
        )

        def on_result(done: int, total: int, result) -> None:
            label = f"{result.server.scheme}://{result.server.host}:{result.server.port}"
            if result.ok and result.latency is not None:
                print(f"[{done}/{total}] OK   {result.latency * 1000:.0f} ms  {label}")
                if best[0] is None or result.latency < best[0][1]:
                    best[0] = (result.server, result.latency)
            else:
                err = f" ({result.error})" if result.error else ""
                print(f"[{done}/{total}] FAIL{err}  {label}")

        stats = await check_and_reorganize_v2ray(
            catalog,
            on_result=on_result,
            **kwargs,
            **probe_kw,
        )
        best[0] = stats.fastest
        summary = db.v2ray_health_summary()
        print()
        print(
            f"This run: {stats.ok} ok / {stats.failed} fail · "
            f"catalog working={summary['working']} failed={summary['failed']} · "
            f"lifetime successes={summary['successes']} failures={summary['failures']}"
        )
        if summary["avg_ok_ms"]:
            print(f"Avg working latency: {summary['avg_ok_ms']:.0f} ms")
        print()
        _print_fastest(stats.fastest)
    finally:
        db.close()


def main() -> None:
    config = load_config()
    best: list = [None]
    try:
        asyncio.run(run(config, best))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        print()
        _print_fastest(best[0])
    if sys.stdin.isatty():
        input("\nPress Enter to exit…")


if __name__ == "__main__":
    main()
