"""Ping MTProto proxies in the SQLite catalog (CLI)."""

from __future__ import annotations

import asyncio
import sys

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.mtproto.ping import check_and_reorganize, patch_telethon_faketls


def _print_fastest(fastest) -> None:
    if fastest is None:
        print("No working proxies found yet.")
        return
    proxy, latency = fastest
    print("Fastest proxy:")
    print(f"  {proxy.to_link()}")
    print(f"  latency: {latency * 1000:.0f} ms")
    print(f"  server:  {proxy.server}:{proxy.port}")


def _probe_kwargs(catalog, config) -> dict:
    return {
        "respect_backoff": bool(getattr(config, "PROBE_RESPECT_BACKOFF", True)),
        "limit": catalog.max_working,
    }


async def run(config, best: list) -> None:
    db, catalog, _v2 = open_catalogs(config)
    try:
        probe_kw = _probe_kwargs(catalog, config)
        queue = catalog.probe_queue(**probe_kw)
        total_unique = len(catalog.all_unique())
        if not queue:
            print(f"No proxies found in {db.path}")
            return

        summary = db.mtproto_health_summary()
        print(
            f"Adaptive probe queue: {len(queue)}/{total_unique} "
            f"(working={len(catalog.working)}, failed={len(catalog.failed)}; "
            f"lifetime ok={summary['successes']} fail={summary['failures']})\n"
            f"Order: highest priority_score first "
            f"(backoff={'on' if probe_kw['respect_backoff'] else 'off'})\n"
        )

        def on_result(done: int, total: int, result) -> None:
            if result.ok and result.latency is not None:
                print(
                    f"[{done}/{total}] OK   {result.latency * 1000:.0f} ms  "
                    f"{result.proxy.to_link()}"
                )
                if best[0] is None or result.latency < best[0][1]:
                    best[0] = (result.proxy, result.latency)
            else:
                err = f" ({result.error})" if result.error else ""
                print(f"[{done}/{total}] FAIL{err}  {result.proxy.to_link()}")

        stats = await check_and_reorganize(
            catalog,
            concurrency=getattr(config, "PING_CONCURRENCY", 20),
            timeout=getattr(config, "PING_TIMEOUT", 8.0),
            on_result=on_result,
            **probe_kw,
        )
        best[0] = stats.fastest
        summary = db.mtproto_health_summary()
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
    patch_telethon_faketls()
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
