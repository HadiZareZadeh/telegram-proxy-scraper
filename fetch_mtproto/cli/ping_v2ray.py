"""Ping V2Ray servers in the SQLite catalog (CLI)."""

from __future__ import annotations

import asyncio
import sys

from fetch_mtproto.cancel import CancelScope
from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.v2ray.ping import (
    DEFAULT_PING_API_PORT,
    DUE_WAVE_LIMIT,
    V2RayReorganizeStats,
    check_and_reorganize_v2ray,
)
from fetch_mtproto.v2ray.port_cleanup import cleanup_ping_xray
from fetch_mtproto.v2ray.settings import v2ray_test_kwargs
from fetch_mtproto.v2ray.store import XRAY_SCHEMES, V2RAY_SCHEMES


def _print_fastest(fastest) -> None:
    if fastest is None:
        print("No working V2Ray servers found yet.")
        return
    server, latency = fastest
    print("Fastest server:")
    print(f"  {server.to_link()}")
    print(f"  latency: {latency * 1000:.0f} ms")
    print(f"  endpoint: {server.scheme}://{server.host}:{server.port}")


def _print_run_summary(stats, summary) -> None:
    print()
    if stats.cancelled:
        print(
            f"Stopped early after {stats.checked}/{stats.total} servers "
            f"— saved {stats.checked} result(s)."
        )
    print(
        f"This run: {stats.ok} ok / {stats.failed} fail · "
        f"inventory={summary['total']} healthy={summary.get('healthy', 0)} "
        f"hot={summary.get('hot', 0)} · "
        f"lifetime successes={summary['successes']} failures={summary['failures']}"
    )
    if summary["avg_ok_ms"]:
        print(f"Avg working latency: {summary['avg_ok_ms']:.0f} ms")
    print()
    _print_fastest(stats.fastest)


async def run(config, best: list) -> None:
    db, _mt, catalog = open_catalogs(config)
    try:
        due = catalog.due_servers()
        working, failed = catalog.counts()
        if not due and not catalog.all_unique():
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
        non_xray = sorted(set(V2RAY_SCHEMES) - set(XRAY_SCHEMES))
        api_port = DEFAULT_PING_API_PORT
        print(
            f"Due-queue probe: {len(due)} server(s) in waves of {DUE_WAVE_LIMIT} "
            f"(inventory={summary['total']}, healthy={summary.get('healthy', 0)}, "
            f"working={working}, failed={failed}; "
            f"lifetime ok={summary['successes']} fail={summary['failures']})\n"
            f"via {kwargs['test_url']} through 1 shared Xray process "
            f"({kwargs['xray_bin']})\n"
            f"concurrency={kwargs['concurrency']} tcp={kwargs['tcp_concurrency']} "
            f"ports={kwargs['base_port']}–"
            f"{kwargs['base_port'] + kwargs['concurrency'] - 1}  "
            f"api={api_port}  "
            f"timeout={kwargs['timeout']}s\n"
            f"Order: probe_due_at (unknown now, healthy 5–15m, degraded 2m, dead backoff)\n"
            f"Xray-testable schemes only: {', '.join(sorted(XRAY_SCHEMES))} "
            f"(skipped in catalog: {', '.join(non_xray)})\n"
        )
        if not due:
            print("No V2Ray servers are due for a probe right now.")
            _print_run_summary(V2RayReorganizeStats(0, 0, None), summary)
            return
        killed = cleanup_ping_xray(
            base_port=kwargs["base_port"],
            concurrency=kwargs["concurrency"],
        )
        if killed:
            print(
                f"Cleared {len(killed)} leftover xray process(es) "
                f"on ping ports {kwargs['base_port']}+.\n"
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

        combined_ok = 0
        combined_fail = 0
        last_stats = V2RayReorganizeStats(0, 0, None)
        wave = 0
        async with CancelScope() as cancel_event:
            while not cancel_event.is_set():
                if not catalog.due_servers(limit=DUE_WAVE_LIMIT):
                    break
                wave += 1
                print(f"--- wave {wave} ---")
                stats = await check_and_reorganize_v2ray(
                    catalog,
                    on_result=on_result,
                    cancel_event=cancel_event,
                    **kwargs,
                )
                last_stats = stats
                combined_ok += stats.ok
                combined_fail += stats.failed
                if stats.fastest is not None and (
                    best[0] is None or stats.fastest[1] < best[0][1]
                ):
                    best[0] = stats.fastest
                summary = db.v2ray_health_summary()
                print(
                    f"Wave {wave}: this wave {stats.ok} ok / {stats.failed} fail · "
                    f"inventory={summary['total']} healthy={summary.get('healthy', 0)} "
                    f"hot={summary.get('hot', 0)}"
                )
                if stats.checked == 0:
                    break
        last_stats = V2RayReorganizeStats(
            combined_ok,
            combined_fail,
            best[0],
            checked=combined_ok + combined_fail,
            total=len(due),
            cancelled=last_stats.cancelled,
        )
        summary = db.v2ray_health_summary()
        _print_run_summary(last_stats, summary)
    finally:
        db.close()


def main() -> None:
    from fetch_mtproto.logging_setup import setup_logging

    setup_logging()
    config = load_config()
    best: list = [None]
    try:
        asyncio.run(run(config, best))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        print()
        _print_fastest(best[0])
    except OSError as exc:
        winerror = getattr(exc, "winerror", None)
        if winerror == 10055 or getattr(exc, "errno", None) in {55, 1055}:
            print(
                "\nWindows ran out of network sockets (WinError 10055).\n"
                "Usually caused by socket exhaustion or a prior ping/pool run.\n"
                "Try:\n"
                "  1. Stop the proxy pool in the GUI\n"
                "  2. Restart the control panel (clears leftover xray on ping/pool ports)\n"
                "  3. Wait ~1–2 minutes for ports to free (TIME_WAIT)\n"
                "  4. Lower v2ray.tcp_concurrency / ping_concurrency in config.yaml\n"
                "  5. Run Ping V2Ray again\n",
                file=sys.stderr,
            )
            sys.exit(1)
        raise
    try:
        if sys.stdin.isatty():
            input("\nPress Enter to exit…")
    except EOFError:
        pass


if __name__ == "__main__":
    main()
