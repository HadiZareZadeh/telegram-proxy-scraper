"""Run the local SOCKS5+HTTP proxy pool (CLI)."""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from fetch_mtproto.config_loader import config_bool, config_float, config_int, load_config
from fetch_mtproto.logging_setup import setup_logging
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.pool_ports import (
    DEFAULT_HTTP_START_PORT,
    DEFAULT_POOL_API_PORT,
    clamp_pool_count,
)
from fetch_mtproto.v2ray.proxy_pool import ProxyPoolRunner

log = logging.getLogger("mtproto-scraper")


def main() -> None:
    setup_logging()
    config = load_config()
    xray_bin = resolve_xray_bin(getattr(config, "XRAY_BIN", None))
    if not xray_bin:
        print(
            "Xray binary not found. Set xray.bin in config.yaml or run setup.",
            file=sys.stderr,
        )
        sys.exit(1)

    start_port = config_int(getattr(config, "PROXY_POOL_START_PORT", None), 10801)
    http_start = config_int(
        getattr(config, "PROXY_POOL_HTTP_START_PORT", None), DEFAULT_HTTP_START_PORT
    )
    api_port = config_int(
        getattr(config, "PROXY_POOL_XRAY_API_PORT", None), DEFAULT_POOL_API_PORT
    )
    count = clamp_pool_count(config_int(getattr(config, "PROXY_POOL_COUNT", None), 300))
    diversity = max(
        0.0,
        config_float(getattr(config, "PROXY_POOL_DIVERSITY_ROTATE_SEC", None), 0.0),
        config_float(getattr(config, "PROXY_POOL_SWITCH_INTERVAL_SEC", None), 0.0),
    )
    reuse_sec = max(
        1.0,
        config_float(getattr(config, "PROXY_POOL_MIN_REUSE_SEC", None), 0.0)
        or config_float(getattr(config, "PROXY_POOL_REUSE_AFTER_SEC", None), 600.0),
    )
    max_latency = config_float(getattr(config, "PROXY_POOL_MAX_LATENCY_MS", None), 3000.0)
    random_pick = config_bool(getattr(config, "PROXY_POOL_RANDOM", None), True)
    standby = config_int(getattr(config, "PROXY_POOL_STANDBY_OUTBOUNDS", None), 300)
    reserve = config_int(getattr(config, "PROXY_POOL_RESERVE_OUTBOUNDS", None), 100)
    hot_refresh = max(2.0, config_float(getattr(config, "HOT_SET_REFRESH_SEC", None), 10.0))
    freshness = max(60.0, config_float(getattr(config, "HOT_SET_FRESHNESS_SEC", None), 900.0))

    stop = threading.Event()

    def _log(msg: str) -> None:
        log.info("%s", msg)
        print(msg, flush=True)

    runner = ProxyPoolRunner(
        start_port=start_port,
        count=count,
        switch_interval_sec=diversity,
        xray_bin=xray_bin,
        reuse_after_sec=reuse_sec,
        max_latency_ms=max_latency,
        random_pick=random_pick,
        http_start_port=http_start,
        api_port=api_port,
        standby_outbounds=standby,
        reserve_outbounds=reserve,
        hot_refresh_sec=hot_refresh,
        freshness_sec=freshness,
        diversity_rotate_sec=diversity,
        log=_log,
        on_finished=lambda: stop.set(),
    )

    def _handle_signal(_signum, _frame) -> None:
        print("\nStopping proxy pool…", flush=True)
        runner.stop()
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    runner.start()
    print(
        f"Proxy pool running: SOCKS {start_port}–{start_port + count - 1}, "
        f"HTTP {http_start}–{http_start + count - 1}, api {api_port}",
        flush=True,
    )
    try:
        while not stop.is_set() and runner.running:
            time.sleep(1.0)
    finally:
        if runner.running:
            runner.stop()


if __name__ == "__main__":
    main()
