"""Shared config helpers for V2Ray ping tests."""

from __future__ import annotations

from fetch_mtproto.v2ray.ping import (
    DEFAULT_TEST_BYTES,
    DEFAULT_TEST_TIMEOUT,
    DEFAULT_TEST_URL,
    resolve_xray_bin,
)


def v2ray_test_kwargs(config) -> dict:
    return {
        "concurrency": int(
            getattr(config, "V2RAY_PING_CONCURRENCY", getattr(config, "PING_CONCURRENCY", 10))
        ),
        "timeout": float(getattr(config, "V2RAY_TEST_TIMEOUT", DEFAULT_TEST_TIMEOUT)),
        "test_url": str(getattr(config, "V2RAY_TEST_URL", DEFAULT_TEST_URL)),
        "test_bytes": int(getattr(config, "V2RAY_TEST_BYTES", DEFAULT_TEST_BYTES)),
        "xray_bin": resolve_xray_bin(getattr(config, "XRAY_BIN", None)),
    }
