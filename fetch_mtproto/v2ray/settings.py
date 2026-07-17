"""Shared config helpers for V2Ray ping tests."""

from __future__ import annotations

from fetch_mtproto.config_loader import config_float
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
        "timeout": config_float(
            getattr(config, "V2RAY_TEST_TIMEOUT", None), DEFAULT_TEST_TIMEOUT
        ),
        "test_url": str(getattr(config, "V2RAY_TEST_URL", DEFAULT_TEST_URL)),
        "test_bytes": int(getattr(config, "V2RAY_TEST_BYTES", DEFAULT_TEST_BYTES)),
        "xray_bin": resolve_xray_bin(getattr(config, "XRAY_BIN", None)),
    }


def v2ray_subscription_expand_kwargs(config) -> dict:
    """Settings for expanding HTTP / Base64 subscriptions into share links."""
    raw_max = getattr(config, "V2RAY_SUBSCRIPTION_MAX_URLS", 5)
    try:
        max_urls = int(raw_max if raw_max is not None else 5)
    except (TypeError, ValueError):
        max_urls = 5
    if max_urls < 0:
        max_urls = 0
    enabled = getattr(config, "V2RAY_EXPAND_SUBSCRIPTIONS", True)
    return {
        "fetch_urls": enabled is not False,
        "timeout": config_float(
            getattr(config, "V2RAY_SUBSCRIPTION_FETCH_TIMEOUT", None), 15.0
        ),
        "max_urls": max_urls,
    }


def ingest_subscription_kwargs(config) -> dict:
    """Keyword args for ingest_message subscription expansion."""
    sub = v2ray_subscription_expand_kwargs(config)
    return {
        "expand_subscriptions": sub["fetch_urls"],
        "subscription_fetch_timeout": sub["timeout"],
        "subscription_max_urls": sub["max_urls"],
    }
