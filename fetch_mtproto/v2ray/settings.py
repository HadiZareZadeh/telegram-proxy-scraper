"""Shared config helpers for V2Ray ping tests."""

from __future__ import annotations

from fetch_mtproto.config_loader import config_float
from fetch_mtproto.v2ray.ping import (
    DEFAULT_PING_CONCURRENCY,
    DEFAULT_TEST_BYTES,
    DEFAULT_TEST_TIMEOUT,
    DEFAULT_TEST_URL,
    clamp_ping_concurrency,
    resolve_xray_bin,
)
from fetch_mtproto.v2ray.port_cleanup import DEFAULT_PING_BASE_PORT


def v2ray_test_kwargs(config) -> dict:
    raw = getattr(
        config,
        "V2RAY_PING_CONCURRENCY",
        getattr(config, "PING_CONCURRENCY", DEFAULT_PING_CONCURRENCY),
    )
    try:
        requested = int(raw if raw is not None else DEFAULT_PING_CONCURRENCY)
    except (TypeError, ValueError):
        requested = DEFAULT_PING_CONCURRENCY
    raw_base = getattr(config, "V2RAY_PING_BASE_PORT", DEFAULT_PING_BASE_PORT)
    try:
        base_port = int(raw_base if raw_base is not None else DEFAULT_PING_BASE_PORT)
    except (TypeError, ValueError):
        base_port = DEFAULT_PING_BASE_PORT
    return {
        "concurrency": clamp_ping_concurrency(requested),
        "base_port": max(1024, min(base_port, 65000)),
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
    parse_attachments = getattr(config, "V2RAY_PARSE_NAPSTERNET_ATTACHMENTS", True)
    decrypt_npvt = getattr(config, "V2RAY_DECRYPT_NPVT_ATTACHMENTS", True)
    return {
        "expand_subscriptions": sub["fetch_urls"],
        "subscription_fetch_timeout": sub["timeout"],
        "subscription_max_urls": sub["max_urls"],
        "parse_napsternet_attachments": parse_attachments is not False,
        "decrypt_npvt_attachments": decrypt_npvt is not False,
    }
