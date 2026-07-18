"""Load user config.yaml from the project root."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import yaml

from fetch_mtproto.paths import PROJECT_ROOT

_CONFIG_PATH = PROJECT_ROOT / "config.yaml"
_EXAMPLE_PATH = PROJECT_ROOT / "config.example.yaml"

_FIELD_MAP: tuple[tuple[str, str, str], ...] = (
    ("telegram", "api_id", "API_ID"),
    ("telegram", "api_hash", "API_HASH"),
    ("telegram", "session_name", "SESSION_NAME"),
    ("telegram", "sources", "SOURCES"),
    ("telegram", "messages_per_source", "MESSAGES_PER_SOURCE"),
    ("storage", "database_file", "DATABASE_FILE"),
    ("storage", "subscription_file", "SUBSCRIPTION_FILE"),
    ("subscription_server", "host", "SUBSCRIPTION_SERVER_HOST"),
    ("subscription_server", "port", "SUBSCRIPTION_SERVER_PORT"),
    ("subscription_server", "lan_ip", "SUBSCRIPTION_SERVER_LAN_IP"),
    ("storage", "proxies_file", "PROXIES_FILE"),
    ("storage", "failed_proxies_file", "FAILED_PROXIES_FILE"),
    ("storage", "v2ray_dir", "V2RAY_DIR"),
    ("xray", "bin", "XRAY_BIN"),
    ("v2ray", "test_url", "V2RAY_TEST_URL"),
    ("v2ray", "test_bytes", "V2RAY_TEST_BYTES"),
    ("v2ray", "test_timeout", "V2RAY_TEST_TIMEOUT"),
    ("v2ray", "ping_concurrency", "V2RAY_PING_CONCURRENCY"),
    ("scraper", "proxy_check_interval", "PROXY_CHECK_INTERVAL"),
    ("scraper", "reconnect_delay", "RECONNECT_DELAY"),
    ("mtproto", "ping_concurrency", "PING_CONCURRENCY"),
    ("mtproto", "ping_timeout", "PING_TIMEOUT"),
    ("mtproto", "max_working", "MTPROTO_MAX_WORKING"),
    ("v2ray", "max_working", "V2RAY_MAX_WORKING"),
    ("v2ray", "subscription_limit", "V2RAY_SUBSCRIPTION_LIMIT"),
    ("v2ray", "expand_subscriptions", "V2RAY_EXPAND_SUBSCRIPTIONS"),
    ("v2ray", "subscription_fetch_timeout", "V2RAY_SUBSCRIPTION_FETCH_TIMEOUT"),
    ("v2ray", "subscription_max_urls_per_message", "V2RAY_SUBSCRIPTION_MAX_URLS"),
    ("v2ray", "parse_napsternet_attachments", "V2RAY_PARSE_NAPSTERNET_ATTACHMENTS"),
    ("v2ray", "decrypt_npvt_attachments", "V2RAY_DECRYPT_NPVT_ATTACHMENTS"),
    ("probe", "respect_backoff", "PROBE_RESPECT_BACKOFF"),
    ("gui", "auto_start_scraper", "GUI_AUTO_START_SCRAPER"),
    ("gui", "auto_start_subscription_server", "GUI_AUTO_START_SUBSCRIPTION_SERVER"),
)


def _parse_config(path: Path) -> SimpleNamespace:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"Invalid {path.name}: expected a YAML mapping at the top level.")

    attrs: dict[str, object] = {}
    for section, key, attr in _FIELD_MAP:
        section_data = data.get(section) or {}
        if not isinstance(section_data, dict):
            raise SystemExit(
                f"Invalid {path.name}: section '{section}' must be a mapping."
            )
        attrs[attr] = section_data.get(key)
    return SimpleNamespace(**attrs)


def config_float(value: object, default: float) -> float:
    """Coerce a config value to float; use default when missing or null."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def resolve_max_working(value: object) -> int | None:
    """Return a positive cap, or None when unlimited (0 / missing / invalid)."""
    try:
        n = int(value or 0)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def resolve_subscription_limit(value: object, *, default: int = 100) -> int | None:
    """Return subscription export cap; default 100; 0 = unlimited."""
    if value is None:
        return default if default > 0 else None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default if default > 0 else None
    return n if n > 0 else None


def load_config(*, required: bool = True) -> SimpleNamespace | None:
    if _CONFIG_PATH.is_file():
        return _parse_config(_CONFIG_PATH)
    if required:
        raise SystemExit(
            "Missing config.yaml — copy config.example.yaml to config.yaml "
            "and fill in your values."
        )
    return None
