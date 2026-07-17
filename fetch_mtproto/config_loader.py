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
    ("storage", "proxies_file", "PROXIES_FILE"),
    ("storage", "failed_proxies_file", "FAILED_PROXIES_FILE"),
    ("storage", "v2ray_dir", "V2RAY_DIR"),
    ("xray", "bin", "XRAY_BIN"),
    ("v2ray", "test_url", "V2RAY_TEST_URL"),
    ("v2ray", "test_bytes", "V2RAY_TEST_BYTES"),
    ("v2ray", "test_timeout", "V2RAY_TEST_TIMEOUT"),
    ("v2ray", "ping_concurrency", "V2RAY_PING_CONCURRENCY"),
    ("scraper", "proxy_check_interval", "PROXY_CHECK_INTERVAL"),
    ("mtproto", "ping_concurrency", "PING_CONCURRENCY"),
    ("mtproto", "ping_timeout", "PING_TIMEOUT"),
    ("mtproto", "max_working", "MTPROTO_MAX_WORKING"),
    ("v2ray", "max_working", "V2RAY_MAX_WORKING"),
    ("probe", "respect_backoff", "PROBE_RESPECT_BACKOFF"),
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


def resolve_max_working(value: object) -> int | None:
    """Return a positive cap, or None when unlimited (0 / missing / invalid)."""
    try:
        n = int(value or 0)
    except (TypeError, ValueError):
        return None
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
