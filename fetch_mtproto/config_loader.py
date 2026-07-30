"""Load user config.yaml from the project root."""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

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
    ("v2ray", "ping_base_port", "V2RAY_PING_BASE_PORT"),
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
    ("probe", "prune_after_failures", "PROBE_PRUNE_AFTER_FAILURES"),
    ("probe", "prune_min_checks", "PROBE_PRUNE_MIN_CHECKS"),
    ("probe", "prune_stale_days", "PROBE_PRUNE_STALE_DAYS"),
    ("probe", "max_failed", "PROBE_MAX_FAILED"),
    ("probe", "failed_limit", "PROBE_FAILED_LIMIT"),
    ("probe", "prune_incompatible_v2ray", "PROBE_PRUNE_INCOMPATIBLE_V2RAY"),
    ("gui", "auto_start_scraper", "GUI_AUTO_START_SCRAPER"),
    ("gui", "auto_start_subscription_server", "GUI_AUTO_START_SUBSCRIPTION_SERVER"),
    ("gui", "auto_start_proxy_pool", "GUI_AUTO_START_PROXY_POOL"),
    ("gui", "proxy_open_top", "GUI_PROXY_OPEN_TOP"),
    ("gui", "main_pane_ratio", "GUI_MAIN_PANE_RATIO"),
    ("proxy_pool", "start_port", "PROXY_POOL_START_PORT"),
    ("proxy_pool", "count", "PROXY_POOL_COUNT"),
    ("proxy_pool", "switch_interval_sec", "PROXY_POOL_SWITCH_INTERVAL_SEC"),
    ("proxy_pool", "reuse_after_rotations", "PROXY_POOL_REUSE_AFTER_ROTATIONS"),
    ("proxy_pool", "reuse_after_sec", "PROXY_POOL_REUSE_AFTER_SEC"),
    ("proxy_pool", "max_latency_ms", "PROXY_POOL_MAX_LATENCY_MS"),
    ("proxy_pool", "random", "PROXY_POOL_RANDOM"),
)

_SECTION_RE = re.compile(r"^([A-Za-z_][\w]*)\s*:")
_KEY_RE = re.compile(r"^(\s*)([A-Za-z_][\w]*)\s*:")


def config_path() -> Path:
    return _CONFIG_PATH


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


def config_int(
    value: object,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Coerce a config value to int with optional bounds."""
    try:
        n = int(value) if value is not None else default
    except (TypeError, ValueError):
        n = default
    if minimum is not None:
        n = max(minimum, n)
    if maximum is not None:
        n = min(maximum, n)
    return n


def config_bool(value: object, default: bool = False) -> bool:
    """Coerce a config value to bool; use default when missing or invalid."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
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


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in ":#{}[]&*!|>'\"%@`"):
        return json.dumps(text, ensure_ascii=False)
    return text


def update_config_values(updates: Mapping[str, Mapping[str, Any]]) -> Path:
    """
    Update nested keys in config.yaml while preserving comments and layout.

    Example:
        update_config_values({"proxy_pool": {"start_port": 10801, "count": 10}})
    """
    if not updates:
        return _CONFIG_PATH

    if _CONFIG_PATH.is_file():
        original = _CONFIG_PATH.read_text(encoding="utf-8")
    elif _EXAMPLE_PATH.is_file():
        original = _EXAMPLE_PATH.read_text(encoding="utf-8")
    else:
        original = ""

    lines = original.splitlines(keepends=True)
    pending: dict[str, dict[str, Any]] = {
        section: dict(values) for section, values in updates.items() if values
    }
    current_section: str | None = None
    out: list[str] = []

    def _flush_section_keys(section: str) -> None:
        if section not in pending or not pending[section]:
            return
        for key, value in list(pending[section].items()):
            out.append(f"  {key}: {_yaml_scalar(value)}\n")
            del pending[section][key]
        del pending[section]

    for line in lines:
        stripped = line.lstrip()
        if stripped and not stripped.startswith("#") and line[:1] not in " \t":
            section_match = _SECTION_RE.match(line)
            if section_match:
                if current_section:
                    _flush_section_keys(current_section)
                current_section = section_match.group(1)
                out.append(line)
                continue

        key_match = _KEY_RE.match(line)
        if key_match and current_section and current_section in pending:
            indent, key = key_match.group(1), key_match.group(2)
            if key in pending[current_section]:
                value = pending[current_section].pop(key)
                comment = ""
                hash_at = line.find("#")
                if hash_at >= 0:
                    before = line[:hash_at]
                    if before.count('"') % 2 == 0 and before.count("'") % 2 == 0:
                        comment = "  " + line[hash_at:].rstrip("\r\n")
                if line.endswith("\r\n"):
                    newline = "\r\n"
                elif line.endswith("\n"):
                    newline = "\n"
                else:
                    newline = "\n"
                out.append(f"{indent}{key}: {_yaml_scalar(value)}{comment}{newline}")
                if not pending[current_section]:
                    del pending[current_section]
                continue

        out.append(line)

    if current_section:
        _flush_section_keys(current_section)

    for section, values in pending.items():
        if not values:
            continue
        if out and out[-1].strip():
            out.append("\n")
        out.append(f"{section}:\n")
        for key, value in values.items():
            out.append(f"  {key}: {_yaml_scalar(value)}\n")

    _CONFIG_PATH.write_text("".join(out), encoding="utf-8")
    return _CONFIG_PATH


_LIST_ITEM_RE = re.compile(r"^\s+-\s+")


def update_config_lists(updates: Mapping[str, Mapping[str, list[str]]]) -> Path:
    """Replace YAML list keys (e.g. telegram.sources) while preserving comments."""
    if not updates:
        return _CONFIG_PATH

    if _CONFIG_PATH.is_file():
        original = _CONFIG_PATH.read_text(encoding="utf-8")
    else:
        original = ""

    pending: dict[tuple[str, str], list[str]] = {}
    for section, keys in updates.items():
        for key, items in keys.items():
            pending[(section, key)] = list(items)

    lines = original.splitlines(keepends=True)
    current_section: str | None = None
    out: list[str] = []
    skipping_list = False

    for line in lines:
        stripped = line.lstrip()
        if stripped and not stripped.startswith("#") and line[:1] not in " \t":
            section_match = _SECTION_RE.match(line)
            if section_match:
                current_section = section_match.group(1)
                skipping_list = False
                out.append(line)
                continue

        if skipping_list:
            if _LIST_ITEM_RE.match(line):
                continue
            if _KEY_RE.match(line):
                skipping_list = False
            else:
                continue

        key_match = _KEY_RE.match(line)
        if (
            key_match
            and current_section
            and (current_section, key_match.group(2)) in pending
        ):
            indent, key = key_match.group(1), key_match.group(2)
            items = pending.pop((current_section, key))
            comment = ""
            hash_at = line.find("#")
            if hash_at >= 0:
                before = line[:hash_at]
                if before.count('"') % 2 == 0 and before.count("'") % 2 == 0:
                    comment = "  " + line[hash_at:].rstrip("\r\n")
            if line.endswith("\r\n"):
                newline = "\r\n"
            elif line.endswith("\n"):
                newline = "\n"
            else:
                newline = "\n"
            out.append(f"{indent}{key}:{comment}{newline}")
            item_indent = indent + "  "
            for item in items:
                out.append(f"{item_indent}- {_yaml_scalar(item)}\n")
            skipping_list = True
            continue

        out.append(line)

    _CONFIG_PATH.write_text("".join(out), encoding="utf-8")
    return _CONFIG_PATH


def save_gui_config(
    scalars: Mapping[str, Mapping[str, Any]],
    lists: Mapping[str, Mapping[str, list[str]]] | None = None,
) -> Path:
    """Persist scalar and list config updates from the GUI."""
    update_config_values(scalars)
    if lists:
        update_config_lists(lists)
    return _CONFIG_PATH
