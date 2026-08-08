"""Config field metadata for the GUI settings panel."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

FieldKind = Literal[
    "int",
    "float",
    "bool",
    "str",
    "nullable_int",
    "nullable_str",
    "list",
]


@dataclass(frozen=True, slots=True)
class ConfigField:
    section: str
    key: str
    label: str
    kind: FieldKind
    default: Any = None
    minimum: int | float | None = None
    maximum: int | float | None = None
    width: int = 36
    hint: str = ""


CONFIG_TABS: tuple[tuple[str, tuple[ConfigField, ...]], ...] = (
    (
        "Telegram",
        (
            ConfigField("telegram", "api_id", "API ID", "int", 0, minimum=0),
            ConfigField("telegram", "api_hash", "API hash", "str", "", width=48),
            ConfigField("telegram", "session_name", "Session name", "str", "mtproto_scraper"),
            ConfigField(
                "telegram",
                "sources",
                "Sources (one per line)",
                "list",
                hint="Usernames, t.me links, or numeric channel IDs.",
            ),
            ConfigField(
                "telegram",
                "messages_per_source",
                "Messages per source",
                "nullable_int",
                500,
                minimum=1,
                hint="Leave empty to scan full history (can be slow).",
            ),
        ),
    ),
    (
        "Storage",
        (
            ConfigField("storage", "database_file", "Database file", "str", "data/catalog.db"),
            ConfigField(
                "storage",
                "subscription_file",
                "Subscription export file",
                "str",
                "data/subscription.txt",
            ),
        ),
    ),
    (
        "Subscription server",
        (
            ConfigField(
                "subscription_server",
                "host",
                "Bind host",
                "str",
                "0.0.0.0",
                hint="0.0.0.0 = reachable on your LAN.",
            ),
            ConfigField(
                "subscription_server",
                "port",
                "Port",
                "int",
                8765,
                minimum=1,
                maximum=65535,
            ),
            ConfigField(
                "subscription_server",
                "lan_ip",
                "LAN IP override",
                "nullable_str",
                hint="Optional fixed IP for QR / URLs when VPN/TUN is active.",
            ),
        ),
    ),
    (
        "Xray",
        (
            ConfigField(
                "xray",
                "bin",
                "Xray binary path",
                "nullable_str",
                hint="Empty = auto-detect PATH or xray/ folder.",
            ),
        ),
    ),
    (
        "V2Ray",
        (
            ConfigField("v2ray", "test_url", "HTTP test URL", "str", "http://www.gstatic.com/generate_204"),
            ConfigField("v2ray", "test_bytes", "Test body bytes", "int", 0, minimum=0),
            ConfigField("v2ray", "test_timeout", "Test timeout (sec)", "float", 8.0, minimum=0.1),
            ConfigField(
                "v2ray",
                "ping_concurrency",
                "Ping batch size",
                "int",
                20,
                minimum=1,
                maximum=64,
                hint="Servers (and local SOCKS ports) per shared Ping Xray process.",
            ),
            ConfigField(
                "v2ray",
                "ping_base_port",
                "Ping base port",
                "int",
                45001,
                minimum=1024,
                maximum=65000,
                hint="First SOCKS port of the Ping batch window.",
            ),
            ConfigField(
                "v2ray",
                "max_working",
                "Max working servers",
                "int",
                300,
                minimum=0,
                hint="0 = unlimited.",
            ),
            ConfigField(
                "v2ray",
                "subscription_limit",
                "Subscription export limit",
                "int",
                100,
                minimum=0,
                hint="0 = unlimited.",
            ),
            ConfigField("v2ray", "expand_subscriptions", "Expand subscription URLs", "bool", True),
            ConfigField(
                "v2ray",
                "subscription_fetch_timeout",
                "Subscription fetch timeout (sec)",
                "float",
                15.0,
                minimum=0.1,
            ),
            ConfigField(
                "v2ray",
                "subscription_max_urls_per_message",
                "Max sub URLs per message",
                "int",
                5,
                minimum=0,
            ),
            ConfigField(
                "v2ray",
                "parse_napsternet_attachments",
                "Parse Napsternet attachments",
                "bool",
                True,
            ),
            ConfigField(
                "v2ray",
                "decrypt_npvt_attachments",
                "Decrypt NPVT attachments",
                "bool",
                True,
            ),
        ),
    ),
    (
        "Scraper",
        (
            ConfigField(
                "scraper",
                "proxy_check_interval",
                "Re-ping interval (sec)",
                "int",
                300,
                minimum=0,
                hint="0 = disable scheduled re-ping.",
            ),
            ConfigField(
                "scraper",
                "reconnect_delay",
                "Reconnect delay (sec)",
                "int",
                5,
                minimum=0,
            ),
        ),
    ),
    (
        "MTProto",
        (
            ConfigField(
                "mtproto",
                "ping_concurrency",
                "Ping concurrency",
                "int",
                100,
                minimum=1,
                maximum=500,
            ),
            ConfigField("mtproto", "ping_timeout", "Ping timeout (sec)", "float", 8.0, minimum=0.1),
            ConfigField(
                "mtproto",
                "max_working",
                "Max working proxies",
                "int",
                1000,
                minimum=0,
                hint="0 = unlimited.",
            ),
        ),
    ),
    (
        "Probe",
        (
            ConfigField("probe", "respect_backoff", "Respect failure backoff", "bool", True),
            ConfigField(
                "probe",
                "prune_after_failures",
                "Prune after consecutive failures",
                "int",
                8,
                minimum=0,
            ),
            ConfigField("probe", "prune_min_checks", "Prune min checks", "int", 5, minimum=0),
            ConfigField("probe", "prune_stale_days", "Prune stale after (days)", "int", 14, minimum=0),
            ConfigField(
                "probe",
                "max_failed",
                "Max failed rows in DB",
                "int",
                2000,
                minimum=0,
                hint="0 = unlimited.",
            ),
            ConfigField(
                "probe",
                "failed_limit",
                "Max failed re-probes per run",
                "int",
                500,
                minimum=0,
                hint="0 = unlimited; working servers are always probed.",
            ),
            ConfigField(
                "probe",
                "prune_incompatible_v2ray",
                "Prune Nekoray-incompatible V2Ray",
                "bool",
                True,
            ),
        ),
    ),
    (
        "Proxy pool",
        (
            ConfigField(
                "proxy_pool",
                "start_port",
                "Start port",
                "int",
                10801,
                minimum=1024,
                maximum=65000,
            ),
            ConfigField("proxy_pool", "count", "Proxy count", "int", 10, minimum=1, maximum=50),
            ConfigField(
                "proxy_pool",
                "switch_interval_sec",
                "Switch interval (sec)",
                "int",
                30,
                minimum=30,
                maximum=86400,
            ),
            ConfigField(
                "proxy_pool",
                "reuse_after_rotations",
                "Reuse after rotations",
                "int",
                5,
                minimum=1,
                maximum=100,
            ),
            ConfigField(
                "proxy_pool",
                "reuse_after_sec",
                "Reuse after (sec)",
                "int",
                1200,
                minimum=60,
                maximum=86400,
            ),
            ConfigField(
                "proxy_pool",
                "max_latency_ms",
                "Max latency (ms)",
                "int",
                2000,
                minimum=100,
                maximum=60000,
            ),
            ConfigField(
                "proxy_pool",
                "random",
                "Pick upstream randomly (off = fastest first)",
                "bool",
                True,
            ),
        ),
    ),
    (
        "GUI",
        (
            ConfigField("gui", "auto_start_scraper", "Auto-start scraper", "bool", False),
            ConfigField(
                "gui",
                "auto_start_subscription_server",
                "Auto-start subscription server",
                "bool",
                False,
            ),
            ConfigField("gui", "auto_start_proxy_pool", "Auto-start proxy pool", "bool", False),
            ConfigField(
                "gui",
                "proxy_open_top",
                "Proxies tab: open top N",
                "int",
                10,
                minimum=1,
                maximum=50,
            ),
            ConfigField(
                "gui",
                "restart_backoff_sec",
                "Restart backoff base (sec)",
                "float",
                2.0,
                minimum=0.5,
                hint="Long-running jobs restart on failure with exponential backoff from this base.",
            ),
            ConfigField(
                "gui",
                "restart_backoff_max_sec",
                "Restart backoff max (sec)",
                "float",
                30.0,
                minimum=1,
                hint="Cap for restart delay (retries continue indefinitely).",
            ),
        ),
    ),
)


def all_fields() -> tuple[ConfigField, ...]:
    out: list[ConfigField] = []
    for _title, fields in CONFIG_TABS:
        out.extend(fields)
    return tuple(out)
