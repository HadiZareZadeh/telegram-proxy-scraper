"""Port layout for the proxy pool, probe Xray, and control APIs."""

from __future__ import annotations

PINNED_XRAY_VERSION = "26.3.27"

DEFAULT_SOCKS_START_PORT = 10801
DEFAULT_HTTP_START_PORT = 11201
DEFAULT_POOL_COUNT = 300
DEFAULT_POOL_API_PORT = 20802
DEFAULT_POOL_STATS_PORT = 20802
DEFAULT_PING_BASE_PORT = 45001
DEFAULT_PING_API_PORT = 45520
DEFAULT_PING_CONCURRENCY = 256
MAX_PING_CONCURRENCY = 1024
MAX_POOL_COUNT = 1000


def clamp_ping_concurrency(value: int) -> int:
    try:
        concurrency = int(value)
    except (TypeError, ValueError):
        concurrency = DEFAULT_PING_CONCURRENCY
    return max(1, min(concurrency, MAX_PING_CONCURRENCY))


def clamp_pool_count(value: int) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = DEFAULT_POOL_COUNT
    return max(1, min(count, MAX_POOL_COUNT))


def slot_socks_port(socks_start: int, index: int) -> int:
    return int(socks_start) + int(index)


def slot_http_port(http_start: int, index: int) -> int:
    return int(http_start) + int(index)


def slot_ports(socks_start: int, http_start: int, index: int) -> tuple[int, int]:
    return slot_socks_port(socks_start, index), slot_http_port(http_start, index)


def last_pool_ports(
    socks_start: int, http_start: int, count: int
) -> tuple[int, int]:
    if count <= 0:
        return int(socks_start), int(http_start)
    return slot_ports(socks_start, http_start, count - 1)


def balancer_tag(slot_index: int) -> str:
    return f"bal-slot-{int(slot_index):03d}"


def socks_inbound_tag(slot_index: int) -> str:
    return f"socks-{int(slot_index):03d}"


def http_inbound_tag(slot_index: int) -> str:
    return f"http-{int(slot_index):03d}"


def fallback_outbound_tag() -> str:
    return "fallback"


def node_outbound_tag(key: str) -> str:
    import hashlib

    digest = hashlib.sha1(key.encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"n-{digest}"


def ping_socks_ports(base_port: int, concurrency: int) -> list[int]:
    base = max(1024, int(base_port))
    count = max(1, int(concurrency))
    return list(range(base, base + count))


def ping_api_port(base_port: int, concurrency: int, *, explicit: int | None = None) -> int:
    if explicit:
        return int(explicit)
    return DEFAULT_PING_API_PORT


def pool_listen_ports(
    *,
    socks_start: int,
    http_start: int,
    count: int,
    api_port: int,
) -> list[int]:
    socks_start = max(1024, int(socks_start))
    http_start = max(1024, int(http_start))
    count = max(1, int(count))
    ports: list[int] = []
    for index in range(count):
        socks, http = slot_ports(socks_start, http_start, index)
        if max(socks, http) > 65535:
            raise ValueError(
                f"Proxy pool ports exceed 65535 (socks={socks_start}, "
                f"http={http_start}, count={count})"
            )
        ports.append(socks)
        ports.append(http)
    api = int(api_port)
    if not (1 <= api <= 65535):
        raise ValueError(f"Invalid pool API port: {api}")
    ports.append(api)
    return ports
