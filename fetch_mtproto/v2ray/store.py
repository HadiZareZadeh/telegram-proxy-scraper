"""Parse, normalize, and persist V2Ray / Xray share links."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable
from urllib.parse import parse_qs, unquote, urlparse

from fetch_mtproto.db import CatalogDB
from fetch_mtproto.v2ray.subscription import write_subscription

if TYPE_CHECKING:
    from fetch_mtproto.prune import PruneSettings

# Common share-link schemes used with V2Ray / Xray clients
V2RAY_SCHEMES = (
    "vmess",
    "vless",
    "trojan",
    "ss",
    "ssr",
    "hysteria",
    "hysteria2",
    "tuic",
    "wireguard",
)

# Schemes the bundled Xray binary can actually probe (Ping V2Ray / proxy pool).
# Other V2RAY_SCHEMES stay in the catalog for NekoRay subscription export only.
XRAY_SCHEMES = frozenset({"vmess", "vless", "trojan", "ss"})

_SCHEME_ALT = "|".join(V2RAY_SCHEMES)
_V2RAY_URL_RE = re.compile(
    rf"(?P<link>(?:{_SCHEME_ALT})://[^\s<>\"'`]+)",
    re.IGNORECASE,
)

# Transports Nekoray v3.26 cannot use (Xray splithttp/xhttp, raw socket).
NEKORAY_INCOMPATIBLE_NETWORKS = frozenset({"xhttp", "splithttp", "raw"})
# Stream TLS modes Xray accepts (not VMess cipher names like "auto").
XRAY_STREAM_SECURITIES = frozenset({"none", "tls", "reality", "xtls"})
_NETWORK_FROM_LINK_RE = re.compile(
    r"(?:^|[?&])(?:type|network|obfs)=([^&\s#]+)",
    re.IGNORECASE,
)


def normalize_stream_security(raw: str | None) -> str | None:
    """Return a known stream security, or None if the value is invalid/unknown."""
    sec = (raw or "").strip().lower()
    if sec in {"", "0", "none"}:
        return "none"
    if sec in XRAY_STREAM_SECURITIES:
        return sec
    return None


def vmess_stream_security(obj: dict) -> str | None:
    """VMess JSON: prefer `tls`; only treat `security` as stream TLS when it is a known mode."""
    tls = str(obj.get("tls") or "").strip().lower()
    if tls in {"", "0"}:
        tls = ""
    if tls:
        return normalize_stream_security(tls)
    sec = str(obj.get("security") or "").strip().lower()
    if sec in XRAY_STREAM_SECURITIES or sec in {"", "0"}:
        return normalize_stream_security(sec)
    # `security` / missing tls often holds the cipher (e.g. "auto") — not stream TLS.
    return "none"


def _b64decode(data: str) -> bytes:
    raw = data.strip().replace("-", "+").replace("_", "/")
    pad = (-len(raw)) % 4
    return base64.b64decode(raw + ("=" * pad))


def _safe_json(data: bytes) -> dict | None:
    try:
        obj = json.loads(data.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    return obj if isinstance(obj, dict) else None


@dataclass(frozen=True, slots=True)
class V2RayServer:
    scheme: str
    link: str
    host: str
    port: int
    identity: str = ""
    network: str = ""
    security: str = ""
    sni: str = ""

    @property
    def key(self) -> str:
        return "|".join(
            [
                self.scheme,
                self.host.lower(),
                str(self.port),
                self.identity.lower(),
                self.network.lower(),
                self.security.lower(),
                self.sni.lower(),
            ]
        )

    @property
    def uses_tls(self) -> bool:
        sec = self.security.lower()
        return sec in {"tls", "xtls", "reality"} or self.scheme in {
            "trojan",
            "hysteria",
            "hysteria2",
            "tuic",
        }

    def to_link(self) -> str:
        return self.link

    def as_db_row(
        self,
    ) -> tuple[str, str, str, str, int, str, str, str, str]:
        return (
            self.key,
            self.scheme,
            self.link,
            self.host,
            self.port,
            self.identity,
            self.network,
            self.security,
            self.sni,
        )


def _trim_link(raw: str) -> str:
    return raw.strip().rstrip(").,>;']\"`}")


def _host_port_from_netloc(netloc: str) -> tuple[str, int] | None:
    netloc = unquote(netloc)
    if "@" in netloc:
        netloc = netloc.rsplit("@", 1)[1]
    if netloc.startswith("["):
        # [ipv6]:port
        end = netloc.find("]")
        if end < 0:
            return None
        host = netloc[1:end]
        rest = netloc[end + 1 :]
        if not rest.startswith(":"):
            return None
        port_s = rest[1:]
    elif ":" in netloc:
        host, port_s = netloc.rsplit(":", 1)
    else:
        return None
    try:
        port = int(port_s)
    except ValueError:
        return None
    if not host or not (1 <= port <= 65535):
        return None
    return host.strip().lower(), port


def parse_vmess(link: str) -> V2RayServer | None:
    body = link.split("://", 1)[1]
    if "#" in body:
        body = body.split("#", 1)[0]
    try:
        obj = _safe_json(_b64decode(body))
    except Exception:
        return None
    if not obj:
        return None
    host = str(obj.get("add") or obj.get("host") or "").strip()
    port_raw = obj.get("port")
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        return None
    if not host or not (1 <= port <= 65535):
        return None
    identity = str(obj.get("id") or "").strip()
    network = str(obj.get("net") or obj.get("type") or "").strip()
    security = vmess_stream_security(obj)
    if security is None:
        return None
    if security == "none":
        security = ""
    sni = str(obj.get("sni") or obj.get("host") or obj.get("peer") or "").strip()
    return V2RayServer(
        scheme="vmess",
        link=_trim_link(link),
        host=host.lower(),
        port=port,
        identity=identity,
        network=network,
        security=security,
        sni=sni,
    )


def parse_ss(link: str) -> V2RayServer | None:
    """Shadowsocks SIP002 / legacy base64 share URIs."""
    raw = _trim_link(link)
    parsed = urlparse(raw)
    if parsed.scheme.lower() != "ss":
        return None

    # SIP002: ss://base64(method:password)@host:port
    try:
        hostname = parsed.hostname
        port_val = parsed.port
    except ValueError:
        return None
    if hostname and port_val:
        host = hostname.lower()
        port = int(port_val)
        userinfo = unquote(parsed.username or "")
        if parsed.password:
            userinfo = f"{userinfo}:{unquote(parsed.password)}"
        identity = userinfo
        return V2RayServer(
            scheme="ss",
            link=raw,
            host=host,
            port=port,
            identity=identity,
        )

    # Legacy: ss://base64(method:password@host:port)#name
    body = raw.split("://", 1)[1]
    if "#" in body:
        body = body.split("#", 1)[0]
    if "?" in body:
        body = body.split("?", 1)[0]
    try:
        decoded = _b64decode(body).decode("utf-8", errors="strict")
    except Exception:
        return None
    if "@" not in decoded or ":" not in decoded:
        return None
    userinfo, hostport = decoded.rsplit("@", 1)
    hp = _host_port_from_netloc(hostport)
    if hp is None:
        return None
    host, port = hp
    return V2RayServer(
        scheme="ss",
        link=raw,
        host=host,
        port=port,
        identity=userinfo,
    )


def parse_ssr(link: str) -> V2RayServer | None:
    raw = _trim_link(link)
    body = raw.split("://", 1)[1]
    if "#" in body:
        body = body.split("#", 1)[0]
    try:
        decoded = _b64decode(body).decode("utf-8", errors="strict")
    except Exception:
        return None
    # host:port:protocol:method:obfs:password_base64/?params
    parts = decoded.split(":")
    if len(parts) < 6:
        return None
    host = parts[0].lower()
    try:
        port = int(parts[1])
    except ValueError:
        return None
    if not host or not (1 <= port <= 65535):
        return None
    identity = parts[5].split("/", 1)[0]
    return V2RayServer(
        scheme="ssr",
        link=raw,
        host=host,
        port=port,
        identity=identity,
        network=parts[2],
        security=parts[4],
    )


def parse_uri_scheme(link: str, scheme: str) -> V2RayServer | None:
    """vless / trojan / hysteria / hysteria2 / tuic / wireguard style URIs."""
    raw = _trim_link(link)
    parsed = urlparse(raw)
    if parsed.scheme.lower() != scheme:
        return None

    # urlparse(...).port raises ValueError on non-numeric / unbracketed-IPv6 junk
    try:
        hostname = parsed.hostname
        port_val = parsed.port
    except ValueError:
        return None

    if not hostname or port_val is None:
        hp = _host_port_from_netloc(parsed.netloc)
        if hp is None:
            return None
        host, port = hp
    else:
        host = hostname.lower()
        port = int(port_val)
        if not (1 <= port <= 65535):
            return None

    identity = unquote(parsed.username or "")
    if parsed.password:
        identity = f"{identity}:{unquote(parsed.password)}"

    qs = parse_qs(parsed.query)

    def q(name: str) -> str:
        vals = qs.get(name) or []
        return unquote(vals[0]) if vals else ""

    network = q("type") or q("network") or q("obfs")
    security_raw = q("security") or q("tls")
    if not security_raw and (
        scheme in {"trojan", "hysteria", "hysteria2", "tuic"} or q("sni")
    ):
        security_raw = "tls"
    security_norm = normalize_stream_security(security_raw) if security_raw else "none"
    if security_norm is None:
        return None
    security = "" if security_norm == "none" else security_norm
    sni = q("sni") or q("peer") or q("host")

    return V2RayServer(
        scheme=scheme,
        link=raw,
        host=host,
        port=port,
        identity=identity,
        network=network,
        security=security,
        sni=sni,
    )


def parse_v2ray_link(link: str) -> V2RayServer | None:
    try:
        raw = _trim_link(link)
        if "://" not in raw:
            return None
        scheme = raw.split("://", 1)[0].lower()
        if scheme not in V2RAY_SCHEMES:
            return None
        if scheme == "vmess":
            return parse_vmess(raw)
        if scheme == "ss":
            return parse_ss(raw)
        if scheme == "ssr":
            return parse_ssr(raw)
        return parse_uri_scheme(raw, scheme)
    except (ValueError, TypeError, IndexError, UnicodeError):
        return None


def normalized_network(server: V2RayServer) -> str:
    net = (server.network or "").strip().lower()
    if net:
        return net
    match = _NETWORK_FROM_LINK_RE.search(server.link)
    if match:
        return unquote(match.group(1)).strip().lower()
    return ""


def is_nekoray_compatible(server: V2RayServer) -> bool:
    return normalized_network(server) not in NEKORAY_INCOMPATIBLE_NETWORKS


def is_importable_v2ray(server: V2RayServer) -> bool:
    """True when the server uses an Xray-testable scheme and builds a valid outbound."""
    if server.scheme not in XRAY_SCHEMES:
        return False
    if not is_nekoray_compatible(server):
        return False
    # Lazy import avoids a store <-> xray cycle at module load.
    from fetch_mtproto.v2ray.xray import link_to_xray_outbound

    try:
        return link_to_xray_outbound(server) is not None
    except (ValueError, TypeError, IndexError, UnicodeError, KeyError):
        return False


def extract_v2ray_from_text(
    text: str, *, importable_only: bool = True
) -> list[V2RayServer]:
    if not text:
        return []
    found: dict[str, V2RayServer] = {}
    for match in _V2RAY_URL_RE.finditer(text):
        server = parse_v2ray_link(match.group("link"))
        if not server:
            continue
        if importable_only and not is_importable_v2ray(server):
            continue
        found[server.key] = server
    return list(found.values())


def _server_from_row(row) -> V2RayServer:
    return V2RayServer(
        scheme=str(row["scheme"]),
        link=str(row["link"]),
        host=str(row["host"]),
        port=int(row["port"]),
        identity=str(row["identity"] or ""),
        network=str(row["network"] or ""),
        security=str(row["security"] or ""),
        sni=str(row["sni"] or ""),
    )


class _V2RayView:
    """Working or failed slice for one scheme over the shared SQLite catalog."""

    def __init__(self, db: CatalogDB, scheme: str, status: str) -> None:
        self.db = db
        self.scheme = scheme
        self.status = status
        self.path = Path(f"v2ray_{scheme}_{status}")

    def all(self) -> list[V2RayServer]:
        return [
            _server_from_row(row)
            for row in self.db.v2ray_list(self.status, self.scheme)
        ]

    def __contains__(self, key: str) -> bool:
        return self.db.v2ray_has(key, self.status, self.scheme)

    def __len__(self) -> int:
        return self.db.v2ray_count(self.status, self.scheme)


class V2RayCatalog:
    """Working + failed V2Ray servers stored in SQLite (subscription is a derived export)."""

    def __init__(
        self,
        db: CatalogDB,
        *,
        subscription_path: str | Path,
        schemes: tuple[str, ...] = V2RAY_SCHEMES,
        max_working: int | None = None,
        subscription_limit: int | None = 100,
        prune_settings: PruneSettings | None = None,
        catalog_max: int = 10000,
    ) -> None:
        self.db = db
        self.schemes = schemes
        self.subscription_path = Path(subscription_path)
        self.max_working = None
        self.catalog_max = int(catalog_max or 0)
        self.subscription_limit = subscription_limit
        self.prune_settings = prune_settings
        self.working: dict[str, _V2RayView] = {
            scheme: _V2RayView(db, scheme, "working") for scheme in schemes
        }
        self.failed: dict[str, _V2RayView] = {
            scheme: _V2RayView(db, scheme, "failed") for scheme in schemes
        }
        del max_working
        self.update_subscription()

    def enforce_max_working(self) -> int:
        """Trim the working set to max_working; returns count demoted to failed."""
        if self.max_working is None:
            return 0
        return self.db.v2ray_trim_working(self.max_working)

    def all_unique(self) -> list[V2RayServer]:
        merged: dict[str, V2RayServer] = {}
        for row in self.db.v2ray_list("failed"):
            server = _server_from_row(row)
            merged[server.key] = server
        for row in self.db.v2ray_list("working"):
            server = _server_from_row(row)
            merged[server.key] = server
        return list(merged.values())

    def probe_queue(
        self,
        *,
        respect_backoff: bool = True,
        limit: int | None = None,
        failed_limit: int | None = None,
    ) -> list[V2RayServer]:
        """Ordered probe list: most successful / freshest / unexplored first."""
        return [
            server
            for row in self.db.v2ray_probe_queue(
                respect_backoff=respect_backoff,
                limit=limit,
                failed_limit=failed_limit,
            )
            if (server := _server_from_row(row)).scheme in XRAY_SCHEMES
            and is_importable_v2ray(server)
        ]

    def due_servers(self, *, limit: int | None = None) -> list[V2RayServer]:
        servers = [
            server
            for row in self.db.v2ray_due_probe_rows()
            if (server := _server_from_row(row)).scheme in XRAY_SCHEMES
            and is_importable_v2ray(server)
        ]
        return servers[:limit] if limit is not None and limit > 0 else servers

    def prune_stale(self) -> dict[str, int]:
        if self.prune_settings is None or not self.prune_settings.enabled:
            return {
                "chronic": 0,
                "stale": 0,
                "cap": 0,
                "incompatible": 0,
                "total": 0,
            }
        from fetch_mtproto.prune import prune_v2ray

        return prune_v2ray(self.db, self.prune_settings)

    def counts(self) -> tuple[int, int]:
        return self.db.v2ray_count("working"), self.db.v2ray_count("failed")

    def update_subscription(self) -> int:
        """Write fastest, most recently checked working servers to the subscription export."""
        rows = self.db.v2ray_subscription_list(self.subscription_limit)
        servers = [
            server
            for row in rows
            if is_nekoray_compatible(server := _server_from_row(row))
        ]
        return write_subscription(servers, self.subscription_path)

    def add(self, servers: Iterable[V2RayServer]) -> int:
        rows = [
            server.as_db_row()
            for server in servers
            if server.scheme in self.working and is_importable_v2ray(server)
        ]
        added = self.db.v2ray_upsert_working(rows)
        if added:
            if self.catalog_max > 0:
                self.db.v2ray_enforce_catalog_max(self.catalog_max)
            self.update_subscription()
        return added

    def add_new(self, servers: Iterable[V2RayServer]) -> int:
        """Insert unknown servers only; skip max_working trim so Ping V2Ray can explore them."""
        rows = [
            server.as_db_row()
            for server in servers
            if server.scheme in self.working and is_importable_v2ray(server)
        ]
        added = self.db.v2ray_insert_new(rows)
        if added:
            if self.catalog_max > 0:
                self.db.v2ray_enforce_catalog_max(self.catalog_max)
            self.update_subscription()
        return added

    def apply_ping_results(self, results: Iterable) -> tuple[int, int]:
        """Persist ping outcomes with health counters; refresh subscription."""
        grouped: dict[str, list] = {}
        ok_n = 0
        fail_n = 0
        for result in results:
            server = result.server
            if server.scheme not in self.working:
                continue
            # Never record "can't test with Xray" as a real probe failure.
            err = getattr(result, "error", None) or ""
            err_l = err.lower()
            if not result.ok and (
                "unsupported scheme" in err_l
                or "invalid xray outbound" in err_l
                or "outbound swap failed" in err_l
                or "unavailable" in err_l
            ):
                continue
            identity = server.as_db_row()
            probe_type = str(getattr(result, "probe_type", "xray") or "xray")
            if result.ok and result.latency is not None:
                grouped.setdefault(probe_type, []).append(
                    (server.key, True, result.latency, None, identity)
                )
                ok_n += 1
            else:
                grouped.setdefault(probe_type, []).append(
                    (server.key, False, None, err or None, identity)
                )
                fail_n += 1
        for probe_type, outcomes in grouped.items():
            tcp_flag = True if probe_type != "tcp" else None
            self.db.v2ray_record_results(
                outcomes, probe_type=probe_type, tcp_reachable=tcp_flag
            )
        self.prune_stale()
        if self.catalog_max > 0:
            self.db.v2ray_enforce_catalog_max(self.catalog_max)
        self.update_subscription()
        return ok_n, fail_n

    def reorganize(
        self,
        ok: Iterable[V2RayServer],
        failed: Iterable[V2RayServer],
    ) -> None:
        self.db.v2ray_reorganize(
            [server.as_db_row() for server in ok if server.scheme in self.working],
            [server.as_db_row() for server in failed if server.scheme in self.working],
        )
        if self.catalog_max > 0:
            self.db.v2ray_enforce_catalog_max(self.catalog_max)
        self.update_subscription()

def load_v2ray_from_text_file(path: Path) -> list[V2RayServer]:
    if not path.exists():
        return []
    found: dict[str, V2RayServer] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        server = parse_v2ray_link(line)
        if server and is_importable_v2ray(server):
            found[server.key] = server
    return list(found.values())
