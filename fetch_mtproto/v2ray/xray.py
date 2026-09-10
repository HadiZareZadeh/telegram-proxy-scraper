"""Convert V2Ray share links to Xray outbound configs."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from fetch_mtproto.v2ray.store import (
    XRAY_SCHEMES,
    V2RayServer,
    _b64decode,
    _safe_json,
    normalize_stream_security,
    vmess_stream_security,
)

_XRAY_CLIENT_NETWORKS = frozenset({"tcp", "ws", "grpc", "httpupgrade"})
_XRAY_SS_METHODS = frozenset(
    {
        "aes-128-gcm",
        "aes-256-gcm",
        "chacha20-poly1305",
        "chacha20-ietf-poly1305",
        "xchacha20-poly1305",
    }
)


def _valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError):
        return False


def _q(qs: dict[str, list[str]], name: str, default: str = "") -> str:
    vals = qs.get(name) or []
    return unquote(vals[0]) if vals else default


def _safe_parsed_host_port(parsed) -> tuple[str | None, int | None]:
    """urlparse(...).port raises ValueError on garbage like unbracketed IPv6."""
    try:
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        return None, None
    if host is None or port is None:
        return host, None
    try:
        port_i = int(port)
    except (TypeError, ValueError):
        return host, None
    if not (1 <= port_i <= 65535):
        return host, None
    return host, port_i


def _stream_settings_from_query(
    qs: dict[str, list[str]], *, default_security: str = ""
) -> dict[str, Any] | None:
    network = (_q(qs, "type") or _q(qs, "network") or "tcp").lower()
    if network not in _XRAY_CLIENT_NETWORKS:
        return None
    security = normalize_stream_security(_q(qs, "security") or default_security or "none")
    if security is None:
        return None

    stream: dict[str, Any] = {"network": network, "security": security}

    sni = _q(qs, "sni") or _q(qs, "peer") or _q(qs, "host")
    fingerprint = _q(qs, "fp") or _q(qs, "fingerprint") or "chrome"
    alpn_raw = _q(qs, "alpn")
    alpn = [p.strip() for p in alpn_raw.split(",") if p.strip()] if alpn_raw else None

    if security == "tls":
        tls: dict[str, Any] = {}
        if sni:
            tls["serverName"] = sni
        if fingerprint:
            tls["fingerprint"] = fingerprint
        if alpn:
            tls["alpn"] = alpn
        stream["tlsSettings"] = tls
    elif security == "reality":
        # Newer Xray uses "password" (pbk in share links); publicKey remains an alias.
        password = _q(qs, "pbk") or _q(qs, "password") or _q(qs, "pwd")
        if not password:
            return None
        stream["realitySettings"] = {
            "serverName": sni or _q(qs, "host"),
            "fingerprint": fingerprint or "chrome",
            "password": password,
            "shortId": _q(qs, "sid"),
            "spiderX": _q(qs, "spx") or "",
        }
    elif security not in {"none", "xtls"}:
        return None

    if network == "ws":
        stream["wsSettings"] = {
            "path": _q(qs, "path") or "/",
            "headers": {"Host": _q(qs, "host") or sni},
        }
    elif network == "grpc":
        stream["grpcSettings"] = {
            "serviceName": _q(qs, "serviceName") or _q(qs, "path"),
        }
    elif network == "httpupgrade":
        stream["httpupgradeSettings"] = {
            "path": _q(qs, "path") or "/",
            "host": _q(qs, "host") or sni,
        }
    elif network in {"splithttp", "xhttp"}:
        key = "xhttpSettings" if network == "xhttp" else "splithttpSettings"
        stream[key] = {
            "path": _q(qs, "path") or "/",
            "host": _q(qs, "host") or sni,
        }
    elif network == "tcp":
        header_type = _q(qs, "headerType") or _q(qs, "header")
        if header_type and header_type != "none":
            stream["tcpSettings"] = {
                "header": {
                    "type": header_type,
                    "request": {
                        "path": [_q(qs, "path") or "/"],
                        "headers": {"Host": [_q(qs, "host") or sni or ""]},
                    },
                }
            }

    return stream


def _outbound_vmess(server: V2RayServer) -> dict[str, Any] | None:
    body = server.link.split("://", 1)[1]
    if "#" in body:
        body = body.split("#", 1)[0]
    try:
        obj = _safe_json(_b64decode(body))
    except Exception:
        return None
    if not obj:
        return None

    host = str(obj.get("add") or "").strip()
    try:
        port = int(obj.get("port"))
    except (TypeError, ValueError):
        return None
    uuid = str(obj.get("id") or "").strip()
    if not host or not _valid_uuid(uuid):
        return None

    try:
        alter_id = int(obj.get("aid") or 0)
    except (TypeError, ValueError):
        alter_id = 0

    network = str(obj.get("net") or "tcp").lower()
    if network not in _XRAY_CLIENT_NETWORKS:
        return None
    security = vmess_stream_security(obj)
    if security is None:
        return None

    stream: dict[str, Any] = {
        "network": network,
        "security": security,
    }
    sni = str(obj.get("sni") or obj.get("host") or "").strip()
    if stream["security"] == "tls":
        stream["tlsSettings"] = {
            "serverName": sni or host,
            "fingerprint": str(obj.get("fp") or "chrome"),
        }
    elif stream["security"] not in {"none", "xtls"}:
        # REALITY/other modes need dedicated settings; skip unbuildable configs.
        return None

    # Cipher field is `scy`; some links misuse `security` for the cipher name.
    cipher = str(obj.get("scy") or "").strip()
    if not cipher:
        maybe_cipher = str(obj.get("security") or "").strip().lower()
        if maybe_cipher and normalize_stream_security(maybe_cipher) is None:
            cipher = maybe_cipher
    if not cipher:
        cipher = "auto"

    if network == "ws":
        stream["wsSettings"] = {
            "path": str(obj.get("path") or "/"),
            "headers": {"Host": str(obj.get("host") or sni or host)},
        }
    elif network == "grpc":
        stream["grpcSettings"] = {"serviceName": str(obj.get("path") or "")}
    elif network == "tcp":
        header_type = str(obj.get("type") or "none")
        if header_type and header_type != "none":
            stream["tcpSettings"] = {"header": {"type": header_type}}

    return {
        "protocol": "vmess",
        "settings": {
            "vnext": [
                {
                    "address": host,
                    "port": port,
                    "users": [
                        {
                            "id": uuid,
                            "alterId": alter_id,
                            "security": cipher,
                        }
                    ],
                }
            ]
        },
        "streamSettings": stream,
    }


def _outbound_vless(server: V2RayServer) -> dict[str, Any] | None:
    parsed = urlparse(server.link)
    host, port = _safe_parsed_host_port(parsed)
    if host is None or port is None:
        host, port = server.host, server.port
    identity = unquote(parsed.username or "")
    if (
        not host
        or not (1 <= int(port) <= 65535)
        or not _valid_uuid(identity)
    ):
        return None
    qs = parse_qs(parsed.query)
    if (_q(qs, "encryption") or "none").lower() != "none":
        return None
    user: dict[str, Any] = {
        "id": identity,
        "encryption": "none",
    }
    flow = _q(qs, "flow")
    if flow:
        user["flow"] = flow

    stream = _stream_settings_from_query(qs)
    if stream is None:
        return None

    return {
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": host,
                    "port": int(port),
                    "users": [user],
                }
            ]
        },
        "streamSettings": stream,
    }


def _outbound_trojan(server: V2RayServer) -> dict[str, Any] | None:
    parsed = urlparse(server.link)
    password = unquote(parsed.username or "")
    if parsed.password:
        password = f"{password}:{unquote(parsed.password)}"
    host, port = _safe_parsed_host_port(parsed)
    if host is None or port is None:
        host, port = server.host, server.port
    if not host or not (1 <= int(port) <= 65535) or not password:
        return None
    qs = parse_qs(parsed.query)
    stream = _stream_settings_from_query(qs, default_security="tls")
    if stream is None:
        return None
    return {
        "protocol": "trojan",
        "settings": {
            "servers": [
                {
                    "address": host,
                    "port": int(port),
                    "password": password,
                }
            ]
        },
        "streamSettings": stream,
    }


def _decode_ss_userinfo(userinfo: str) -> tuple[str, str] | None:
    userinfo = unquote(userinfo)
    if ":" not in userinfo:
        try:
            decoded = _b64decode(userinfo).decode("utf-8")
        except Exception:
            return None
        userinfo = decoded
    if ":" not in userinfo:
        return None
    method, password = userinfo.split(":", 1)
    return method, password


def _outbound_ss(server: V2RayServer) -> dict[str, Any] | None:
    raw = server.link
    parsed = urlparse(raw)
    method = ""
    password = ""
    host = ""
    port = 0

    parsed_host, parsed_port = _safe_parsed_host_port(parsed)
    if parsed_host and parsed_port:
        host = parsed_host
        port = parsed_port
        userinfo = parsed.username or ""
        if parsed.password:
            userinfo = f"{userinfo}:{parsed.password}"
        decoded = _decode_ss_userinfo(userinfo)
        if not decoded:
            return None
        method, password = decoded
    else:
        body = raw.split("://", 1)[1]
        if "#" in body:
            body = body.split("#", 1)[0]
        if "?" in body:
            body = body.split("?", 1)[0]
        try:
            decoded = _b64decode(body).decode("utf-8")
        except Exception:
            return None
        if "@" not in decoded:
            return None
        userinfo, hostport = decoded.rsplit("@", 1)
        parts = _decode_ss_userinfo(userinfo)
        if not parts:
            return None
        method, password = parts
        if hostport.startswith("[") and "]:" in hostport:
            host = hostport[1 : hostport.index("]")]
            try:
                port = int(hostport.split("]:", 1)[1])
            except ValueError:
                return None
        else:
            try:
                host, port_s = hostport.rsplit(":", 1)
                port = int(port_s)
            except ValueError:
                return None

    method = method.strip().lower()
    if (
        not host
        or method not in _XRAY_SS_METHODS
        or not password
        or not (1 <= port <= 65535)
    ):
        return None

    return {
        "protocol": "shadowsocks",
        "settings": {
            "servers": [
                {
                    "address": host,
                    "port": port,
                    "method": method,
                    "password": password,
                }
            ]
        },
    }


def link_to_xray_outbound(server: V2RayServer) -> dict[str, Any] | None:
    if server.scheme not in XRAY_SCHEMES:
        return None
    try:
        if server.scheme == "vmess":
            return _outbound_vmess(server)
        if server.scheme == "vless":
            return _outbound_vless(server)
        if server.scheme == "trojan":
            return _outbound_trojan(server)
        if server.scheme == "ss":
            return _outbound_ss(server)
    except (ValueError, TypeError, IndexError, UnicodeError, KeyError):
        return None
    return None


@dataclass(frozen=True, slots=True)
class XrayRouteEntry:
    """One upstream outbound exposed on optional local SOCKS and/or HTTP ports."""

    outbound: dict[str, Any]
    tag: str
    socks_port: int | None = None
    http_port: int | None = None


def build_xray_routed_config(
    entries: list[XrayRouteEntry],
    *,
    api_port: int | None = None,
) -> dict[str, Any]:
    """Single Xray config: N local inbounds routed to N tagged outbounds.

    Each entry may expose SOCKS5 and/or HTTP. Traffic from those inbounds is
    forced to that entry's outbound via routing rules.
    """
    if not entries:
        raise ValueError("build_xray_routed_config requires at least one entry")

    inbounds: list[dict[str, Any]] = []
    outbounds: list[dict[str, Any]] = []
    rules: list[dict[str, Any]] = []
    seen_tags: set[str] = set()

    for entry in entries:
        tag = entry.tag.strip()
        if not tag:
            raise ValueError("outbound tag must be non-empty")
        if tag in seen_tags:
            raise ValueError(f"duplicate outbound tag: {tag}")
        seen_tags.add(tag)

        outbound = dict(entry.outbound)
        outbound["tag"] = tag
        outbounds.append(outbound)

        inbound_tags: list[str] = []
        if entry.socks_port is not None:
            in_tag = f"socks-{tag}"
            inbounds.append(
                {
                    "tag": in_tag,
                    "listen": "127.0.0.1",
                    "port": int(entry.socks_port),
                    "protocol": "socks",
                    "settings": {"udp": False, "auth": "noauth"},
                }
            )
            inbound_tags.append(in_tag)
        if entry.http_port is not None:
            in_tag = f"http-{tag}"
            inbounds.append(
                {
                    "tag": in_tag,
                    "listen": "127.0.0.1",
                    "port": int(entry.http_port),
                    "protocol": "http",
                    "settings": {},
                }
            )
            inbound_tags.append(in_tag)
        if not inbound_tags:
            raise ValueError(f"entry {tag!r} needs socks_port and/or http_port")
        rules.append(
            {
                "type": "field",
                "inboundTag": inbound_tags,
                "outboundTag": tag,
            }
        )

    outbounds.extend(
        [
            {"protocol": "freedom", "tag": "direct"},
            {"protocol": "blackhole", "tag": "block"},
        ]
    )

    config: dict[str, Any] = {
        "log": {"loglevel": "error"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {"domainStrategy": "AsIs", "rules": rules},
    }

    if api_port is not None:
        inbounds.insert(
            0,
            {
                "tag": "api",
                "listen": "127.0.0.1",
                "port": int(api_port),
                "protocol": "dokodemo-door",
                "settings": {"address": "127.0.0.1"},
            },
        )
        config["stats"] = {}
        config["api"] = {
            "tag": "api",
            "services": ["HandlerService", "RoutingService", "StatsService"],
        }
        config["policy"] = {
            "system": {
                "statsOutboundUplink": True,
                "statsOutboundDownlink": True,
                "statsInboundUplink": True,
                "statsInboundDownlink": True,
            }
        }
        rules.insert(
            0,
            {
                "type": "field",
                "inboundTag": ["api"],
                "outboundTag": "api",
            },
        )
        outbounds.append({"protocol": "freedom", "tag": "api"})
    return config


def build_xray_config(outbound: dict[str, Any], socks_port: int) -> dict[str, Any]:
    """Single SOCKS inbound → one upstream (legacy helper)."""
    return build_xray_routed_config(
        [XrayRouteEntry(outbound=outbound, tag="proxy", socks_port=socks_port)]
    )


def build_xray_pool_config(
    outbound: dict[str, Any],
    socks_port: int,
    http_port: int,
    *,
    api_port: int | None = None,
) -> dict[str, Any]:
    """SOCKS5 + HTTP inbounds sharing one upstream (legacy single-slot helper)."""
    return build_xray_routed_config(
        [
            XrayRouteEntry(
                outbound=outbound,
                tag="proxy",
                socks_port=socks_port,
                http_port=http_port,
            )
        ],
        api_port=api_port,
    )


def build_xray_ping_batch_config(
    outbounds: list[dict[str, Any]],
    *,
    base_port: int,
) -> dict[str, Any]:
    """One SOCKS inbound per outbound for batched Ping V2Ray probes."""
    entries = [
        XrayRouteEntry(
            outbound=outbound,
            tag=f"proxy-{index}",
            socks_port=int(base_port) + index,
        )
        for index, outbound in enumerate(outbounds)
    ]
    return build_xray_routed_config(entries)


def build_xray_multi_pool_config(
    slot_outbounds: list[dict[str, Any] | None],
    *,
    start_port: int,
    api_port: int | None = None,
) -> dict[str, Any]:
    """All proxy-pool slots in one process (SOCKS+HTTP per assigned upstream)."""
    entries: list[XrayRouteEntry] = []
    for index, outbound in enumerate(slot_outbounds):
        if outbound is None:
            continue
        socks_port = int(start_port) + index * 2
        entries.append(
            XrayRouteEntry(
                outbound=outbound,
                tag=f"proxy-{index}",
                socks_port=socks_port,
                http_port=socks_port + 1,
            )
        )
    if not entries:
        raise ValueError("multi pool config needs at least one assigned slot")
    return build_xray_routed_config(entries, api_port=api_port)


def pool_outbound_tag(slot_index: int) -> str:
    return f"proxy-{int(slot_index)}"


def blackhole_json(tag: str) -> dict[str, Any]:
    return {"protocol": "blackhole", "tag": tag, "settings": {}}


def build_xray_slot_balancer_config(
    *,
    slot_count: int,
    socks_start: int,
    http_start: int,
    api_port: int,
    hot_outbounds: list[dict[str, Any]],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """300 SOCKS+HTTP inbounds → per-slot balancers → hot outbounds.

    Balancers use selector prefix ``n-`` and ``fallbackTag``. Assignment is
    applied later via RoutingService.OverrideBalancerTarget (not leastPing).
    """
    from fetch_mtproto.v2ray.pool_ports import (
        balancer_tag,
        fallback_outbound_tag,
        http_inbound_tag,
        slot_ports,
        socks_inbound_tag,
    )

    if slot_count <= 0:
        raise ValueError("slot_count must be positive")

    inbounds: list[dict[str, Any]] = [
        {
            "tag": "api",
            "listen": "127.0.0.1",
            "port": int(api_port),
            "protocol": "dokodemo-door",
            "settings": {"address": "127.0.0.1"},
        }
    ]
    outbounds: list[dict[str, Any]] = []
    rules: list[dict[str, Any]] = [
        {"type": "field", "inboundTag": ["api"], "outboundTag": "api"}
    ]
    balancers: list[dict[str, Any]] = []

    fallback_ob = dict(fallback)
    fallback_ob["tag"] = fallback_outbound_tag()
    outbounds.append(fallback_ob)

    seen_tags: set[str] = {fallback_ob["tag"]}
    for outbound in hot_outbounds:
        tag = str(outbound.get("tag") or "").strip()
        if not tag or tag in seen_tags:
            continue
        seen_tags.add(tag)
        ob = dict(outbound)
        ob["tag"] = tag
        outbounds.append(ob)

    for index in range(int(slot_count)):
        socks_port, http_port = slot_ports(socks_start, http_start, index)
        s_tag = socks_inbound_tag(index)
        h_tag = http_inbound_tag(index)
        b_tag = balancer_tag(index)
        inbounds.append(
            {
                "tag": s_tag,
                "listen": "127.0.0.1",
                "port": int(socks_port),
                "protocol": "socks",
                "settings": {"udp": False, "auth": "noauth"},
            }
        )
        inbounds.append(
            {
                "tag": h_tag,
                "listen": "127.0.0.1",
                "port": int(http_port),
                "protocol": "http",
                "settings": {},
            }
        )
        rules.append(
            {
                "type": "field",
                "inboundTag": [s_tag, h_tag],
                "balancerTag": b_tag,
            }
        )
        balancers.append(
            {
                "tag": b_tag,
                "selector": ["n-"],
                "fallbackTag": fallback_outbound_tag(),
                "strategy": {"type": "random"},
            }
        )

    outbounds.extend(
        [
            {"protocol": "freedom", "tag": "api"},
            {"protocol": "freedom", "tag": "direct"},
            {"protocol": "blackhole", "tag": "block"},
        ]
    )
    return {
        "log": {"loglevel": "error"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {
            "domainStrategy": "AsIs",
            "rules": rules,
            "balancers": balancers,
        },
        "stats": {},
        "api": {
            "tag": "api",
            "services": ["HandlerService", "RoutingService", "StatsService"],
        },
        "burstObservatory": {
            "subjectSelector": [fallback_outbound_tag()],
            "pingConfig": {
                "destination": "http://www.gstatic.com/generate_204",
                "interval": "5m",
                "timeout": "2s",
                "sampling": 1,
            },
        },
        "burstObservatory": {
            "subjectSelector": [fallback_outbound_tag()],
            "pingConfig": {
                "destination": "http://www.gstatic.com/generate_204",
                "interval": "5m",
                "timeout": "2s",
                "sampling": 1,
            },
        },
        "policy": {
            "system": {
                "statsOutboundUplink": True,
                "statsOutboundDownlink": True,
                "statsInboundUplink": True,
                "statsInboundDownlink": True,
            }
        },
    }


def dumps_config(config: dict[str, Any]) -> str:
    return json.dumps(config, ensure_ascii=False, indent=2)


def format_traffic_bytes(n: int) -> str:
    """Human-readable size like NekoRay (B / KB / MB / GB)."""
    value = float(max(0, int(n)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"
