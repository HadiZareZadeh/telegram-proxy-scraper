"""Convert V2Ray share links to Xray outbound configs."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from fetch_mtproto.v2ray.store import V2RayServer, _b64decode, _safe_json


XRAY_SCHEMES = frozenset({"vmess", "vless", "trojan", "ss"})


def _q(qs: dict[str, list[str]], name: str, default: str = "") -> str:
    vals = qs.get(name) or []
    return unquote(vals[0]) if vals else default


def _stream_settings_from_query(
    qs: dict[str, list[str]], *, default_security: str = ""
) -> dict[str, Any]:
    network = (_q(qs, "type") or _q(qs, "network") or "tcp").lower()
    security = (_q(qs, "security") or default_security or "none").lower()
    if security in {"", "none", "0"}:
        security = "none"

    stream: dict[str, Any] = {"network": network, "security": security}

    sni = _q(qs, "sni") or _q(qs, "peer") or _q(qs, "host")
    fingerprint = _q(qs, "fp") or _q(qs, "fingerprint") or "chrome"
    alpn_raw = _q(qs, "alpn")
    alpn = [p.strip() for p in alpn_raw.split(",") if p.strip()] if alpn_raw else None

    if security == "tls":
        tls: dict[str, Any] = {"allowInsecure": True}
        if sni:
            tls["serverName"] = sni
        if fingerprint:
            tls["fingerprint"] = fingerprint
        if alpn:
            tls["alpn"] = alpn
        stream["tlsSettings"] = tls
    elif security == "reality":
        stream["realitySettings"] = {
            "serverName": sni or _q(qs, "host"),
            "fingerprint": fingerprint or "chrome",
            "publicKey": _q(qs, "pbk"),
            "shortId": _q(qs, "sid"),
            "spiderX": _q(qs, "spx") or "",
        }

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
    if not host or not uuid:
        return None

    try:
        alter_id = int(obj.get("aid") or 0)
    except (TypeError, ValueError):
        alter_id = 0

    network = str(obj.get("net") or "tcp").lower()
    security = str(obj.get("tls") or obj.get("security") or "none").lower()
    if security in {"", "0"}:
        security = "none"

    stream: dict[str, Any] = {
        "network": network,
        "security": security if security else "none",
    }
    sni = str(obj.get("sni") or obj.get("host") or "").strip()
    if stream["security"] == "tls":
        stream["tlsSettings"] = {
            "serverName": sni or host,
            "allowInsecure": True,
            "fingerprint": str(obj.get("fp") or "chrome"),
        }

    if network == "ws":
        stream["wsSettings"] = {
            "path": str(obj.get("path") or "/"),
            "headers": {"Host": str(obj.get("host") or sni or host)},
        }
    elif network == "grpc":
        stream["grpcSettings"] = {"serviceName": str(obj.get("path") or "")}
    elif network in {"h2", "http"}:
        stream["network"] = "http"
        stream["httpSettings"] = {
            "path": str(obj.get("path") or "/"),
            "host": [str(obj.get("host") or sni or host)],
        }
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
                            "security": str(obj.get("scy") or "auto"),
                        }
                    ],
                }
            ]
        },
        "streamSettings": stream,
    }


def _outbound_vless(server: V2RayServer) -> dict[str, Any] | None:
    parsed = urlparse(server.link)
    if not parsed.hostname or parsed.port is None or not parsed.username:
        return None
    qs = parse_qs(parsed.query)
    user: dict[str, Any] = {
        "id": unquote(parsed.username),
        "encryption": _q(qs, "encryption") or "none",
    }
    flow = _q(qs, "flow")
    if flow:
        user["flow"] = flow

    return {
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": parsed.hostname,
                    "port": int(parsed.port),
                    "users": [user],
                }
            ]
        },
        "streamSettings": _stream_settings_from_query(qs),
    }


def _outbound_trojan(server: V2RayServer) -> dict[str, Any] | None:
    parsed = urlparse(server.link)
    password = unquote(parsed.username or "")
    if parsed.password:
        password = f"{password}:{unquote(parsed.password)}"
    if not parsed.hostname or parsed.port is None or not password:
        return None
    qs = parse_qs(parsed.query)
    return {
        "protocol": "trojan",
        "settings": {
            "servers": [
                {
                    "address": parsed.hostname,
                    "port": int(parsed.port),
                    "password": password,
                }
            ]
        },
        "streamSettings": _stream_settings_from_query(qs, default_security="tls"),
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

    if parsed.hostname and parsed.port:
        host = parsed.hostname
        port = int(parsed.port)
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
            port = int(hostport.split("]:", 1)[1])
        else:
            host, port_s = hostport.rsplit(":", 1)
            port = int(port_s)

    if not host or not method or not password or not (1 <= port <= 65535):
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
    if server.scheme == "vmess":
        return _outbound_vmess(server)
    if server.scheme == "vless":
        return _outbound_vless(server)
    if server.scheme == "trojan":
        return _outbound_trojan(server)
    if server.scheme == "ss":
        return _outbound_ss(server)
    return None


def build_xray_config(outbound: dict[str, Any], socks_port: int) -> dict[str, Any]:
    outbound = dict(outbound)
    outbound.setdefault("tag", "proxy")
    return {
        "log": {"loglevel": "error"},
        "inbounds": [
            {
                "tag": "socks-in",
                "listen": "127.0.0.1",
                "port": socks_port,
                "protocol": "socks",
                "settings": {"udp": False, "auth": "noauth"},
            }
        ],
        "outbounds": [
            outbound,
            {"protocol": "freedom", "tag": "direct"},
            {"protocol": "blackhole", "tag": "block"},
        ],
    }


def dumps_config(config: dict[str, Any]) -> str:
    return json.dumps(config, ensure_ascii=False, indent=2)
