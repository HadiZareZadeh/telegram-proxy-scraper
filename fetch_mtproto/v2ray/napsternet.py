"""Extract V2Ray share links from non-encrypted Napsternet / NPV Tunnel config files."""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path

from fetch_mtproto.v2ray.store import (
    V2RayServer,
    extract_v2ray_from_text,
    parse_v2ray_link,
)
from fetch_mtproto.v2ray.subscription_import import expand_subscription_payload

log = logging.getLogger("mtproto-scraper")

NAPSTERNET_EXTENSIONS = frozenset({".npv4", ".npv", ".npv2", ".npvt", ".inpv"})
_ENCRYPTED_HEADERS = ("NPVT1", "NPVTSUB1")
_MAX_FILE_BYTES = 2_000_000
_BINARY_LINK_RE = re.compile(
    rb"(?P<link>(?:vmess|vless|trojan|ss|ssr|hysteria|hysteria2|tuic|wireguard)://[^\x00-\x20\"'<>]+)",
    re.IGNORECASE,
)


def is_napsternet_filename(name: str) -> bool:
    return Path(name).suffix.lower() in NAPSTERNET_EXTENSIONS


def is_encrypted_napsternet(data: bytes) -> bool:
    """True for NPVT1 / NPVTSUB1 white-box encrypted configs."""
    head = data[:32].decode("utf-8", errors="ignore").lstrip()
    return any(head.startswith(header) for header in _ENCRYPTED_HEADERS)


def _merge(servers: list[V2RayServer]) -> list[V2RayServer]:
    merged: dict[str, V2RayServer] = {}
    for server in servers:
        merged[server.key] = server
    return list(merged.values())


def _decode_text_blobs(data: bytes) -> list[str]:
    texts: list[str] = []
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = data.decode(encoding)
        except UnicodeDecodeError:
            continue
        text = text.strip()
        if text and text not in texts:
            texts.append(text)
    return texts


def _json_chunks_from_text(text: str) -> list[object]:
    chunks: list[object] = []
    stripped = text.strip()
    if not stripped:
        return chunks
    try:
        chunks.append(json.loads(stripped))
    except json.JSONDecodeError:
        pass
    for match in re.finditer(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", text, re.DOTALL):
        snippet = match.group(0)
        try:
            chunks.append(json.loads(snippet))
        except json.JSONDecodeError:
            continue
    return chunks


def _walk_json_strings(obj: object, sink: list[str]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in {"v2rayJson", "v2ray", "config", "raw", "subscription"} and isinstance(
                value, str
            ):
                sink.append(value)
            _walk_json_strings(value, sink)
    elif isinstance(obj, list):
        for item in obj:
            _walk_json_strings(item, sink)
    elif isinstance(obj, str) and len(obj) >= 8:
        sink.append(obj)


def _vmess_link_from_outbound(outbound: dict) -> str | None:
    settings = outbound.get("settings")
    if not isinstance(settings, dict):
        return None
    vnext_list = settings.get("vnext")
    if not isinstance(vnext_list, list) or not vnext_list:
        return None
    vnext = vnext_list[0]
    if not isinstance(vnext, dict):
        return None
    users = vnext.get("users")
    if not isinstance(users, list) or not users:
        return None
    user = users[0]
    if not isinstance(user, dict):
        return None

    stream = outbound.get("streamSettings")
    stream = stream if isinstance(stream, dict) else {}
    network = str(stream.get("network") or "tcp")
    security = str(stream.get("security") or "none").lower()
    if security in {"", "0", "none"}:
        security = ""

    tls_settings = stream.get("tlsSettings") or stream.get("realitySettings") or {}
    if not isinstance(tls_settings, dict):
        tls_settings = {}
    ws_settings = stream.get("wsSettings") or {}
    if not isinstance(ws_settings, dict):
        ws_settings = {}
    ws_headers = ws_settings.get("headers") or {}
    if not isinstance(ws_headers, dict):
        ws_headers = {}

    host = str(vnext.get("address") or "").strip()
    port = vnext.get("port")
    uuid = str(user.get("id") or "").strip()
    if not host or port is None or not uuid:
        return None

    obj: dict[str, object] = {
        "v": "2",
        "ps": str(outbound.get("tag") or outbound.get("remarks") or ""),
        "add": host,
        "port": str(port),
        "id": uuid,
        "aid": str(user.get("alterId") or user.get("alterID") or 0),
        "scy": str(user.get("security") or "auto"),
        "net": network,
        "type": "none",
        "tls": security if security not in {"", "none"} else "",
    }
    sni = str(tls_settings.get("serverName") or ws_headers.get("Host") or "").strip()
    if sni:
        obj["sni"] = sni
        obj["host"] = sni
    path = str(ws_settings.get("path") or "").strip()
    if path:
        obj["path"] = path

    encoded = base64.b64encode(json.dumps(obj, ensure_ascii=False).encode("utf-8")).decode(
        "ascii"
    )
    return f"vmess://{encoded}"


def _uri_link_from_outbound(outbound: dict, scheme: str) -> str | None:
    settings = outbound.get("settings")
    if not isinstance(settings, dict):
        return None

    stream = outbound.get("streamSettings")
    stream = stream if isinstance(stream, dict) else {}
    network = str(stream.get("network") or "tcp")
    security = str(stream.get("security") or "none").lower()
    tls_settings = stream.get("tlsSettings") or stream.get("realitySettings") or {}
    if not isinstance(tls_settings, dict):
        tls_settings = {}
    ws_settings = stream.get("wsSettings") or {}
    if not isinstance(ws_settings, dict):
        ws_settings = {}

    if scheme == "vless":
        vnext_list = settings.get("vnext")
        if not isinstance(vnext_list, list) or not vnext_list:
            return None
        vnext = vnext_list[0]
        users = vnext.get("users") if isinstance(vnext, dict) else None
        if not isinstance(users, list) or not users:
            return None
        user = users[0]
        host = str(vnext.get("address") or "").strip()
        port = vnext.get("port")
        uuid = str(user.get("id") or "").strip()
        if not host or port is None or not uuid:
            return None
        params = [f"type={network}"]
        if security not in {"", "none"}:
            params.append(f"security={security}")
        sni = str(tls_settings.get("serverName") or "").strip()
        if sni:
            params.append(f"sni={sni}")
        path = str(ws_settings.get("path") or "").strip()
        if path:
            params.append(f"path={path}")
        flow = str(user.get("flow") or "").strip()
        if flow:
            params.append(f"flow={flow}")
        encryption = str(user.get("encryption") or "none")
        params.append(f"encryption={encryption}")
        tag = str(outbound.get("tag") or "")
        query = "&".join(params)
        return f"vless://{uuid}@{host}:{port}?{query}#{tag}" if query else f"vless://{uuid}@{host}:{port}#{tag}"

    if scheme == "trojan":
        servers = settings.get("servers")
        if not isinstance(servers, list) or not servers:
            return None
        server = servers[0]
        if not isinstance(server, dict):
            return None
        host = str(server.get("address") or "").strip()
        port = server.get("port")
        password = str(server.get("password") or "").strip()
        if not host or port is None or not password:
            return None
        params = [f"type={network}"]
        sni = str(tls_settings.get("serverName") or server.get("sni") or host).strip()
        if sni:
            params.append(f"sni={sni}")
        path = str(ws_settings.get("path") or "").strip()
        if path:
            params.append(f"path={path}")
        tag = str(outbound.get("tag") or "")
        query = "&".join(params)
        return f"trojan://{password}@{host}:{port}?{query}#{tag}" if query else f"trojan://{password}@{host}:{port}#{tag}"

    if scheme in {"ss", "shadowsocks"}:
        servers = settings.get("servers")
        if not isinstance(servers, list) or not servers:
            return None
        server = servers[0]
        if not isinstance(server, dict):
            return None
        host = str(server.get("address") or "").strip()
        port = server.get("port")
        method = str(server.get("method") or "").strip()
        password = str(server.get("password") or "").strip()
        if not host or port is None or not method or not password:
            return None
        userinfo = base64.urlsafe_b64encode(f"{method}:{password}".encode()).decode().rstrip("=")
        tag = str(outbound.get("tag") or "")
        return f"ss://{userinfo}@{host}:{port}#{tag}"

    return None


def _servers_from_xray_json(config: object) -> list[V2RayServer]:
    if not isinstance(config, dict):
        return []
    outbounds = config.get("outbounds")
    if not isinstance(outbounds, list):
        return []

    found: list[V2RayServer] = []
    for outbound in outbounds:
        if not isinstance(outbound, dict):
            continue
        tag = str(outbound.get("tag") or "").lower()
        if tag in {"direct", "block", "dns-out"}:
            continue
        protocol = str(outbound.get("protocol") or "").lower()
        link: str | None = None
        if protocol == "vmess":
            link = _vmess_link_from_outbound(outbound)
        elif protocol == "vless":
            link = _uri_link_from_outbound(outbound, "vless")
        elif protocol == "trojan":
            link = _uri_link_from_outbound(outbound, "trojan")
        elif protocol in {"ss", "shadowsocks"}:
            link = _uri_link_from_outbound(outbound, "ss")
        if not link:
            continue
        server = parse_v2ray_link(link)
        if server:
            found.append(server)
    return found


def _servers_from_json_tree(obj: object) -> list[V2RayServer]:
    found: list[V2RayServer] = []
    found.extend(_servers_from_xray_json(obj))

    extra_strings: list[str] = []
    _walk_json_strings(obj, extra_strings)
    for chunk in extra_strings:
        for nested in _json_chunks_from_text(chunk):
            found.extend(_servers_from_json_tree(nested))
        found.extend(extract_v2ray_from_text(chunk))
        found.extend(expand_subscription_payload(chunk))
    return found


def _servers_from_binary_scan(data: bytes) -> list[V2RayServer]:
    found: dict[str, V2RayServer] = {}
    for match in _BINARY_LINK_RE.finditer(data):
        try:
            link = match.group("link").decode("ascii", errors="ignore")
        except UnicodeDecodeError:
            continue
        server = parse_v2ray_link(link)
        if server:
            found[server.key] = server
    return list(found.values())


def extract_v2ray_from_napsternet_file(
    data: bytes,
    *,
    filename: str = "",
) -> list[V2RayServer]:
    """Parse a non-encrypted Napsternet file into individual V2Ray servers."""
    if not data:
        return []
    if len(data) > _MAX_FILE_BYTES:
        log.debug("Napsternet file too large (%s)", filename or "attachment")
        return []
    if is_encrypted_napsternet(data):
        log.debug("Skipping encrypted Napsternet file %s", filename or "attachment")
        return []

    found: list[V2RayServer] = []

    for text in _decode_text_blobs(data):
        found.extend(extract_v2ray_from_text(text))
        found.extend(expand_subscription_payload(text))
        for obj in _json_chunks_from_text(text):
            found.extend(_servers_from_json_tree(obj))

    found.extend(_servers_from_binary_scan(data))
    return _merge(found)
