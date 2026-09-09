"""Convert Xray JSON outbound dicts to HandlerService protobuf messages."""

from __future__ import annotations

import base64
import ipaddress
from typing import Any

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path

ensure_generated_path()

from app.proxyman import config_pb2 as proxyman_pb2  # noqa: E402
from common.net import address_pb2  # noqa: E402
from common.protocol import headers_pb2, server_spec_pb2, user_pb2  # noqa: E402
from common.serial import typed_message_pb2  # noqa: E402
from core import config_pb2 as core_pb2  # noqa: E402
from proxy.blackhole import config_pb2 as blackhole_pb2  # noqa: E402
from proxy.shadowsocks import config_pb2 as ss_pb2  # noqa: E402
from proxy.trojan import config_pb2 as trojan_pb2  # noqa: E402
from proxy.vless import account_pb2 as vless_account_pb2  # noqa: E402
from proxy.vless.outbound import config_pb2 as vless_out_pb2  # noqa: E402
from proxy.vmess import account_pb2 as vmess_account_pb2  # noqa: E402
from proxy.vmess.outbound import config_pb2 as vmess_out_pb2  # noqa: E402
from transport.internet import config_pb2 as internet_pb2  # noqa: E402
from transport.internet import grpc  # noqa: F401,E402  # package
from transport.internet.grpc import config_pb2 as grpc_pb2  # noqa: E402
from transport.internet.httpupgrade import config_pb2 as httpupgrade_pb2  # noqa: E402
from transport.internet.reality import config_pb2 as reality_pb2  # noqa: E402
from transport.internet.tls import config_pb2 as tls_pb2  # noqa: E402
from transport.internet.websocket import config_pb2 as ws_pb2  # noqa: E402

_SS_CIPHER = {
    "aes-128-gcm": ss_pb2.AES_128_GCM,
    "aes-256-gcm": ss_pb2.AES_256_GCM,
    "chacha20-ietf-poly1305": ss_pb2.CHACHA20_POLY1305,
    "chacha20-poly1305": ss_pb2.CHACHA20_POLY1305,
    "xchacha20-ietf-poly1305": ss_pb2.XCHACHA20_POLY1305,
    "xchacha20-poly1305": ss_pb2.XCHACHA20_POLY1305,
    "none": ss_pb2.NONE,
}

_VMESS_SECURITY = {
    "auto": headers_pb2.AUTO,
    "aes-128-gcm": headers_pb2.AES128_GCM,
    "chacha20-poly1305": headers_pb2.CHACHA20_POLY1305,
    "none": headers_pb2.NONE,
    "zero": headers_pb2.ZERO,
}

_NETWORK_NAME = {
    "ws": "websocket",
    "websocket": "websocket",
    "grpc": "grpc",
    "tcp": "tcp",
    "httpupgrade": "httpupgrade",
    "http": "http",
    "h2": "http",
}


def to_typed(message) -> typed_message_pb2.TypedMessage:
    return typed_message_pb2.TypedMessage(
        type=message.DESCRIPTOR.full_name,
        value=message.SerializeToString(),
    )


def _ip_or_domain(host: str) -> address_pb2.IPOrDomain:
    host = (host or "").strip().strip("[]")
    try:
        ip = ipaddress.ip_address(host)
        return address_pb2.IPOrDomain(ip=ip.packed)
    except ValueError:
        return address_pb2.IPOrDomain(domain=host)


def _b64url(raw: str) -> bytes:
    text = (raw or "").strip().replace("-", "+").replace("_", "/")
    pad = (-len(text)) % 4
    try:
        return base64.b64decode(text + ("=" * pad))
    except Exception:
        return b""


def _hex_bytes(raw: str) -> bytes:
    text = (raw or "").strip()
    if not text:
        return b""
    try:
        return bytes.fromhex(text)
    except ValueError:
        return text.encode("utf-8", errors="replace")


def _user(account_msg, *, email: str = "") -> user_pb2.User:
    return user_pb2.User(level=0, email=email or "", account=to_typed(account_msg))


def _endpoint(host: str, port: int, user: user_pb2.User) -> server_spec_pb2.ServerEndpoint:
    return server_spec_pb2.ServerEndpoint(
        address=_ip_or_domain(host),
        port=int(port),
        user=user,
    )


def _stream_config(stream: dict[str, Any] | None) -> internet_pb2.StreamConfig | None:
    if not stream:
        return None
    network = str(stream.get("network") or "tcp").lower()
    protocol_name = _NETWORK_NAME.get(network, network)
    security = str(stream.get("security") or "none").lower()
    cfg = internet_pb2.StreamConfig(protocol_name=protocol_name)

    if protocol_name == "websocket":
        ws = stream.get("wsSettings") or {}
        headers = ws.get("headers") or {}
        host = str(headers.get("Host") or headers.get("host") or "")
        path = str(ws.get("path") or "/")
        header_map = {str(k): str(v) for k, v in headers.items()}
        transport = internet_pb2.TransportConfig(
            protocol_name="websocket",
            settings=to_typed(ws_pb2.Config(host=host, path=path, header=header_map)),
        )
        cfg.transport_settings.append(transport)
    elif protocol_name == "grpc":
        grpc_s = stream.get("grpcSettings") or {}
        transport = internet_pb2.TransportConfig(
            protocol_name="grpc",
            settings=to_typed(
                grpc_pb2.Config(service_name=str(grpc_s.get("serviceName") or ""))
            ),
        )
        cfg.transport_settings.append(transport)
    elif protocol_name == "httpupgrade":
        hu = stream.get("httpupgradeSettings") or {}
        transport = internet_pb2.TransportConfig(
            protocol_name="httpupgrade",
            settings=to_typed(
                httpupgrade_pb2.Config(
                    host=str(hu.get("host") or ""),
                    path=str(hu.get("path") or "/"),
                )
            ),
        )
        cfg.transport_settings.append(transport)

    if security == "tls":
        tls = stream.get("tlsSettings") or {}
        tls_cfg = tls_pb2.Config(
            server_name=str(tls.get("serverName") or ""),
            fingerprint=str(tls.get("fingerprint") or ""),
        )
        alpn = tls.get("alpn") or []
        if isinstance(alpn, str):
            alpn = [p.strip() for p in alpn.split(",") if p.strip()]
        for proto in alpn:
            tls_cfg.next_protocol.append(str(proto))
        cfg.security_type = tls_cfg.DESCRIPTOR.full_name
        cfg.security_settings.append(to_typed(tls_cfg))
    elif security == "reality":
        reality = stream.get("realitySettings") or {}
        password = str(reality.get("password") or reality.get("publicKey") or "")
        short_id = str(reality.get("shortId") or "")
        reality_cfg = reality_pb2.Config(
            Fingerprint=str(reality.get("fingerprint") or "chrome"),
            server_name=str(reality.get("serverName") or ""),
            public_key=_b64url(password),
            short_id=_hex_bytes(short_id),
            spider_x=str(reality.get("spiderX") or ""),
        )
        cfg.security_type = reality_cfg.DESCRIPTOR.full_name
        cfg.security_settings.append(to_typed(reality_cfg))

    return cfg


def _sender(stream: dict[str, Any] | None) -> typed_message_pb2.TypedMessage | None:
    stream_cfg = _stream_config(stream)
    if stream_cfg is None:
        return None
    sender = proxyman_pb2.SenderConfig(stream_settings=stream_cfg)
    return to_typed(sender)


def _vless_proxy(settings: dict[str, Any]) -> typed_message_pb2.TypedMessage:
    vnext = (settings.get("vnext") or [{}])[0]
    users = (vnext.get("users") or [{}])[0]
    account = vless_account_pb2.Account(
        id=str(users.get("id") or ""),
        encryption=str(users.get("encryption") or "none"),
        flow=str(users.get("flow") or ""),
    )
    endpoint = _endpoint(
        str(vnext.get("address") or ""),
        int(vnext.get("port") or 0),
        _user(account),
    )
    return to_typed(vless_out_pb2.Config(vnext=endpoint))


def _vmess_proxy(settings: dict[str, Any]) -> typed_message_pb2.TypedMessage:
    vnext = (settings.get("vnext") or [{}])[0]
    users = (vnext.get("users") or [{}])[0]
    cipher = str(users.get("security") or "auto").lower()
    account = vmess_account_pb2.Account(
        id=str(users.get("id") or ""),
        security_settings=headers_pb2.SecurityConfig(
            type=_VMESS_SECURITY.get(cipher, headers_pb2.AUTO)
        ),
    )
    endpoint = _endpoint(
        str(vnext.get("address") or ""),
        int(vnext.get("port") or 0),
        _user(account),
    )
    return to_typed(vmess_out_pb2.Config(Receiver=endpoint))


def _trojan_proxy(settings: dict[str, Any]) -> typed_message_pb2.TypedMessage:
    server = (settings.get("servers") or [{}])[0]
    account = trojan_pb2.Account(password=str(server.get("password") or ""))
    endpoint = _endpoint(
        str(server.get("address") or ""),
        int(server.get("port") or 0),
        _user(account),
    )
    return to_typed(trojan_pb2.ClientConfig(server=endpoint))


def _ss_proxy(settings: dict[str, Any]) -> typed_message_pb2.TypedMessage:
    server = (settings.get("servers") or [{}])[0]
    method = str(server.get("method") or "").lower()
    account = ss_pb2.Account(
        password=str(server.get("password") or ""),
        cipher_type=_SS_CIPHER.get(method, ss_pb2.UNKNOWN),
    )
    endpoint = _endpoint(
        str(server.get("address") or ""),
        int(server.get("port") or 0),
        _user(account),
    )
    return to_typed(ss_pb2.ClientConfig(server=endpoint))


def _blackhole_proxy() -> typed_message_pb2.TypedMessage:
    return to_typed(blackhole_pb2.Config())


def json_outbound_to_handler_config(
    outbound: dict[str, Any],
    *,
    tag: str | None = None,
) -> core_pb2.OutboundHandlerConfig:
    """Build HandlerService AddOutbound payload from an Xray JSON outbound dict."""
    protocol = str(outbound.get("protocol") or "").lower()
    settings = outbound.get("settings") or {}
    stream = outbound.get("streamSettings")
    used_tag = tag or str(outbound.get("tag") or "")
    if not used_tag:
        raise ValueError("outbound tag is required")

    if protocol == "vless":
        proxy = _vless_proxy(settings)
    elif protocol == "vmess":
        proxy = _vmess_proxy(settings)
    elif protocol == "trojan":
        proxy = _trojan_proxy(settings)
    elif protocol in {"shadowsocks", "ss"}:
        proxy = _ss_proxy(settings)
    elif protocol == "blackhole":
        proxy = _blackhole_proxy()
    else:
        raise ValueError(f"unsupported outbound protocol: {protocol}")

    config = core_pb2.OutboundHandlerConfig(tag=used_tag, proxy_settings=proxy)
    sender = _sender(stream)
    if sender is not None:
        config.sender_settings.CopyFrom(sender)
    return config
