"""Subscription HTTP server helpers (URLs, LAN discovery, QR codes)."""

from __future__ import annotations

import ipaddress
import platform
import re
import subprocess
from typing import TYPE_CHECKING

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8765

# Substrings that identify VPN/TUN/virtual adapters (case-insensitive).
_VIRTUAL_ADAPTER_KEYWORDS = (
    "tun",
    "tap",
    "wintun",
    "nekoray",
    "meta",
    "wireguard",
    "vpn",
    "virtual",
    "hyper-v",
    "vmware",
    "vethernet",
    "wiresock",
    "openvpn",
    "loopback",
    "tunnel",
    "sing-box",
    "clash",
    "outline",
    "nordlynx",
    "tailscale",
    "zerotier",
    "hamachi",
    "bluetooth",
    "pseudo",
)


def _is_private_ipv4(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
        return addr.version == 4 and addr.is_private and not addr.is_loopback
    except ValueError:
        return False


def _is_virtual_adapter(name: str) -> bool:
    lower = name.lower()
    return any(keyword in lower for keyword in _VIRTUAL_ADAPTER_KEYWORDS)


def _score_lan_candidate(
    name: str, ip: str, gateway: str | None, connected: bool
) -> int:
    if not connected:
        return -1
    if _is_virtual_adapter(name):
        return -1
    if not _is_private_ipv4(ip):
        return -1

    score = 0
    lower = name.lower()

    if gateway and _is_private_ipv4(gateway):
        score += 100

    if "wi-fi" in lower or "wireless" in lower:
        score += 50
    elif "ethernet" in lower:
        score += 45

    if ip.startswith("192.168."):
        score += 30
    elif ip.startswith("10."):
        score += 25
    elif ip.startswith("172."):
        # 172.x is common for TUN adapters that slipped through naming filters.
        score += 5

    return score


def _windows_ipconfig_candidates() -> list[tuple[str, str, str | None, bool]]:
    """Return (adapter_name, ip, gateway, connected) from ipconfig."""
    try:
        proc = subprocess.run(
            ["ipconfig"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []

    candidates: list[tuple[str, str, str | None, bool]] = []
    current_name = ""
    current_ip: str | None = None
    current_gw: str | None = None
    connected = True

    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not line.startswith((" ", "\t")) and stripped.endswith(":"):
            if current_name and current_ip:
                candidates.append((current_name, current_ip, current_gw, connected))
            current_name = stripped[:-1].strip()
            current_ip = None
            current_gw = None
            connected = True
            continue

        if not current_name:
            continue

        lower = stripped.lower()
        if "media state" in lower and "disconnected" in lower:
            connected = False
        elif stripped.startswith(("IPv4 Address", "IP Address")) and "ipv6" not in lower:
            match = re.search(r":\s*([\d.]+)", stripped)
            if match:
                current_ip = match.group(1)
        elif stripped.startswith("Default Gateway"):
            gateway_text = stripped.split(":", 1)[-1].strip()
            for part in gateway_text.replace(",", " ").split():
                if _is_private_ipv4(part):
                    current_gw = part
                    break

    if current_name and current_ip:
        candidates.append((current_name, current_ip, current_gw, connected))

    return candidates


def _linux_ip_candidates() -> list[tuple[str, str, str | None, bool]]:
    """Return candidates from `ip -4 addr` and default routes."""
    try:
        addr_proc = subprocess.run(
            ["ip", "-4", "-o", "addr", "show"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
        route_proc = subprocess.run(
            ["ip", "-4", "route", "show", "default"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []

    gateways: dict[str, str] = {}
    for line in route_proc.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0] == "default" and parts[1] == "via":
            gateways[parts[4]] = parts[2]

    candidates: list[tuple[str, str, str | None, bool]] = []
    for line in addr_proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        iface = parts[1]
        ip = parts[3].split("/", 1)[0]
        if not _is_private_ipv4(ip):
            continue
        candidates.append((iface, ip, gateways.get(iface), True))

    return candidates


def _darwin_ifconfig_candidates() -> list[tuple[str, str, str | None, bool]]:
    """Return candidates from ifconfig on macOS."""
    try:
        proc = subprocess.run(
            ["ifconfig"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []

    candidates: list[tuple[str, str, str | None, bool]] = []
    current_name = ""
    flags = ""
    current_ip: str | None = None

    for line in proc.stdout.splitlines():
        if line and not line.startswith(("\t", " ")):
            if current_name and current_ip:
                up = "up" in flags.split() and "loopback" not in flags
                candidates.append((current_name, current_ip, None, up))
            current_name = line.split(":", 1)[0]
            flags = line.split(":", 1)[-1] if ":" in line else ""
            current_ip = None
            continue

        match = re.search(r"\binet\s+([\d.]+)\b", line)
        if match:
            current_ip = match.group(1)

    if current_name and current_ip:
        up = "up" in flags.split() and "loopback" not in flags
        candidates.append((current_name, current_ip, None, up))

    return candidates


def _collect_lan_candidates() -> list[tuple[str, str, str | None, bool]]:
    system = platform.system()
    if system == "Windows":
        return _windows_ipconfig_candidates()
    if system == "Linux":
        return _linux_ip_candidates()
    if system == "Darwin":
        return _darwin_ifconfig_candidates()
    return []


def _rank_lan_ips(candidates: list[tuple[str, str, str | None, bool]]) -> list[str]:
    scored: list[tuple[int, str]] = []
    for name, ip, gateway, connected in candidates:
        score = _score_lan_candidate(name, ip, gateway, connected)
        if score >= 0:
            scored.append((score, ip))

    scored.sort(key=lambda item: item[0], reverse=True)

    seen: set[str] = set()
    ranked: list[str] = []
    for _, ip in scored:
        if ip not in seen:
            seen.add(ip)
            ranked.append(ip)
    return ranked


def _connect_trick_ip() -> str | None:
    """Last-resort IP when interface enumeration finds nothing."""
    import socket

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
            return ip if _is_private_ipv4(ip) else None
    except OSError:
        return None


def get_lan_ips(config=None) -> list[str]:
    """Return LAN IPv4 addresses from physical adapters, best first."""
    override = None
    if config is not None:
        raw = getattr(config, "SUBSCRIPTION_SERVER_LAN_IP", None)
        if raw:
            override = str(raw).strip()
            if _is_private_ipv4(override):
                return [override]

    ranked = _rank_lan_ips(_collect_lan_candidates())
    if ranked:
        return ranked

    fallback = _connect_trick_ip()
    return [fallback] if fallback else []


def get_lan_ip(config=None) -> str | None:
    """Return the best LAN IPv4 address, or None if unavailable."""
    ips = get_lan_ips(config)
    return ips[0] if ips else None


def resolve_server_settings(config=None) -> tuple[str, int]:
    host = DEFAULT_HOST
    port = DEFAULT_PORT
    if config is not None:
        raw_host = getattr(config, "SUBSCRIPTION_SERVER_HOST", None)
        raw_port = getattr(config, "SUBSCRIPTION_SERVER_PORT", None)
        if raw_host:
            host = str(raw_host)
        if raw_port is not None:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                pass
    return host, port


def subscription_urls(
    *,
    bind_host: str,
    port: int,
    filename: str,
    config=None,
) -> list[tuple[str, str]]:
    """Return (label, url) pairs for reachable subscription endpoints."""
    urls: list[tuple[str, str]] = [
        ("Local", f"http://127.0.0.1:{port}/{filename}"),
    ]
    if bind_host in {"0.0.0.0", "::"}:
        lan_ips = get_lan_ips(config)
        for index, lan in enumerate(lan_ips):
            label = "LAN" if index == 0 else f"LAN ({lan})"
            urls.append((label, f"http://{lan}:{port}/{filename}"))
    elif bind_host not in {"127.0.0.1", "localhost"}:
        urls.append(("Network", f"http://{bind_host}:{port}/{filename}"))
    return urls


def primary_subscription_url(
    *, bind_host: str, port: int, filename: str, config=None
) -> str:
    """Best URL for QR / copy — prefer LAN when bound to all interfaces."""
    for label, url in subscription_urls(
        bind_host=bind_host, port=port, filename=filename, config=config
    ):
        if label == "LAN" or label.startswith("LAN ("):
            return url
    return subscription_urls(
        bind_host=bind_host, port=port, filename=filename, config=config
    )[0][1]


def print_subscription_urls(*, bind_host: str, port: int, filename: str) -> None:
    from fetch_mtproto.config_loader import load_config

    config = load_config(required=False)
    for label, url in subscription_urls(
        bind_host=bind_host, port=port, filename=filename, config=config
    ):
        print(f"NekoRay subscription URL ({label}): {url}")


if TYPE_CHECKING:
    from tkinter import PhotoImage


def make_qr_photoimage(url: str, size: int = 180) -> PhotoImage | None:
    """Render a subscription URL as a Tk PhotoImage QR code."""
    try:
        import qrcode
        from PIL import Image, ImageTk
    except ImportError:
        return None

    qr = qrcode.QRCode(box_size=4, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img = img.resize((size, size), Image.Resampling.NEAREST)
    return ImageTk.PhotoImage(img)
