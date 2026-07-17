"""Subscription HTTP server helpers (URLs, LAN discovery, QR codes)."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8765


def get_lan_ip() -> str | None:
    """Return the primary LAN IPv4 address, or None if unavailable."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return None


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
) -> list[tuple[str, str]]:
    """Return (label, url) pairs for reachable subscription endpoints."""
    urls: list[tuple[str, str]] = [
        ("Local", f"http://127.0.0.1:{port}/{filename}"),
    ]
    if bind_host in {"0.0.0.0", "::"}:
        lan = get_lan_ip()
        if lan:
            urls.append(("LAN", f"http://{lan}:{port}/{filename}"))
    elif bind_host not in {"127.0.0.1", "localhost"}:
        urls.append(("Network", f"http://{bind_host}:{port}/{filename}"))
    return urls


def primary_subscription_url(*, bind_host: str, port: int, filename: str) -> str:
    """Best URL for QR / copy — prefer LAN when bound to all interfaces."""
    for label, url in subscription_urls(
        bind_host=bind_host, port=port, filename=filename
    ):
        if label == "LAN":
            return url
    return subscription_urls(bind_host=bind_host, port=port, filename=filename)[0][1]


def print_subscription_urls(*, bind_host: str, port: int, filename: str) -> None:
    for label, url in subscription_urls(
        bind_host=bind_host, port=port, filename=filename
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
