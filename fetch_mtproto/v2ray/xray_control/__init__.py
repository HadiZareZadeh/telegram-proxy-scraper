"""Native gRPC control plane for a running Xray process."""

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path

ensure_generated_path()

from fetch_mtproto.v2ray.xray_control.channel import XrayControlChannel
from fetch_mtproto.v2ray.xray_control.handler import HandlerClient
from fetch_mtproto.v2ray.xray_control.routing import RoutingClient
from fetch_mtproto.v2ray.xray_control.stats import StatsClient

__all__ = [
    "HandlerClient",
    "RoutingClient",
    "StatsClient",
    "XrayControlChannel",
]
