"""Persistent gRPC channel to a local Xray API inbound."""

from __future__ import annotations

import grpc

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path

ensure_generated_path()

from app.proxyman.command import command_pb2_grpc as handler_grpc  # noqa: E402
from app.router.command import command_pb2_grpc as routing_grpc  # noqa: E402
from app.stats.command import command_pb2_grpc as stats_grpc  # noqa: E402


class XrayControlChannel:
    """One insecure local channel + Handler / Routing / Stats stubs."""

    def __init__(self, host: str = "127.0.0.1", port: int = 20802) -> None:
        self.target = f"{host}:{int(port)}"
        self.channel: grpc.aio.Channel | None = None
        self.handler: handler_grpc.HandlerServiceStub | None = None
        self.routing: routing_grpc.RoutingServiceStub | None = None
        self.stats: stats_grpc.StatsServiceStub | None = None

    async def connect(self) -> None:
        if self.channel is not None:
            return
        self.channel = grpc.aio.insecure_channel(self.target)
        self.handler = handler_grpc.HandlerServiceStub(self.channel)
        self.routing = routing_grpc.RoutingServiceStub(self.channel)
        self.stats = stats_grpc.StatsServiceStub(self.channel)

    async def close(self) -> None:
        channel = self.channel
        self.channel = None
        self.handler = None
        self.routing = None
        self.stats = None
        if channel is not None:
            await channel.close()

    async def __aenter__(self) -> "XrayControlChannel":
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
