"""HandlerService helpers (Add/Remove outbound) over a persistent gRPC channel."""

from __future__ import annotations

from typing import Any

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path
from fetch_mtproto.v2ray.xray_control.channel import XrayControlChannel
from fetch_mtproto.v2ray.xray_control.outbound_pb import json_outbound_to_handler_config

ensure_generated_path()

from app.proxyman.command import command_pb2 as handler_pb2  # noqa: E402


class HandlerClient:
    def __init__(self, control: XrayControlChannel) -> None:
        self._control = control

    def _stub(self):
        if self._control.handler is None:
            raise RuntimeError("Xray control channel is not connected")
        return self._control.handler

    async def add_outbound(self, outbound: dict[str, Any], *, tag: str) -> None:
        config = json_outbound_to_handler_config(outbound, tag=tag)
        await self._stub().AddOutbound(
            handler_pb2.AddOutboundRequest(outbound=config),
            timeout=5.0,
        )

    async def remove_outbound(self, tag: str) -> None:
        try:
            await self._stub().RemoveOutbound(
                handler_pb2.RemoveOutboundRequest(tag=tag),
                timeout=5.0,
            )
        except Exception:
            # Missing tag is fine (first swap / already cleared).
            return

    async def replace_outbound(self, outbound: dict[str, Any], *, tag: str) -> None:
        await self.remove_outbound(tag)
        await self.add_outbound(outbound, tag=tag)
