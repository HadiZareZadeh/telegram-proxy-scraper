"""RoutingService helpers — OverrideBalancerTarget on the hot path."""

from __future__ import annotations

import asyncio

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path
from fetch_mtproto.v2ray.xray_control.channel import XrayControlChannel

ensure_generated_path()

from app.router.command import command_pb2 as routing_pb2  # noqa: E402


class RoutingClient:
    def __init__(self, control: XrayControlChannel) -> None:
        self._control = control

    def _stub(self):
        if self._control.routing is None:
            raise RuntimeError("Xray control channel is not connected")
        return self._control.routing

    async def override_balancer_target(self, balancer_tag: str, target: str) -> None:
        await self._stub().OverrideBalancerTarget(
            routing_pb2.OverrideBalancerTargetRequest(
                balancerTag=balancer_tag,
                target=target,
            ),
            timeout=2.0,
        )

    async def override_many(self, assignments: dict[str, str]) -> list[str | None]:
        """Parallel balancer overrides. Returns per-tag error or None."""
        tags = list(assignments.items())

        async def _one(balancer: str, target: str) -> str | None:
            try:
                await self.override_balancer_target(balancer, target)
                return None
            except Exception as exc:
                return str(exc) or type(exc).__name__

        return await asyncio.gather(*(_one(b, t) for b, t in tags))
