"""StatsService QueryStats over gRPC (no xray CLI)."""

from __future__ import annotations

from fetch_mtproto.v2ray.xray_control._paths import ensure_generated_path
from fetch_mtproto.v2ray.xray_control.channel import XrayControlChannel

ensure_generated_path()

from app.stats.command import command_pb2 as stats_pb2  # noqa: E402


class StatsClient:
    def __init__(self, control: XrayControlChannel) -> None:
        self._control = control

    def _stub(self):
        if self._control.stats is None:
            raise RuntimeError("Xray control channel is not connected")
        return self._control.stats

    async def query(self, pattern: str = "", *, reset: bool = False) -> dict[str, int]:
        response = await self._stub().QueryStats(
            stats_pb2.QueryStatsRequest(pattern=pattern, reset=reset),
            timeout=2.0,
        )
        out: dict[str, int] = {}
        for item in response.stat:
            try:
                out[str(item.name)] = int(item.value)
            except (TypeError, ValueError):
                continue
        return out

    async def outbound_traffic(self) -> dict[str, tuple[int, int]]:
        """tag -> (uplink, downlink) for outbound>>>tag>>>traffic>>>..."""
        stats = await self.query("outbound>>>")
        by_tag: dict[str, list[int]] = {}
        for name, value in stats.items():
            parts = name.split(">>>")
            if len(parts) != 4 or parts[0] != "outbound" or parts[2] != "traffic":
                continue
            tag = parts[1]
            pair = by_tag.setdefault(tag, [0, 0])
            if parts[3] == "uplink":
                pair[0] = value
            elif parts[3] == "downlink":
                pair[1] = value
        return {tag: (pair[0], pair[1]) for tag, pair in by_tag.items()}
