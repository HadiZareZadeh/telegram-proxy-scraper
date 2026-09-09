"""Long-lived Xray process: fixed local ports, swap upstreams via Handler API."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fetch_mtproto.process_tree import hide_console_kwargs, kill_pid_tree
from fetch_mtproto.v2ray.xray import (
    XrayRouteEntry,
    build_xray_routed_config,
    dumps_config,
)


@dataclass(frozen=True, slots=True)
class LiveSlotSpec:
    """One fixed local SOCKS/HTTP inbound routed to a stable outbound tag."""

    tag: str
    socks_port: int | None = None
    http_port: int | None = None


def blackhole_outbound(tag: str) -> dict[str, Any]:
    return {"protocol": "blackhole", "tag": tag, "settings": {}}


def build_xray_live_shell_config(
    slots: list[LiveSlotSpec],
    *,
    api_port: int,
) -> dict[str, Any]:
    """Shell config: N local inbounds + blackhole placeholders + Handler API."""
    if not slots:
        raise ValueError("build_xray_live_shell_config requires at least one slot")
    entries = [
        XrayRouteEntry(
            outbound=blackhole_outbound(slot.tag),
            tag=slot.tag,
            socks_port=slot.socks_port,
            http_port=slot.http_port,
        )
        for slot in slots
    ]
    return build_xray_routed_config(entries, api_port=int(api_port))


def ping_live_slots(base_port: int, concurrency: int) -> list[LiveSlotSpec]:
    """SOCKS slots for Ping V2Ray (tags proxy-0 .. proxy-N-1)."""
    base = int(base_port)
    count = max(1, int(concurrency))
    return [
        LiveSlotSpec(tag=f"proxy-{index}", socks_port=base + index)
        for index in range(count)
    ]


def ping_api_port(base_port: int, concurrency: int) -> int:
    from fetch_mtproto.v2ray.pool_ports import DEFAULT_PING_API_PORT

    del base_port, concurrency
    return DEFAULT_PING_API_PORT


def pool_test_live_slot(base_port: int) -> LiveSlotSpec:
    """Single SOCKS+HTTP slot for proxy-pool validation (tag proxy-test)."""
    base = int(base_port)
    return LiveSlotSpec(tag="proxy-test", socks_port=base, http_port=base + 1)


def pool_test_api_port(base_port: int) -> int:
    return int(base_port) + 2


def _port_is_open(host: str, port: int, *, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


class XrayLiveSession:
    """One Xray process; swap outbound tags via HandlerService gRPC."""

    def __init__(
        self,
        *,
        bin_path: str,
        slots: list[LiveSlotSpec],
        api_port: int,
        prefix: str = "xray-live",
    ) -> None:
        if not slots:
            raise ValueError("XrayLiveSession requires at least one slot")
        self.bin_path = bin_path
        self.slots = list(slots)
        self.api_port = int(api_port)
        self.prefix = prefix
        self._proc: subprocess.Popen | None = None
        self._cfg_path: str | None = None
        self._tags = [slot.tag for slot in self.slots]
        self._tag_index = {slot.tag: index for index, slot in enumerate(self.slots)}
        self._control = None
        self._handler = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def socks_port(self, index: int = 0) -> int | None:
        return self.slots[index].socks_port

    def http_port(self, index: int = 0) -> int | None:
        return self.slots[index].http_port

    def start(self, *, ready_timeout: float = 12.0) -> None:
        if self.running:
            return
        config = build_xray_live_shell_config(self.slots, api_port=self.api_port)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
            prefix=f"{self.prefix}-",
        ) as handle:
            handle.write(dumps_config(config))
            self._cfg_path = handle.name

        try:
            err_path = Path(self._cfg_path).with_suffix(".err")
            self._err_file = open(err_path, "w", encoding="utf-8")
            self._proc = subprocess.Popen(
                [self.bin_path, "run", "-c", self._cfg_path],
                stdout=subprocess.DEVNULL,
                stderr=self._err_file,
                **hide_console_kwargs(),
            )
        except OSError:
            if getattr(self, "_err_file", None) is not None:
                try:
                    self._err_file.close()
                except Exception:
                    pass
                self._err_file = None
            self._cleanup_cfg()
            raise

        try:
            self._wait_ready(timeout=ready_timeout)
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        proc = self._proc
        self._proc = None
        err_file = getattr(self, "_err_file", None)
        self._err_file = None
        if proc is not None and proc.poll() is None:
            try:
                kill_pid_tree(proc.pid, timeout=3.0)
            except Exception:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            try:
                proc.wait(timeout=3.0)
            except Exception:
                pass
        if err_file is not None:
            try:
                err_file.close()
            except Exception:
                pass
        self._cleanup_cfg()

    def set_slot_outbound(self, index: int, outbound: dict[str, Any] | None) -> None:
        raise RuntimeError("use set_outbounds_async — CLI Handler API is removed")

    def set_outbounds(
        self, outbounds: list[dict[str, Any] | None]
    ) -> list[str | None]:
        raise RuntimeError("use set_outbounds_async — CLI Handler API is removed")

    async def connect_control(self) -> None:
        from fetch_mtproto.v2ray.xray_control import HandlerClient, XrayControlChannel

        if self._control is not None:
            return
        control = XrayControlChannel(port=self.api_port)
        await control.connect()
        self._control = control
        self._handler = HandlerClient(control)

    async def close_control(self) -> None:
        control = self._control
        self._control = None
        self._handler = None
        if control is not None:
            await control.close()

    async def set_outbounds_async(
        self, outbounds: list[dict[str, Any] | None]
    ) -> list[str | None]:
        """Replace slot outbounds in parallel via HandlerService gRPC."""
        import asyncio

        if len(outbounds) > len(self.slots):
            raise ValueError(
                f"too many outbounds ({len(outbounds)}) for {len(self.slots)} slots"
            )
        await self.connect_control()
        handler = self._handler
        assert handler is not None
        errors: list[str | None] = [None] * len(self.slots)

        async def _one(index: int) -> None:
            outbound = outbounds[index] if index < len(outbounds) else None
            tag = self.slots[index].tag
            payload = blackhole_outbound(tag) if outbound is None else dict(outbound)
            payload["tag"] = tag
            try:
                await handler.replace_outbound(payload, tag=tag)
            except Exception as exc:
                errors[index] = str(exc) or type(exc).__name__
                try:
                    await handler.replace_outbound(blackhole_outbound(tag), tag=tag)
                except Exception:
                    pass

        await asyncio.gather(*(_one(i) for i in range(len(self.slots))))
        return errors

    def _wait_ready(self, *, timeout: float) -> None:
        ports: list[int] = [self.api_port]
        for slot in self.slots:
            if slot.socks_port is not None:
                ports.append(int(slot.socks_port))
            if slot.http_port is not None:
                ports.append(int(slot.http_port))
        deadline = time.perf_counter() + max(1.0, float(timeout))
        while time.perf_counter() < deadline:
            if self._proc is not None and self._proc.poll() is not None:
                raise RuntimeError(
                    f"Xray exited early (code {self._proc.returncode})"
                )
            if all(_port_is_open("127.0.0.1", port) for port in ports):
                return
            time.sleep(0.05)
        missing = [p for p in ports if not _port_is_open("127.0.0.1", p)]
        raise TimeoutError(
            f"Xray live session ports not ready: {missing}"
        )

    def _cleanup_cfg(self) -> None:
        path = self._cfg_path
        self._cfg_path = None
        if not path:
            return
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            pass

    def __enter__(self) -> XrayLiveSession:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
