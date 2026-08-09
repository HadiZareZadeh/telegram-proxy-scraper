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
    return int(base_port) + max(1, int(concurrency))


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
    """One Xray process; swap outbound tags via `xray api rmo` / `ado`."""

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
            self._proc = subprocess.Popen(
                [self.bin_path, "run", "-c", self._cfg_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **hide_console_kwargs(),
            )
        except OSError:
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
        self._cleanup_cfg()

    def set_slot_outbound(self, index: int, outbound: dict[str, Any] | None) -> None:
        if index < 0 or index >= len(self.slots):
            raise IndexError(f"slot index out of range: {index}")
        tag = self.slots[index].tag
        payload = blackhole_outbound(tag) if outbound is None else dict(outbound)
        payload["tag"] = tag
        self._replace_outbound(tag, payload)

    def set_outbounds(
        self, outbounds: list[dict[str, Any] | None]
    ) -> list[str | None]:
        """Replace slots; return per-slot error (None = ok). Bad slots become blackhole."""
        if len(outbounds) > len(self.slots):
            raise ValueError(
                f"too many outbounds ({len(outbounds)}) for {len(self.slots)} slots"
            )
        errors: list[str | None] = [None] * len(self.slots)
        for index in range(len(self.slots)):
            outbound = outbounds[index] if index < len(outbounds) else None
            try:
                self.set_slot_outbound(index, outbound)
            except Exception as exc:
                detail = str(exc) or type(exc).__name__
                errors[index] = detail
                try:
                    self.set_slot_outbound(index, None)
                except Exception:
                    pass
        return errors

    def _replace_outbound(self, tag: str, outbound: dict[str, Any]) -> None:
        if not self.running:
            raise RuntimeError("Xray live session is not running")
        last_error = ""
        for attempt in range(4):
            self._api_rmo(tag)
            if attempt:
                time.sleep(0.05 * attempt)
            err = self._api_ado(outbound)
            if err is None:
                return
            last_error = err
            time.sleep(0.08 * (attempt + 1))
        # Keep a placeholder outbound so routing still has a valid tag.
        self._api_rmo(tag)
        self._api_ado(blackhole_outbound(tag))
        raise RuntimeError(f"failed to set outbound {tag!r}: {last_error}")

    def _api_rmo(self, tag: str) -> None:
        # Removal of a missing tag is fine (first swap / already cleared).
        self._run_api(["rmo", f"--server=127.0.0.1:{self.api_port}", tag])

    def _api_ado(self, outbound: dict[str, Any]) -> str | None:
        path = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                suffix=".json",
                delete=False,
                prefix=f"{self.prefix}-ado-",
            ) as handle:
                # CLI expects a config fragment: {"outbounds": [ ... ]}
                json.dump({"outbounds": [outbound]}, handle, ensure_ascii=False)
                path = handle.name
            code, stdout, stderr = self._run_api(
                ["ado", f"--server=127.0.0.1:{self.api_port}", path]
            )
            if code == 0:
                return None
            detail = (stderr or stdout or "").strip()
            return detail or f"ado failed (code {code})"
        finally:
            if path:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    def _run_api(self, args: list[str]) -> tuple[int, str, str]:
        try:
            result = subprocess.run(
                [self.bin_path, "api", *args],
                capture_output=True,
                text=True,
                timeout=5.0,
                **hide_console_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return 1, "", str(exc)
        return (
            int(result.returncode),
            result.stdout or "",
            result.stderr or "",
        )

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
