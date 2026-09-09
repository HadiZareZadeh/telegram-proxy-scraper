"""Integration: pool Xray survives routing rotations without process restart.

Uses a reduced slot count so CI / local runs stay light. Skips if xray is missing.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree
from fetch_mtproto.v2ray.ping import resolve_xray_bin
from fetch_mtproto.v2ray.pool_ports import balancer_tag, fallback_outbound_tag
from fetch_mtproto.v2ray.xray import blackhole_json, build_xray_slot_balancer_config
from fetch_mtproto.v2ray.xray_control import (
    HandlerClient,
    RoutingClient,
    XrayControlChannel,
)


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


class XrayRotationTests(unittest.TestCase):
    def test_fallback_never_direct(self) -> None:
        cfg = build_xray_slot_balancer_config(
            slot_count=2,
            socks_start=18001,
            http_start=18101,
            api_port=18099,
            hot_outbounds=[],
            fallback=blackhole_json(fallback_outbound_tag()),
        )
        self.assertIn("RoutingService", cfg["api"]["services"])
        for balancer in cfg["routing"]["balancers"]:
            self.assertEqual(balancer["fallbackTag"], "fallback")
            self.assertNotEqual(balancer["fallbackTag"], "direct")
        tags = {ob["tag"] for ob in cfg["outbounds"]}
        self.assertIn("fallback", tags)
        fallback = next(ob for ob in cfg["outbounds"] if ob["tag"] == "fallback")
        self.assertEqual(fallback["protocol"], "blackhole")
        self.assertIn("burstObservatory", cfg)

    def test_rotations_keep_same_pid(self) -> None:
        bin_path = resolve_xray_bin()
        if not bin_path:
            self.skipTest("xray binary not found")

        socks = 25001
        http = 25101
        api = 25099
        n_slots = 4
        n_rotations = 20
        cfg = build_xray_slot_balancer_config(
            slot_count=n_slots,
            socks_start=socks,
            http_start=http,
            api_port=api,
            hot_outbounds=[
                blackhole_json("n-aaaa"),
                blackhole_json("n-bbbb"),
            ],
            fallback=blackhole_json(fallback_outbound_tag()),
        )
        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, prefix="xray-rot-"
        )
        tmp.write(json.dumps(cfg))
        tmp.close()
        proc = subprocess.Popen(
            [bin_path, "run", "-c", tmp.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **hide_console_kwargs(),
        )
        try:
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline and not _port_open(api):
                if proc.poll() is not None:
                    self.fail(f"xray exited early: {proc.returncode}")
                time.sleep(0.05)
            self.assertTrue(_port_open(api), "API port did not open")
            pid = proc.pid

            async def _rotate() -> None:
                control = XrayControlChannel(port=api)
                await control.connect()
                routing = RoutingClient(control)
                handler = HandlerClient(control)
                try:
                    await handler.add_outbound(
                        blackhole_json("n-cccc"), tag="n-cccc"
                    )
                except Exception:
                    pass
                for _ in range(n_rotations):
                    assignments = {
                        balancer_tag(i): "n-aaaa" if i % 2 == 0 else "n-bbbb"
                        for i in range(n_slots)
                    }
                    errors = await routing.override_many(assignments)
                    self.assertTrue(all(err is None for err in errors), errors)
                await control.close()

            asyncio.run(_rotate())
            self.assertIsNone(proc.poll())
            self.assertEqual(proc.pid, pid)
        finally:
            if proc.poll() is None:
                kill_process_tree(proc)
            try:
                Path(tmp.name).unlink(missing_ok=True)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
