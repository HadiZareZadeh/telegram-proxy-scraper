"""Proxy pool keep-alive: respawn Xray after it dies."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from fetch_mtproto.v2ray.proxy_pool import (
    ProxyPoolRunner,
    _looks_like_control_error,
    xray_restart_delay,
)


class ProxyPoolKeepaliveTests(unittest.TestCase):
    def test_restart_delay_backoff(self) -> None:
        self.assertEqual(xray_restart_delay(0), 2.0)
        self.assertEqual(xray_restart_delay(1), 4.0)
        self.assertEqual(xray_restart_delay(2), 8.0)
        self.assertEqual(xray_restart_delay(10), 30.0)

    def test_control_error_detection(self) -> None:
        self.assertTrue(_looks_like_control_error(ConnectionError("unavailable")))
        self.assertTrue(_looks_like_control_error(RuntimeError("StatusCode.UNAVAILABLE")))
        self.assertFalse(_looks_like_control_error(ValueError("bad catalog row")))

    def test_dead_process_needs_restart_until_stop(self) -> None:
        runner = ProxyPoolRunner(start_port=16001, count=1)
        self.assertTrue(runner._xray_needs_restart())

        alive = MagicMock()
        alive.poll.return_value = None
        runner._pool_process = alive
        self.assertFalse(runner._xray_needs_restart())

        alive.poll.return_value = 1
        self.assertTrue(runner._xray_needs_restart())

        runner._stop_event.set()
        self.assertFalse(runner._xray_needs_restart())
