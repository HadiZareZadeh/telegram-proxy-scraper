"""Port-range math for pool and probe layouts."""

from __future__ import annotations

import unittest

from fetch_mtproto.v2ray.pool_ports import (
    DEFAULT_HTTP_START_PORT,
    DEFAULT_SOCKS_START_PORT,
    balancer_tag,
    clamp_ping_concurrency,
    clamp_pool_count,
    last_pool_ports,
    node_outbound_tag,
    ping_socks_ports,
    pool_listen_ports,
    slot_ports,
)
from fetch_mtproto.v2ray.win_ports import colliding_ports, DynamicPortRange, ExcludedPortRange


class PortTests(unittest.TestCase):
    def test_split_socks_http_ranges(self) -> None:
        socks, http = slot_ports(DEFAULT_SOCKS_START_PORT, DEFAULT_HTTP_START_PORT, 0)
        self.assertEqual(socks, 10801)
        self.assertEqual(http, 11201)
        last_s, last_h = last_pool_ports(10801, 11201, 300)
        self.assertEqual(last_s, 11100)
        self.assertEqual(last_h, 11500)

    def test_pool_listen_ports_include_api(self) -> None:
        ports = pool_listen_ports(
            socks_start=10801, http_start=11201, count=2, api_port=20802
        )
        self.assertEqual(ports, [10801, 11201, 10802, 11202, 20802])

    def test_clamp_limits(self) -> None:
        self.assertEqual(clamp_pool_count(0), 1)
        self.assertEqual(clamp_pool_count(5000), 1000)
        self.assertEqual(clamp_ping_concurrency(64), 64)
        self.assertEqual(clamp_ping_concurrency(4096), 1024)

    def test_balancer_and_node_tags(self) -> None:
        self.assertEqual(balancer_tag(7), "bal-slot-007")
        self.assertTrue(node_outbound_tag("vless://x").startswith("n-"))
        self.assertEqual(node_outbound_tag("a"), node_outbound_tag("a"))
        self.assertNotEqual(node_outbound_tag("a"), node_outbound_tag("b"))

    def test_ping_socks_ports(self) -> None:
        self.assertEqual(ping_socks_ports(45001, 3), [45001, 45002, 45003])

    def test_excluded_collision_detected(self) -> None:
        issues = colliding_ports(
            [10801, 10802],
            dynamic=DynamicPortRange(start=49152, count=16384),
            excluded=[ExcludedPortRange(start=10800, end=10810)],
        )
        self.assertTrue(any("fatal:" in item for item in issues))

    def test_no_collision_in_safe_range(self) -> None:
        issues = colliding_ports(
            [10801, 11201, 20802],
            dynamic=DynamicPortRange(start=49152, count=16384),
            excluded=[],
        )
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main()
