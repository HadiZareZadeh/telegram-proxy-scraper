"""JSON outbound → HandlerService protobuf."""

from __future__ import annotations

import unittest

from fetch_mtproto.v2ray.xray_control.outbound_pb import json_outbound_to_handler_config


class OutboundPbTests(unittest.TestCase):
    def test_vless_tcp_builds(self) -> None:
        outbound = {
            "protocol": "vless",
            "tag": "n-test",
            "settings": {
                "vnext": [
                    {
                        "address": "example.com",
                        "port": 443,
                        "users": [{"id": "11111111-1111-1111-1111-111111111111", "encryption": "none"}],
                    }
                ]
            },
            "streamSettings": {"network": "tcp", "security": "none"},
        }
        cfg = json_outbound_to_handler_config(outbound, tag="n-test")
        self.assertEqual(cfg.tag, "n-test")
        self.assertTrue(cfg.proxy_settings.type)

    def test_blackhole_builds(self) -> None:
        cfg = json_outbound_to_handler_config(
            {"protocol": "blackhole", "tag": "fallback", "settings": {}},
            tag="fallback",
        )
        self.assertEqual(cfg.tag, "fallback")


if __name__ == "__main__":
    unittest.main()
