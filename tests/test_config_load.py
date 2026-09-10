"""Config loader defaults for the redesigned pool/catalog."""

from __future__ import annotations

import unittest

from fetch_mtproto.config_loader import _EXAMPLE_PATH, _parse_config


class ConfigLoadTests(unittest.TestCase):
    def test_example_yaml_target_numbers(self) -> None:
        ns = _parse_config(_EXAMPLE_PATH)
        self.assertEqual(ns.PROXY_POOL_COUNT, 300)
        self.assertEqual(ns.PROXY_POOL_START_PORT, 10801)
        self.assertEqual(ns.PROXY_POOL_HTTP_START_PORT, 11201)
        self.assertEqual(ns.PROXY_POOL_XRAY_API_PORT, 20802)
        self.assertEqual(ns.V2RAY_PING_CONCURRENCY, 256)
        self.assertEqual(ns.V2RAY_TCP_CONCURRENCY, 500)
        self.assertEqual(ns.V2RAY_CATALOG_MAX, 10000)
        self.assertEqual(ns.V2RAY_SUBSCRIPTION_LIMIT, 100)
        self.assertEqual(ns.HOT_SET_REFRESH_SEC, 10)
        self.assertEqual(ns.PROXY_POOL_DIVERSITY_ROTATE_SEC, 0)
        self.assertFalse(ns.PROXY_POOL_ROTATE_EXISTING_CONNECTIONS)
        self.assertEqual(ns.PROXY_POOL_STANDBY_OUTBOUNDS, 300)
        self.assertEqual(ns.PROXY_POOL_RESERVE_OUTBOUNDS, 100)

    def test_example_yaml_gui_theme(self) -> None:
        ns = _parse_config(_EXAMPLE_PATH)
        self.assertEqual(ns.GUI_THEME, "dark")


if __name__ == "__main__":
    unittest.main()
