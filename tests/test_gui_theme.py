"""GUI theme palettes and name normalization."""

from __future__ import annotations

import unittest

from fetch_mtproto.gui.theme import PALETTES, normalize_theme, palette_for


class ThemeTests(unittest.TestCase):
    def test_normalize_theme(self) -> None:
        self.assertEqual(normalize_theme("Light"), "light")
        self.assertEqual(normalize_theme("DARK"), "dark")
        self.assertEqual(normalize_theme("nope"), "dark")
        self.assertEqual(normalize_theme(None), "dark")

    def test_palettes_cover_both_modes(self) -> None:
        self.assertEqual(set(PALETTES), {"light", "dark"})
        light = palette_for("light")
        dark = palette_for("dark")
        self.assertNotEqual(light.bg, dark.bg)
        self.assertNotEqual(light.fg, dark.fg)
        self.assertTrue(light.bg.startswith("#"))
        self.assertTrue(dark.input_bg.startswith("#"))


if __name__ == "__main__":
    unittest.main()
