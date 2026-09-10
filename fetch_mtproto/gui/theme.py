"""Light and dark palettes applied to ttk styles and classic Tk widgets."""

from __future__ import annotations

import tkinter as tk
from dataclasses import dataclass
from tkinter import ttk
from typing import Mapping

THEMES = ("light", "dark")


@dataclass(frozen=True, slots=True)
class Palette:
    name: str
    bg: str
    surface: str
    surface2: str
    fg: str
    muted: str
    accent: str
    button: str
    button_hover: str
    input_bg: str
    select_bg: str
    select_fg: str
    border: str
    tree_alt: str
    trough: str


LIGHT = Palette(
    name="light",
    bg="#f3f3f3",
    surface="#ffffff",
    surface2="#ececec",
    fg="#1a1a1a",
    muted="#5c5c5c",
    accent="#0078d4",
    button="#e5e5e5",
    button_hover="#d8d8d8",
    input_bg="#ffffff",
    select_bg="#cce4f7",
    select_fg="#1a1a1a",
    border="#c8c8c8",
    tree_alt="#f6f6f6",
    trough="#e0e0e0",
)

DARK = Palette(
    name="dark",
    bg="#1e1e1e",
    surface="#252526",
    surface2="#2d2d30",
    fg="#e8e8e8",
    muted="#9a9a9a",
    accent="#0e7ec8",
    button="#3c3c3c",
    button_hover="#4a4a4a",
    input_bg="#1b1b1b",
    select_bg="#264f78",
    select_fg="#ffffff",
    border="#3f3f46",
    tree_alt="#2a2a2c",
    trough="#2b2b2b",
)

PALETTES: Mapping[str, Palette] = {"light": LIGHT, "dark": DARK}

_CLASSIC_BG_WIDGETS = (
    tk.Tk,
    tk.Toplevel,
    tk.Frame,
    tk.LabelFrame,
    tk.PanedWindow,
    tk.Canvas,
)
_CLASSIC_TEXT_WIDGETS = (tk.Text, tk.Entry, tk.Spinbox, tk.Listbox)
_CLASSIC_SCROLLBAR = tk.Scrollbar
_CLASSIC_LABEL = tk.Label
_CLASSIC_BUTTON = tk.Button
_CLASSIC_CHECK = (tk.Checkbutton, tk.Radiobutton)
_CLASSIC_SCALE = tk.Scale


def normalize_theme(value: object, default: str = "dark") -> str:
    name = str(value or "").strip().lower()
    if name in PALETTES:
        return name
    return default


def palette_for(name: object) -> Palette:
    return PALETTES[normalize_theme(name)]


def apply_theme(root: tk.Misc, name: object) -> Palette:
    """Restyle ttk + walk classic Tk widgets so the whole window matches."""
    palette = palette_for(name)
    _apply_ttk(root, palette)
    _apply_root_options(root, palette)
    _walk(root, palette)
    return palette


def style_menu(menu: tk.Menu, palette: Palette) -> None:
    try:
        menu.configure(
            background=palette.surface,
            foreground=palette.fg,
            activebackground=palette.select_bg,
            activeforeground=palette.select_fg,
            disabledforeground=palette.muted,
            selectcolor=palette.fg,
            borderwidth=1,
            relief="flat",
        )
    except tk.TclError:
        pass


def style_classic_widget(widget: tk.Misc, palette: Palette) -> None:
    _apply_classic(widget, palette)


def _apply_root_options(root: tk.Misc, p: Palette) -> None:
    try:
        root.configure(background=p.bg)
    except tk.TclError:
        pass
    try:
        root.tk_setPalette(
            background=p.bg,
            foreground=p.fg,
            activeBackground=p.select_bg,
            activeForeground=p.select_fg,
            highlightBackground=p.bg,
            highlightColor=p.accent,
            insertBackground=p.fg,
            selectBackground=p.select_bg,
            selectForeground=p.select_fg,
            troughColor=p.trough,
        )
    except tk.TclError:
        pass
    add = root.option_add
    add("*TCombobox*Listbox.background", p.input_bg)
    add("*TCombobox*Listbox.foreground", p.fg)
    add("*TCombobox*Listbox.selectBackground", p.select_bg)
    add("*TCombobox*Listbox.selectForeground", p.select_fg)
    add("*Menu.background", p.surface)
    add("*Menu.foreground", p.fg)
    add("*Menu.activeBackground", p.select_bg)
    add("*Menu.activeForeground", p.select_fg)
    add("*Menu.disabledForeground", p.muted)


def _apply_ttk(root: tk.Misc, p: Palette) -> None:
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    style.configure(
        ".",
        background=p.bg,
        foreground=p.fg,
        fieldbackground=p.input_bg,
        bordercolor=p.border,
        darkcolor=p.border,
        lightcolor=p.border,
        troughcolor=p.trough,
        focuscolor=p.accent,
        selectbackground=p.select_bg,
        selectforeground=p.select_fg,
        insertcolor=p.fg,
        arrowcolor=p.fg,
        indicatorcolor=p.input_bg,
    )
    style.configure("TFrame", background=p.bg)
    style.configure("TLabelframe", background=p.bg, foreground=p.fg, bordercolor=p.border)
    style.configure("TLabelframe.Label", background=p.bg, foreground=p.fg)
    style.configure("TLabel", background=p.bg, foreground=p.fg)
    style.configure("Muted.TLabel", background=p.bg, foreground=p.muted)
    style.configure(
        "TButton",
        background=p.button,
        foreground=p.fg,
        bordercolor=p.border,
        lightcolor=p.button,
        darkcolor=p.button,
        focusthickness=1,
        padding=(10, 4),
    )
    style.map(
        "TButton",
        background=[("active", p.button_hover), ("pressed", p.accent), ("disabled", p.surface2)],
        foreground=[("disabled", p.muted)],
        bordercolor=[("focus", p.accent)],
    )
    style.configure("TCheckbutton", background=p.bg, foreground=p.fg, indicatorcolor=p.input_bg)
    style.map(
        "TCheckbutton",
        background=[("active", p.bg)],
        foreground=[("disabled", p.muted)],
        indicatorcolor=[("selected", p.accent), ("pressed", p.accent)],
    )
    style.configure("TRadiobutton", background=p.bg, foreground=p.fg, indicatorcolor=p.input_bg)
    style.map(
        "TRadiobutton",
        background=[("active", p.bg)],
        indicatorcolor=[("selected", p.accent)],
    )
    style.configure(
        "TEntry",
        fieldbackground=p.input_bg,
        foreground=p.fg,
        insertcolor=p.fg,
        bordercolor=p.border,
        lightcolor=p.border,
        darkcolor=p.border,
        padding=3,
    )
    style.map(
        "TEntry",
        fieldbackground=[("disabled", p.surface2), ("readonly", p.surface)],
        foreground=[("disabled", p.muted)],
        bordercolor=[("focus", p.accent)],
    )
    style.configure(
        "TSpinbox",
        fieldbackground=p.input_bg,
        foreground=p.fg,
        insertcolor=p.fg,
        bordercolor=p.border,
        arrowcolor=p.fg,
        padding=2,
    )
    style.map(
        "TSpinbox",
        fieldbackground=[("disabled", p.surface2)],
        foreground=[("disabled", p.muted)],
        arrowcolor=[("disabled", p.muted)],
    )
    style.configure(
        "TCombobox",
        fieldbackground=p.input_bg,
        foreground=p.fg,
        background=p.button,
        arrowcolor=p.fg,
        bordercolor=p.border,
        padding=3,
    )
    style.map(
        "TCombobox",
        fieldbackground=[("readonly", p.input_bg), ("disabled", p.surface2)],
        foreground=[("disabled", p.muted)],
        arrowcolor=[("disabled", p.muted)],
    )
    style.configure(
        "TNotebook",
        background=p.bg,
        bordercolor=p.border,
        tabmargins=(4, 4, 4, 0),
    )
    style.configure(
        "TNotebook.Tab",
        background=p.surface2,
        foreground=p.fg,
        bordercolor=p.border,
        lightcolor=p.surface2,
        padding=(12, 6),
    )
    style.map(
        "TNotebook.Tab",
        background=[("selected", p.surface), ("active", p.button_hover)],
        foreground=[("selected", p.fg), ("disabled", p.muted)],
        lightcolor=[("selected", p.surface)],
    )
    style.configure(
        "Treeview",
        background=p.input_bg,
        fieldbackground=p.input_bg,
        foreground=p.fg,
        bordercolor=p.border,
        rowheight=22,
    )
    style.configure(
        "Treeview.Heading",
        background=p.surface2,
        foreground=p.fg,
        bordercolor=p.border,
        relief="flat",
    )
    style.map(
        "Treeview",
        background=[("selected", p.select_bg)],
        foreground=[("selected", p.select_fg)],
    )
    style.map(
        "Treeview.Heading",
        background=[("active", p.button_hover)],
    )
    style.configure(
        "Vertical.TScrollbar",
        background=p.button,
        troughcolor=p.trough,
        bordercolor=p.border,
        arrowcolor=p.fg,
        lightcolor=p.button,
        darkcolor=p.button,
    )
    style.configure(
        "Horizontal.TScrollbar",
        background=p.button,
        troughcolor=p.trough,
        bordercolor=p.border,
        arrowcolor=p.fg,
        lightcolor=p.button,
        darkcolor=p.button,
    )
    style.map(
        "Vertical.TScrollbar",
        background=[("active", p.button_hover)],
        arrowcolor=[("disabled", p.muted)],
    )
    style.map(
        "Horizontal.TScrollbar",
        background=[("active", p.button_hover)],
        arrowcolor=[("disabled", p.muted)],
    )
    style.configure("TPanedwindow", background=p.bg)
    style.configure("Sash", sashthickness=6, gripsize=0)
    style.configure("TSeparator", background=p.border)
    style.configure("TProgressbar", background=p.accent, troughcolor=p.trough, bordercolor=p.border)
    style.configure("TSizegrip", background=p.bg)


def _walk(widget: tk.Misc, p: Palette) -> None:
    _apply_classic(widget, p)
    try:
        children = widget.winfo_children()
    except tk.TclError:
        return
    for child in children:
        _walk(child, p)


def _apply_classic(widget: tk.Misc, p: Palette) -> None:
    if isinstance(widget, ttk.Widget):
        return
    try:
        if isinstance(widget, _CLASSIC_BG_WIDGETS):
            widget.configure(background=p.bg, highlightbackground=p.bg, highlightcolor=p.accent)
            if isinstance(widget, tk.Canvas):
                widget.configure(insertbackground=p.fg)
        elif isinstance(widget, _CLASSIC_TEXT_WIDGETS):
            widget.configure(
                background=p.input_bg,
                foreground=p.fg,
                insertbackground=p.fg,
                selectbackground=p.select_bg,
                selectforeground=p.select_fg,
                highlightbackground=p.border,
                highlightcolor=p.accent,
            )
        elif isinstance(widget, _CLASSIC_SCROLLBAR):
            widget.configure(
                background=p.button,
                troughcolor=p.trough,
                activebackground=p.button_hover,
                highlightbackground=p.bg,
            )
        elif isinstance(widget, _CLASSIC_LABEL):
            widget.configure(background=p.bg, foreground=p.fg)
        elif isinstance(widget, _CLASSIC_BUTTON):
            widget.configure(
                background=p.button,
                foreground=p.fg,
                activebackground=p.button_hover,
                activeforeground=p.fg,
                highlightbackground=p.border,
            )
        elif isinstance(widget, _CLASSIC_CHECK):
            widget.configure(
                background=p.bg,
                foreground=p.fg,
                activebackground=p.bg,
                activeforeground=p.fg,
                selectcolor=p.input_bg,
                highlightbackground=p.bg,
            )
        elif isinstance(widget, _CLASSIC_SCALE):
            widget.configure(
                background=p.bg,
                foreground=p.fg,
                troughcolor=p.trough,
                highlightbackground=p.bg,
            )
        elif isinstance(widget, tk.Menu):
            style_menu(widget, p)
    except tk.TclError:
        pass
