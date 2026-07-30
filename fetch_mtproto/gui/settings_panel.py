"""Tabbed settings panel for all config.yaml options."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from fetch_mtproto.config_loader import config_bool, config_float, config_int
from fetch_mtproto.gui.config_fields import CONFIG_TABS, ConfigField

if TYPE_CHECKING:
    from fetch_mtproto.gui.app import App


class ConfigSettingsPanel:
    """All config.yaml fields grouped in a nested tabbed panel."""

    def __init__(self, app: App) -> None:
        self.app = app
        self.vars: dict[tuple[str, str], tk.Variable] = {}
        self.list_widgets: dict[tuple[str, str], tk.Text] = {}
        self._pool_input_widgets: list[tk.Widget] = []
        self._init_vars()

    def _key(self, field: ConfigField) -> tuple[str, str]:
        return (field.section, field.key)

    def var(self, section: str, key: str) -> tk.Variable:
        return self.vars[(section, key)]

    def _init_vars(self) -> None:
        for _title, fields in CONFIG_TABS:
            for field in fields:
                key = self._key(field)
                if field.kind == "bool":
                    self.vars[key] = tk.BooleanVar(value=bool(field.default))
                elif field.kind == "int":
                    self.vars[key] = tk.IntVar(value=int(field.default or 0))
                elif field.kind == "float":
                    self.vars[key] = tk.DoubleVar(value=float(field.default or 0.0))
                else:
                    self.vars[key] = tk.StringVar(
                        value="" if field.default is None else str(field.default)
                    )

    def build(self, parent: ttk.Frame) -> None:
        ttk.Label(
            parent,
            text="All settings are saved automatically to config.yaml.",
        ).pack(anchor="w", pady=(0, 8))

        notebook = ttk.Notebook(parent)
        notebook.pack(fill="both", expand=True)

        for title, fields in CONFIG_TABS:
            tab = ttk.Frame(notebook, padding=4)
            notebook.add(tab, text=title)
            inner = self._scrollable(tab)
            self._build_section(inner, fields)

    def _scrollable(self, parent: ttk.Frame) -> ttk.Frame:
        canvas = tk.Canvas(parent, highlightthickness=0, borderwidth=0)
        scroll = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind(
            "<Configure>",
            lambda _e, c=canvas: c.configure(scrollregion=c.bbox("all")),
        )
        window = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_canvas_configure(event, c=canvas, item=window) -> None:
            c.itemconfigure(item, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)

        def _on_mousewheel(event, c=canvas) -> None:
            if event.delta:
                c.yview_scroll(int(-event.delta / 120), "units")
            elif event.num == 4:
                c.yview_scroll(-3, "units")
            elif event.num == 5:
                c.yview_scroll(3, "units")

        canvas.bind("<MouseWheel>", _on_mousewheel)
        canvas.bind("<Button-4>", _on_mousewheel)
        canvas.bind("<Button-5>", _on_mousewheel)
        inner.bind("<MouseWheel>", _on_mousewheel)
        inner.bind("<Button-4>", _on_mousewheel)
        inner.bind("<Button-5>", _on_mousewheel)

        canvas.configure(yscrollcommand=scroll.set)
        canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        return inner

    def _build_section(self, parent: ttk.Frame, fields: tuple[ConfigField, ...]) -> None:
        display_row = 0
        for field in fields:
            ttk.Label(parent, text=field.label).grid(
                row=display_row, column=0, sticky="nw", padx=(0, 10), pady=(6, 2)
            )
            widget = self._build_field(parent, field)
            widget.grid(row=display_row, column=1, sticky="ew", pady=(6, 2))
            display_row += 1
            if field.hint:
                ttk.Label(parent, text=field.hint, foreground="gray", wraplength=520).grid(
                    row=display_row, column=1, sticky="w", pady=(0, 6)
                )
                display_row += 1
            if field.section == "proxy_pool" and field.kind != "bool":
                self._pool_input_widgets.append(widget)
        parent.columnconfigure(1, weight=1)

    def _build_field(self, parent: ttk.Frame, field: ConfigField) -> tk.Widget:
        key = self._key(field)
        variable = self.vars[key]

        if field.kind == "bool":
            widget = ttk.Checkbutton(parent, variable=variable)
            self.app._watch_config_var(variable)
            return widget

        if field.kind == "list":
            text = tk.Text(parent, height=8, width=48, wrap="none", font=("Consolas", 9))
            text.bind("<KeyRelease>", lambda _e: self.app._schedule_save_ui_config())
            self.list_widgets[key] = text
            self.app._attach_entry_menu(text)
            return text

        if field.kind == "int":
            widget = ttk.Spinbox(
                parent,
                textvariable=variable,
                from_=int(field.minimum or 0),
                to=int(field.maximum or 999999),
                width=min(12, max(6, field.width // 4)),
            )
        elif field.kind == "float":
            widget = ttk.Entry(parent, textvariable=variable, width=min(16, field.width // 3))
        else:
            widget = ttk.Entry(parent, textvariable=variable, width=min(48, field.width))

        self.app._watch_config_var(variable)
        self.app._attach_entry_menu(widget)
        return widget

    def load_from_config(self, config: SimpleNamespace | None) -> None:
        if config is None:
            return
        from fetch_mtproto.config_loader import _FIELD_MAP

        attr_map = {(sec, key): attr for sec, key, attr in _FIELD_MAP}

        for _title, fields in CONFIG_TABS:
            for field in fields:
                key = self._key(field)
                attr = attr_map.get(key)
                raw = getattr(config, attr, None) if attr else None
                if field.kind == "list":
                    text = self.list_widgets.get(key)
                    if text is None:
                        continue
                    text.delete("1.0", "end")
                    items = raw if isinstance(raw, list) else []
                    text.insert("1.0", "\n".join(str(item) for item in items))
                    continue

                variable = self.vars[key]
                if field.kind == "bool":
                    variable.set(config_bool(raw, bool(field.default)))
                elif field.kind == "int":
                    variable.set(config_int(raw, int(field.default or 0)))
                elif field.kind == "float":
                    variable.set(config_float(raw, float(field.default or 0.0)))
                elif field.kind == "nullable_int":
                    variable.set("" if raw is None else str(int(raw)))
                elif field.kind == "nullable_str":
                    variable.set("" if raw is None else str(raw))
                else:
                    variable.set("" if raw is None else str(raw))

    def read_values(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, list[str]]]]:
        scalars: dict[str, dict[str, Any]] = {}
        lists: dict[str, dict[str, list[str]]] = {}

        for _title, fields in CONFIG_TABS:
            for field in fields:
                key = self._key(field)
                section = field.section
                if field.kind == "list":
                    text = self.list_widgets.get(key)
                    items: list[str] = []
                    if text is not None:
                        items = [
                            line.strip()
                            for line in text.get("1.0", "end").splitlines()
                            if line.strip()
                        ]
                    lists.setdefault(section, {})[field.key] = items
                    continue

                variable = self.vars[key]
                try:
                    raw = variable.get()
                except tk.TclError:
                    raw = field.default

                if field.kind == "bool":
                    value: Any = bool(raw)
                elif field.kind == "int":
                    value = config_int(raw, int(field.default or 0))
                    if field.minimum is not None:
                        value = max(int(field.minimum), value)
                    if field.maximum is not None:
                        value = min(int(field.maximum), value)
                elif field.kind == "float":
                    value = config_float(raw, float(field.default or 0.0))
                    if field.minimum is not None:
                        value = max(float(field.minimum), value)
                    if field.maximum is not None:
                        value = min(float(field.maximum), value)
                elif field.kind == "nullable_int":
                    text = str(raw).strip()
                    value = None if not text else config_int(text, int(field.default or 0))
                elif field.kind == "nullable_str":
                    text = str(raw).strip()
                    value = None if not text else text
                else:
                    value = str(raw)

                scalars.setdefault(section, {})[field.key] = value

        return scalars, lists

    def set_pool_inputs_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for widget in self._pool_input_widgets:
            try:
                widget.configure(state=state)
            except tk.TclError:
                pass
