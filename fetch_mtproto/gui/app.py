"""All-in-one Tkinter GUI: tabbed control panel for scrape, ping, subscription."""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk

from fetch_mtproto.cancel import CANCEL_ENV
from fetch_mtproto.paths import LOGS_DIR, PROJECT_ROOT
from fetch_mtproto.process_tree import hide_console_kwargs, kill_process_tree

TELEGRAM_EXE = os.path.join(
    os.environ.get("APPDATA", ""), "Telegram Desktop", "Telegram.exe"
)


@dataclass
class Job:
    """A subprocess task streamed into the GUI log."""

    name: str
    args: list[str]
    long_running: bool = False
    stoppable: bool = False
    process: subprocess.Popen | None = field(default=None, repr=False)
    cancel_file: Path | None = field(default=None, repr=False)

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None


class App:
    JOBS: dict[str, dict] = {
        "scrape": {
            "label": "Scraper",
            "module": "fetch_mtproto.cli.scrape",
            "long_running": True,
        },
        "ping_mtproto": {
            "label": "Ping MTProto",
            "module": "fetch_mtproto.cli.ping_mtproto",
            "long_running": False,
            "stoppable": True,
        },
        "ping_v2ray": {
            "label": "Ping V2Ray",
            "module": "fetch_mtproto.cli.ping_v2ray",
            "long_running": False,
            "stoppable": True,
        },
        "serve": {
            "label": "Subscription server",
            "module": "fetch_mtproto.cli.update_subscription",
            "long_running": True,
        },
    }

    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("fetch-mtproto control panel")
        self.root.geometry("980x640")
        self.root.minsize(760, 480)

        self.jobs: dict[str, Job] = {}
        self.buttons: dict[str, ttk.Button] = {}
        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.active_stdin: str | None = None
        self._subscription_urls: list[tuple[str, str]] = []
        self._qr_photo = None
        self.proxy_pool: "ProxyPoolRunner | None" = None

        self._build_ui()
        self.root.after(100, self._drain_log_queue)
        self.root.after(300, self.refresh_status)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------ UI

    def _build_ui(self) -> None:
        self._build_status_bar()
        self._build_notebook()
        self._build_log_area()
        self._build_input_bar()

    def _build_status_bar(self) -> None:
        bar = ttk.Frame(self.root, padding=(8, 8, 8, 0))
        bar.pack(fill="x")
        ttk.Button(bar, text="Refresh status", command=self.refresh_status).pack(
            side="right"
        )
        self.status_var = tk.StringVar(value="Status: loading…")
        status_label = ttk.Label(bar, textvariable=self.status_var)
        status_label.pack(side="right", padx=12)
        self._attach_status_menu(status_label)

    def _build_notebook(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=8, pady=4)
        self.notebook = notebook

        jobs_tab = ttk.Frame(notebook, padding=12)
        notebook.add(jobs_tab, text="Jobs")
        self._build_jobs_tab(jobs_tab)

        sub_tab = ttk.Frame(notebook, padding=12)
        notebook.add(sub_tab, text="Subscription")
        self.sub_tab = sub_tab
        self._build_subscription_tab(sub_tab)

        proxies_tab = ttk.Frame(notebook, padding=12)
        notebook.add(proxies_tab, text="Proxies")
        self._build_proxies_tab(proxies_tab)

        pool_tab = ttk.Frame(notebook, padding=12)
        notebook.add(pool_tab, text="Proxy pool")
        self.proxy_pool_tab = pool_tab
        self._build_proxy_pool_tab(pool_tab)

    def _build_jobs_tab(self, parent: ttk.Frame) -> None:
        ttk.Label(
            parent,
            text="Start or stop background tasks. Output appears in the log below.",
        ).pack(anchor="w", pady=(0, 12))

        job_keys = ("scrape", "ping_mtproto", "ping_v2ray")
        job_hints = {
            "scrape": "Collect MTProto / V2Ray proxies from Telegram channels.",
            "ping_mtproto": "Test MTProto servers and update the working catalog.",
            "ping_v2ray": "Test V2Ray servers and update the working catalog.",
        }
        for key in job_keys:
            spec = self.JOBS[key]
            row = ttk.Frame(parent)
            row.pack(fill="x", pady=6)
            btn = ttk.Button(
                row,
                text=self._job_button_start_text(spec),
                command=lambda k=key: self.toggle_job(k),
                width=24,
            )
            btn.pack(side="left")
            self.buttons[key] = btn
            ttk.Label(row, text=job_hints[key], wraplength=520).pack(
                side="left", padx=(12, 0)
            )

    def _build_subscription_tab(self, parent: ttk.Frame) -> None:
        serve_spec = self.JOBS["serve"]
        serve_row = ttk.Frame(parent)
        serve_row.pack(fill="x", pady=(0, 12))
        serve_btn = ttk.Button(
            serve_row,
            text=self._job_button_start_text(serve_spec),
            command=lambda: self.toggle_job("serve"),
            width=24,
        )
        serve_btn.pack(side="left")
        self.buttons["serve"] = serve_btn
        ttk.Label(
            serve_row,
            text="Serve subscription URLs for NekoRay / v2rayNG on the LAN.",
            wraplength=520,
        ).pack(side="left", padx=(12, 0))

        self.sub_frame = ttk.LabelFrame(
            parent, text="Subscription link", padding=8
        )
        self.sub_frame.pack(fill="both", expand=True)
        sub_content = ttk.Frame(self.sub_frame)
        sub_content.pack(fill="both", expand=True)

        sub_left = ttk.Frame(sub_content)
        sub_left.pack(side="left", fill="both", expand=True)
        self.sub_placeholder = ttk.Label(
            sub_left,
            text="Start the subscription server to see URLs and a QR code.",
            foreground="gray",
        )
        self.sub_placeholder.pack(anchor="w")
        self.sub_urls_box = tk.Text(
            sub_left,
            height=3,
            wrap="word",
            font=("Consolas", 9),
            state="disabled",
            relief="flat",
            borderwidth=0,
            highlightthickness=0,
        )
        ttk.Button(
            sub_left, text="Copy LAN URL", command=self._copy_subscription_url
        ).pack(anchor="w", pady=(6, 0))

        self.qr_label = ttk.Label(sub_content)
        self.qr_missing_label = ttk.Label(
            sub_content,
            text="Install qrcode[pil] to show QR",
            foreground="gray",
        )

    def _build_proxies_tab(self, parent: ttk.Frame) -> None:
        ttk.Label(
            parent,
            text="Open working MTProto proxies in Telegram Desktop or copy links.",
        ).pack(anchor="w", pady=(0, 12))

        row = ttk.Frame(parent)
        row.pack(fill="x", pady=6)
        ttk.Label(row, text="Open top").pack(side="left")
        self.top_count = tk.IntVar(value=10)
        self.top_spin = ttk.Spinbox(
            row,
            from_=1,
            to=50,
            width=4,
            textvariable=self.top_count,
        )
        self.top_spin.pack(side="left", padx=4)
        ttk.Button(
            row,
            text="proxies in Telegram",
            command=self.open_top_proxies,
        ).pack(side="left", padx=4)
        ttk.Button(
            row,
            text="copy 10 links",
            command=self.copy_top_proxies,
        ).pack(side="left", padx=4)
        self._attach_entry_menu(self.top_spin)

    def _build_proxy_pool_tab(self, parent: ttk.Frame) -> None:
        ttk.Label(
            parent,
            text=(
                "Run local SOCKS5 + HTTP proxy pairs (one upstream V2Ray server per slot). "
                "Each slot uses two ports: SOCKS5 then HTTP. No config files written."
            ),
            wraplength=720,
        ).pack(anchor="w", pady=(0, 12))

        form = ttk.Frame(parent)
        form.pack(fill="x", pady=(0, 8))

        ttk.Label(form, text="Start port").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.pool_start_port = tk.IntVar(value=10801)
        start_port_spin = ttk.Spinbox(
            form,
            from_=1024,
            to=65000,
            width=8,
            textvariable=self.pool_start_port,
        )
        start_port_spin.grid(row=0, column=1, sticky="w")
        self._attach_entry_menu(start_port_spin)

        ttk.Label(form, text="Proxy count").grid(
            row=0, column=2, sticky="w", padx=(24, 8)
        )
        self.pool_count = tk.IntVar(value=10)
        count_spin = ttk.Spinbox(
            form,
            from_=1,
            to=50,
            width=4,
            textvariable=self.pool_count,
        )
        count_spin.grid(row=0, column=3, sticky="w")
        self._attach_entry_menu(count_spin)

        ttk.Label(form, text="Switch servers every (sec)").grid(
            row=1, column=0, sticky="w", pady=(8, 0), padx=(0, 8)
        )
        self.pool_switch_sec = tk.IntVar(value=300)
        switch_spin = ttk.Spinbox(
            form,
            from_=30,
            to=86400,
            width=8,
            textvariable=self.pool_switch_sec,
        )
        switch_spin.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self._attach_entry_menu(switch_spin)

        btn_row = ttk.Frame(parent)
        btn_row.pack(fill="x", pady=(8, 12))
        self.pool_btn = ttk.Button(
            btn_row,
            text="Start proxy pool",
            command=self.toggle_proxy_pool,
            width=24,
        )
        self.pool_btn.pack(side="left")
        self.pool_pool_inputs = (start_port_spin, count_spin, switch_spin)

        status_frame = ttk.LabelFrame(parent, text="Active proxies", padding=8)
        status_frame.pack(fill="both", expand=True)
        columns = ("slot", "socks", "http", "upstream", "scheme", "latency", "status")
        self.pool_tree = ttk.Treeview(
            status_frame,
            columns=columns,
            show="headings",
            height=8,
        )
        headings = {
            "slot": "Slot",
            "socks": "SOCKS5",
            "http": "HTTP",
            "upstream": "Upstream",
            "scheme": "Scheme",
            "latency": "Latency",
            "status": "Status",
        }
        widths = {
            "slot": 40,
            "socks": 160,
            "http": 160,
            "upstream": 180,
            "scheme": 70,
            "latency": 70,
            "status": 120,
        }
        for col in columns:
            self.pool_tree.heading(col, text=headings[col])
            self.pool_tree.column(col, width=widths[col], stretch=col not in {"slot"})
        scroll = ttk.Scrollbar(
            status_frame, orient="vertical", command=self.pool_tree.yview
        )
        self.pool_tree.configure(yscrollcommand=scroll.set)
        self.pool_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

    def _build_log_area(self) -> None:
        self.log_wrap = tk.BooleanVar(value=True)
        self.log_autoscroll = tk.BooleanVar(value=True)
        self.log = scrolledtext.ScrolledText(
            self.root, wrap="word", state="disabled", font=("Consolas", 9), height=12
        )
        self.log.pack(fill="both", expand=True, padx=8, pady=(0, 4))
        self._attach_log_menu(self.log)

    def _build_input_bar(self) -> None:
        bottom = ttk.Frame(self.root, padding=(8, 0, 8, 8))
        bottom.pack(fill="x")
        ttk.Label(bottom, text="Input (login code / phone):").pack(side="left")
        self.stdin_entry = ttk.Entry(bottom)
        self.stdin_entry.pack(side="left", fill="x", expand=True, padx=6)
        self.stdin_entry.bind("<Return>", lambda _e: self.send_stdin())
        ttk.Button(bottom, text="Send", command=self.send_stdin).pack(side="left")
        self._attach_entry_menu(self.stdin_entry)

    # ------------------------------------------------------- context menus

    def _popup_menu(self, menu: tk.Menu, event: tk.Event) -> str:
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()
        return "break"

    def _clipboard_has_text(self) -> bool:
        try:
            return bool(self.root.clipboard_get())
        except tk.TclError:
            return False

    def _attach_entry_menu(self, widget: tk.Widget) -> None:
        """Cut / Copy / Paste / Delete / Select All / Clear for entry-like fields."""

        def cut() -> None:
            widget.event_generate("<<Cut>>")

        def copy() -> None:
            widget.event_generate("<<Copy>>")

        def paste() -> None:
            widget.event_generate("<<Paste>>")

        def delete_sel() -> None:
            widget.event_generate("<<Clear>>")

        def select_all() -> None:
            widget.selection_range(0, "end")
            widget.icursor("end")

        def clear_all() -> None:
            widget.delete(0, "end")

        def show(event: tk.Event) -> str:
            widget.focus_set()
            has_sel = widget.selection_present()
            has_text = bool(widget.get())
            menu = tk.Menu(widget, tearoff=0)
            menu.add_command(
                label="Cut", command=cut, state="normal" if has_sel else "disabled"
            )
            menu.add_command(
                label="Copy", command=copy, state="normal" if has_sel else "disabled"
            )
            menu.add_command(
                label="Paste",
                command=paste,
                state="normal" if self._clipboard_has_text() else "disabled",
            )
            menu.add_command(
                label="Delete",
                command=delete_sel,
                state="normal" if has_sel else "disabled",
            )
            menu.add_separator()
            menu.add_command(
                label="Select All",
                command=select_all,
                state="normal" if has_text else "disabled",
            )
            menu.add_command(
                label="Clear",
                command=clear_all,
                state="normal" if has_text else "disabled",
            )
            return self._popup_menu(menu, event)

        widget.bind("<Button-3>", show)

    def _attach_log_menu(self, widget: tk.Text) -> None:
        """Read-only log: copy/select helpers plus save, clear and view options."""

        def copy_selection() -> None:
            widget.event_generate("<<Copy>>")

        def select_all() -> None:
            widget.tag_add("sel", "1.0", "end-1c")
            widget.mark_set("insert", "end-1c")

        def line_range(index: str) -> tuple[str, str]:
            return f"{index} linestart", f"{index} lineend"

        def select_line(index: str) -> None:
            widget.tag_remove("sel", "1.0", "end")
            start, end = line_range(index)
            widget.tag_add("sel", start, end)

        def copy_line(index: str) -> None:
            start, end = line_range(index)
            text = widget.get(start, end)
            if text:
                self.root.clipboard_clear()
                self.root.clipboard_append(text)

        def copy_all() -> None:
            text = widget.get("1.0", "end-1c")
            if text:
                self.root.clipboard_clear()
                self.root.clipboard_append(text)

        def save_as() -> None:
            LOGS_DIR.mkdir(parents=True, exist_ok=True)
            path = filedialog.asksaveasfilename(
                title="Save log",
                defaultextension=".txt",
                initialdir=str(LOGS_DIR),
                initialfile=f"fetch-mtproto-log-{time.strftime('%Y%m%d-%H%M%S')}.txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(widget.get("1.0", "end-1c"))
                self.log_line(f"[log] saved to {path}")
            except OSError as exc:
                messagebox.showerror("fetch-mtproto", f"Failed to save log: {exc}")

        def clear_log() -> None:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.configure(state="disabled")

        def toggle_wrap() -> None:
            widget.configure(wrap="word" if self.log_wrap.get() else "none")

        def scroll_to_end() -> None:
            widget.see("end")

        def show(event: tk.Event) -> str:
            index = widget.index(f"@{event.x},{event.y}")
            has_sel = bool(widget.tag_ranges("sel"))
            has_text = bool(widget.get("1.0", "end-1c"))
            on_line = bool(widget.get(*line_range(index)).strip())
            menu = tk.Menu(widget, tearoff=0)
            menu.add_command(
                label="Copy",
                command=copy_selection,
                state="normal" if has_sel else "disabled",
            )
            menu.add_command(
                label="Copy Line",
                command=lambda: copy_line(index),
                state="normal" if on_line else "disabled",
            )
            menu.add_command(
                label="Copy All",
                command=copy_all,
                state="normal" if has_text else "disabled",
            )
            menu.add_separator()
            menu.add_command(
                label="Select Line",
                command=lambda: select_line(index),
                state="normal" if on_line else "disabled",
            )
            menu.add_command(
                label="Select All",
                command=select_all,
                state="normal" if has_text else "disabled",
            )
            menu.add_separator()
            menu.add_command(
                label="Save Log As…",
                command=save_as,
                state="normal" if has_text else "disabled",
            )
            menu.add_command(
                label="Clear Log",
                command=clear_log,
                state="normal" if has_text else "disabled",
            )
            menu.add_separator()
            menu.add_command(label="Scroll to End", command=scroll_to_end)
            menu.add_checkbutton(
                label="Word Wrap", variable=self.log_wrap, command=toggle_wrap
            )
            menu.add_checkbutton(
                label="Auto-scroll on New Output", variable=self.log_autoscroll
            )
            return self._popup_menu(menu, event)

        widget.bind("<Button-3>", show)
        # Ctrl+A doesn't work on a disabled Text widget by default.
        widget.bind("<Control-a>", lambda _e: (select_all(), "break")[1])

    def _attach_status_menu(self, widget: tk.Widget) -> None:
        def copy_status() -> None:
            self.root.clipboard_clear()
            self.root.clipboard_append(self.status_var.get())

        def show(event: tk.Event) -> str:
            menu = tk.Menu(widget, tearoff=0)
            menu.add_command(label="Copy Status", command=copy_status)
            menu.add_command(label="Refresh Status", command=self.refresh_status)
            return self._popup_menu(menu, event)

        widget.bind("<Button-3>", show)

    # ------------------------------------------------------------- logging

    def log_line(self, text: str) -> None:
        self.log_queue.put(text.rstrip("\n"))

    def _drain_log_queue(self) -> None:
        try:
            lines: list[str] = []
            while True:
                lines.append(self.log_queue.get_nowait())
        except queue.Empty:
            pass
        if lines:
            self.log.configure(state="normal")
            self.log.insert("end", "\n".join(lines) + "\n")
            if self.log_autoscroll.get():
                self.log.see("end")
            self.log.configure(state="disabled")
        self.root.after(100, self._drain_log_queue)

    # ---------------------------------------------------------------- jobs

    @staticmethod
    def _job_button_start_text(spec: dict) -> str:
        if spec["long_running"]:
            return f"Start {spec['label']}"
        return spec["label"]

    @staticmethod
    def _job_button_stop_text(spec: dict) -> str:
        return f"Stop {spec['label']}"

    def toggle_job(self, key: str) -> None:
        spec = self.JOBS[key]
        job = self.jobs.get(key)
        if job is not None and job.running:
            if spec["long_running"] or spec.get("stoppable"):
                self.stop_job(key)
            else:
                self.log_line(f"[{spec['label']}] already running…")
            return
        self.start_job(key)

    def start_job(self, key: str) -> None:
        spec = self.JOBS[key]
        args = [sys.executable, "-u", "-m", spec["module"]]
        if key == "serve":
            from fetch_mtproto.catalogs import subscription_path
            from fetch_mtproto.config_loader import load_config
            from fetch_mtproto.subscription_server import resolve_server_settings

            config = load_config(required=False)
            host, port = resolve_server_settings(config)
            args.extend(["--host", host, "--port", str(port)])
            filename = subscription_path(config).name
            self.root.after(
                0,
                lambda: self._show_subscription_panel(host, port, filename),
            )
        # Force the child to emit UTF-8 so Persian / non-ASCII logs render
        # correctly instead of falling back to \uXXXX escapes on Windows.
        child_env = dict(os.environ)
        child_env["PYTHONIOENCODING"] = "utf-8"
        child_env["PYTHONUTF8"] = "1"
        cancel_file: Path | None = None
        if spec.get("stoppable"):
            LOGS_DIR.mkdir(parents=True, exist_ok=True)
            cancel_file = LOGS_DIR / f".cancel-{key}.flag"
            cancel_file.unlink(missing_ok=True)
            child_env[CANCEL_ENV] = str(cancel_file)
        popen_kw: dict = {
            "cwd": str(PROJECT_ROOT),
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "env": child_env,
            **hide_console_kwargs(),
        }
        if sys.platform != "win32":
            # Isolate CLI jobs in their own process group so tree kill is safe.
            popen_kw["start_new_session"] = True
        try:
            process = subprocess.Popen(args, **popen_kw)
        except OSError as exc:
            messagebox.showerror("fetch-mtproto", f"Failed to start: {exc}")
            return

        job = Job(
            name=key,
            args=args,
            long_running=spec["long_running"],
            stoppable=bool(spec.get("stoppable")),
            process=process,
            cancel_file=cancel_file,
        )
        self.jobs[key] = job
        self.active_stdin = key
        self.log_line(f"[{spec['label']}] started (pid {process.pid})")
        if spec["long_running"] or spec.get("stoppable"):
            self.buttons[key].configure(text=self._job_button_stop_text(spec))

        threading.Thread(
            target=self._pump_output, args=(key,), daemon=True
        ).start()

    def _pump_output(self, key: str) -> None:
        spec = self.JOBS[key]
        job = self.jobs[key]
        assert job.process is not None and job.process.stdout is not None
        buffer = ""
        while True:
            ch = job.process.stdout.read(1)
            if ch == "":
                break
            if ch == "\n":
                self.log_line(f"[{spec['label']}] {buffer}")
                buffer = ""
                continue
            buffer += ch
            # Show input() prompts (they end with ': ' and have no newline)
            if buffer.endswith(": "):
                self.log_line(f"[{spec['label']}] {buffer}")
                buffer = ""
        if buffer:
            self.log_line(f"[{spec['label']}] {buffer}")
        code = job.process.wait()
        self.log_line(f"[{spec['label']}] exited with code {code}")
        self.root.after(0, self._job_finished, key)

    def _job_finished(self, key: str) -> None:
        spec = self.JOBS[key]
        job = self.jobs.get(key)
        if job is not None and job.cancel_file is not None:
            try:
                job.cancel_file.unlink(missing_ok=True)
            except OSError:
                pass
        if key == "serve":
            self._hide_subscription_panel()
        if spec["long_running"] or spec.get("stoppable"):
            self.buttons[key].configure(text=self._job_button_start_text(spec))
        if self.active_stdin == key:
            self.active_stdin = None
        self.refresh_status()

    def stop_job(self, key: str) -> None:
        job = self.jobs.get(key)
        if job is None or not job.running:
            return
        spec = self.JOBS[key]
        if key == "serve":
            self._hide_subscription_panel()
        assert job.process is not None
        if spec.get("stoppable") and job.cancel_file is not None:
            self.log_line(f"[{spec['label']}] stopping (saving checked servers)…")
            try:
                job.cancel_file.parent.mkdir(parents=True, exist_ok=True)
                job.cancel_file.write_text("1", encoding="utf-8")
            except OSError as exc:
                self.log_line(f"[{spec['label']}] cancel flag failed: {exc}")
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if job.process.poll() is not None:
                    return
                time.sleep(0.2)
            self.log_line(f"[{spec['label']}] force stopping…")
            kill_process_tree(job.process)
            return
        self.log_line(f"[{spec['label']}] stopping…")
        kill_process_tree(job.process)

    def send_stdin(self) -> None:
        text = self.stdin_entry.get()
        self.stdin_entry.delete(0, "end")
        key = self.active_stdin
        if key is None or not self.jobs.get(key, Job("", [])).running:
            running = [k for k, j in self.jobs.items() if j.running]
            key = running[0] if running else None
        if key is None:
            self.log_line("[input] no running task to send input to")
            return
        job = self.jobs[key]
        assert job.process is not None and job.process.stdin is not None
        try:
            job.process.stdin.write(text + "\n")
            job.process.stdin.flush()
            self.log_line(f"[input → {self.JOBS[key]['label']}] {text}")
        except OSError as exc:
            self.log_line(f"[input] failed: {exc}")

    # ------------------------------------------------------- subscription

    def _show_subscription_panel(
        self, bind_host: str, port: int, filename: str
    ) -> None:
        from fetch_mtproto.config_loader import load_config
        from fetch_mtproto.subscription_server import (
            make_qr_photoimage,
            primary_subscription_url,
            subscription_urls,
        )

        config = load_config(required=False)
        self._subscription_urls = subscription_urls(
            bind_host=bind_host, port=port, filename=filename, config=config
        )
        lines = [f"{label}: {url}" for label, url in self._subscription_urls]
        self.sub_placeholder.pack_forget()
        self.sub_urls_box.pack(fill="x", pady=(4, 6))
        self.sub_urls_box.configure(state="normal")
        self.sub_urls_box.delete("1.0", "end")
        self.sub_urls_box.insert("1.0", "\n".join(lines))
        self.sub_urls_box.configure(state="disabled")

        primary = primary_subscription_url(
            bind_host=bind_host, port=port, filename=filename, config=config
        )
        self._qr_photo = make_qr_photoimage(primary)
        if self._qr_photo is not None:
            self.qr_missing_label.pack_forget()
            self.qr_label.configure(image=self._qr_photo)
            self.qr_label.pack(side="right", padx=(12, 0))
        else:
            self.qr_label.pack_forget()
            self.qr_missing_label.pack(side="right", padx=(12, 0))

        self.notebook.select(self.sub_tab)

    def _hide_subscription_panel(self) -> None:
        self.sub_urls_box.pack_forget()
        self.qr_label.pack_forget()
        self.qr_missing_label.pack_forget()
        self.qr_label.configure(image="")
        self._qr_photo = None
        self._subscription_urls = []
        self.sub_urls_box.configure(state="normal")
        self.sub_urls_box.delete("1.0", "end")
        self.sub_urls_box.configure(state="disabled")
        if not self.sub_placeholder.winfo_ismapped():
            self.sub_placeholder.pack(anchor="w")

    def _copy_subscription_url(self) -> None:
        if not self._subscription_urls:
            return
        # Reconstruct primary from stored URLs (LAN preferred).
        for label, url in self._subscription_urls:
            if label == "LAN" or label.startswith("LAN ("):
                self.root.clipboard_clear()
                self.root.clipboard_append(url)
                self.log_line(f"[subscription] copied LAN URL: {url}")
                return
        _, url = self._subscription_urls[0]
        self.root.clipboard_clear()
        self.root.clipboard_append(url)
        self.log_line(f"[subscription] copied URL: {url}")

    # ------------------------------------------------------------- actions

    # ----------------------------------------------------------- proxy pool

    def toggle_proxy_pool(self) -> None:
        if self.proxy_pool is not None and self.proxy_pool.running:
            self.stop_proxy_pool()
        else:
            self.start_proxy_pool()

    def start_proxy_pool(self) -> None:
        from fetch_mtproto.config_loader import load_config
        from fetch_mtproto.v2ray.ping import resolve_xray_bin
        from fetch_mtproto.v2ray.proxy_pool import ProxyPoolRunner

        if self.proxy_pool is not None and self.proxy_pool.running:
            self.log_line("[proxy pool] already running")
            return

        try:
            start_port = max(1024, int(self.pool_start_port.get()))
            count = max(1, min(50, int(self.pool_count.get())))
            switch_sec = max(30, int(self.pool_switch_sec.get()))
        except tk.TclError:
            messagebox.showerror("fetch-mtproto", "Invalid proxy pool settings.")
            return

        last_port = ProxyPoolRunner.last_port(start_port, count)
        if last_port > 65535:
            messagebox.showerror(
                "fetch-mtproto",
                f"Port range {start_port}–{last_port} exceeds 65535 "
                f"(each slot uses SOCKS5 + HTTP on two consecutive ports).",
            )
            return

        config = load_config(required=False)
        xray_bin = resolve_xray_bin(getattr(config, "XRAY_BIN", None) if config else None)
        if not xray_bin:
            messagebox.showerror(
                "fetch-mtproto",
                "Xray binary not found. Run setup or set xray.bin in config.yaml.",
            )
            return

        self.proxy_pool = ProxyPoolRunner(
            start_port=start_port,
            count=count,
            switch_interval_sec=float(switch_sec),
            xray_bin=xray_bin,
            log=self.log_line,
            on_status=self._update_proxy_pool_status,
            on_finished=self._proxy_pool_finished,
        )
        self.proxy_pool.start()
        self.pool_btn.configure(text="Stop proxy pool")
        for widget in self.pool_pool_inputs:
            widget.configure(state="disabled")
        self.log_line(
            f"[proxy pool] starting {count} slot(s): SOCKS5+HTTP on ports "
            f"{start_port}–{last_port} (switch every {switch_sec}s)"
        )
        self.notebook.select(self.proxy_pool_tab)

    def stop_proxy_pool(self) -> None:
        if self.proxy_pool is None:
            return
        pool = self.proxy_pool
        self.log_line("[proxy pool] stopping…")
        self.pool_btn.configure(state="disabled")
        # Don't block the Tk UI — kill Xray + join on a worker thread.
        threading.Thread(
            target=self._stop_proxy_pool_worker,
            args=(pool,),
            daemon=True,
            name="proxy-pool-stop",
        ).start()

    def _stop_proxy_pool_worker(self, pool) -> None:
        try:
            pool.stop()
        except Exception as exc:
            self.log_line(f"[proxy pool] stop failed: {exc}")
            self.root.after(0, self._proxy_pool_finished)

    def _update_proxy_pool_status(self, statuses) -> None:
        def apply() -> None:
            self.pool_tree.delete(*self.pool_tree.get_children())
            for index, item in enumerate(statuses, start=1):
                latency = (
                    f"{item.latency_ms:.0f} ms"
                    if item.latency_ms is not None
                    else "—"
                )
                if item.error:
                    state = f"error: {item.error}"
                elif item.running:
                    state = "running"
                else:
                    state = "stopped"
                self.pool_tree.insert(
                    "",
                    "end",
                    values=(
                        index,
                        f"socks5://127.0.0.1:{item.socks_port}",
                        f"http://127.0.0.1:{item.http_port}",
                        item.host,
                        item.scheme,
                        latency,
                        state,
                    ),
                )

        self.root.after(0, apply)

    def _proxy_pool_finished(self) -> None:
        def apply() -> None:
            self.proxy_pool = None
            self.pool_btn.configure(text="Start proxy pool", state="normal")
            for widget in self.pool_pool_inputs:
                widget.configure(state="normal")
            self._update_proxy_pool_status([])

        self.root.after(0, apply)

    def _load_top_proxies(self, count: int):
        from fetch_mtproto.catalogs import open_catalogs
        from fetch_mtproto.config_loader import load_config

        config = load_config(required=False)
        db, catalog, _v2 = open_catalogs(config)
        try:
            return catalog.working.all()[:count]
        finally:
            db.close()

    def open_top_proxies(self) -> None:
        count = max(1, int(self.top_count.get()))
        threading.Thread(
            target=self._open_top_proxies_worker, args=(count,), daemon=True
        ).start()

    def copy_top_proxies(self) -> None:
        proxies = self._load_top_proxies(10)
        if not proxies:
            self.log_line(
                "[proxies] no working MTProto proxies — run Ping MTProto first"
            )
            return
        text = "\n".join(proxy.to_link() for proxy in proxies)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.log_line(f"[proxies] copied {len(proxies)} link(s) to clipboard")

    def _open_top_proxies_worker(self, count: int) -> None:
        proxies = self._load_top_proxies(count)

        if not proxies:
            self.log_line(
                "[proxies] no working MTProto proxies — run Ping MTProto first"
            )
            return

        use_exe = os.path.exists(TELEGRAM_EXE)
        if not use_exe:
            self.log_line(
                "[proxies] Telegram Desktop not found — using tg:// protocol handler"
            )
        self.log_line(
            f"[proxies] opening {len(proxies)} link(s) in Telegram "
            "(accept each 'Enable proxy?' prompt)"
        )
        for i, proxy in enumerate(proxies, 1):
            link = proxy.to_link()
            self.log_line(f"[proxies] [{i}/{len(proxies)}] {link}")
            try:
                if use_exe:
                    subprocess.Popen([TELEGRAM_EXE, "--", link], **hide_console_kwargs())
                else:
                    os.startfile(link)  # tg:// handler
            except OSError as exc:
                self.log_line(f"[proxies] failed to open: {exc}")
                return
            if i < len(proxies):
                time.sleep(2)
        self.log_line("[proxies] done — check Telegram Settings → Connection type")

    def refresh_status(self) -> None:
        threading.Thread(target=self._refresh_status_worker, daemon=True).start()

    def _refresh_status_worker(self) -> None:
        try:
            from fetch_mtproto.catalogs import open_catalogs
            from fetch_mtproto.config_loader import load_config

            config = load_config(required=False)
            db, mt, v2 = open_catalogs(config)
            try:
                mt_h = db.mtproto_health_summary()
                v2_h = db.v2ray_health_summary()
            finally:
                db.close()
            mt_avg = f", ~{mt_h['avg_ok_ms']:.0f}ms" if mt_h["avg_ok_ms"] else ""
            v2_avg = f", ~{v2_h['avg_ok_ms']:.0f}ms" if v2_h["avg_ok_ms"] else ""
            text = (
                f"MTProto {mt_h['working']}/{mt_h['total']} ok "
                f"(Σ✓{mt_h['successes']} Σ✗{mt_h['failures']}{mt_avg}) · "
                f"V2Ray {v2_h['working']}/{v2_h['total']} ok "
                f"(Σ✓{v2_h['successes']} Σ✗{v2_h['failures']}{v2_avg})"
            )
        except Exception as exc:
            text = f"status error: {exc}"
        self.root.after(0, self.status_var.set, f"Status: {text}")

    # ------------------------------------------------------------ shutdown

    def _on_close(self) -> None:
        running = [k for k, j in self.jobs.items() if j.running]
        pool_running = self.proxy_pool is not None and self.proxy_pool.running
        if running or pool_running:
            labels = ", ".join(self.JOBS[k]["label"] for k in running)
            if pool_running:
                labels = f"{labels}, Proxy pool" if labels else "Proxy pool"
            if not messagebox.askyesno(
                "fetch-mtproto", f"Stop running tasks and exit?\n({labels})"
            ):
                return
            for key in running:
                job = self.jobs[key]
                if job.process is not None:
                    kill_process_tree(job.process)
            if pool_running and self.proxy_pool is not None:
                self.proxy_pool.stop()
        self.root.destroy()

    def _schedule_auto_start_jobs(self) -> None:
        from fetch_mtproto.config_loader import load_config

        config = load_config(required=False)
        if not config:
            return
        if bool(getattr(config, "GUI_AUTO_START_SCRAPER", False)):
            self.root.after(100, lambda: self.start_job("scrape"))
        if bool(getattr(config, "GUI_AUTO_START_SUBSCRIPTION_SERVER", False)):
            self.root.after(200, lambda: self.start_job("serve"))

    def run(self) -> None:
        self.log_line("fetch-mtproto control panel ready.")
        self.log_line(
            "Scraper login: when prompted, type phone / code below and press Send."
        )
        self._schedule_auto_start_jobs()
        self.root.mainloop()


def main() -> None:
    App().run()


if __name__ == "__main__":
    main()
