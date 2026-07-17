"""Cooperative cancellation for long-running ping jobs."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

CANCEL_ENV = "FETCH_MTPROTO_CANCEL_FILE"


def cancel_file_path() -> Path | None:
    raw = os.environ.get(CANCEL_ENV, "").strip()
    if not raw:
        return None
    return Path(raw)


def request_cancel() -> None:
    path = cancel_file_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("1", encoding="utf-8")


def clear_cancel_file() -> None:
    path = cancel_file_path()
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def install_signal_handlers(
    cancel_event: asyncio.Event,
    loop: asyncio.AbstractEventLoop,
) -> None:
    def _request(*_args: object) -> None:
        cancel_event.set()
        request_cancel()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, _request)
        signal.signal(getattr(signal, "SIGBREAK", signal.SIGINT), _request)
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, cancel_event.set)
        except NotImplementedError:
            signal.signal(sig, _request)


async def watch_cancel(
    cancel_event: asyncio.Event,
    *,
    poll_interval: float = 0.2,
) -> None:
    """Set cancel_event when a GUI cancel flag file appears."""
    path = cancel_file_path()
    if path is None:
        return
    while not cancel_event.is_set():
        if path.exists():
            cancel_event.set()
            return
        await asyncio.sleep(poll_interval)


class CancelScope:
    """Install signal + cancel-file watchers for a ping job."""

    def __init__(self) -> None:
        self.event = asyncio.Event()
        self._watcher: asyncio.Task | None = None

    async def __aenter__(self) -> asyncio.Event:
        clear_cancel_file()
        loop = asyncio.get_running_loop()
        install_signal_handlers(self.event, loop)
        self._watcher = asyncio.create_task(watch_cancel(self.event))
        return self.event

    async def __aexit__(self, *_args: object) -> None:
        if self._watcher is not None:
            self._watcher.cancel()
            try:
                await self._watcher
            except asyncio.CancelledError:
                pass
        clear_cancel_file()
