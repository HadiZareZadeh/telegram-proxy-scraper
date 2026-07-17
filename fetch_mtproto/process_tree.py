"""Kill a process and its descendants (e.g. Xray workers spawned by CLI jobs)."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time


def hide_console_kwargs() -> dict:
    """Extra subprocess kwargs to avoid flashing console windows on Windows."""
    if sys.platform == "win32":
        return {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0)}
    return {}


def kill_process_tree(
    process: subprocess.Popen,
    *,
    timeout: float = 5.0,
) -> None:
    """Stop a subprocess and every process it spawned."""
    if process.poll() is not None:
        return

    pid = process.pid
    if sys.platform == "win32":
        _kill_tree_windows(pid)
    else:
        _kill_tree_unix(pid, timeout=min(timeout, 3.0))

    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pass


def _kill_tree_windows(pid: int) -> None:
    # terminate() on Windows kills only the root process; grandchildren like
    # xray.exe keep running unless the whole tree is torn down.
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        **hide_console_kwargs(),
    )


def _kill_tree_unix(pid: int, *, timeout: float) -> None:
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return

    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            break
        time.sleep(0.05)

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
