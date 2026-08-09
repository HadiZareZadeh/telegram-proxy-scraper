"""Shared logging setup: console + separate error/debug log files."""

from __future__ import annotations

import logging
import time
from logging.handlers import RotatingFileHandler

from fetch_mtproto.paths import LOGS_DIR, ensure_runtime_dirs

LOGGER_NAME = "mtproto-scraper"
_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"
_CONSOLE_DATEFMT = "%H:%M:%S"

# Rotate before logs grow unbounded (GUI/CLI can run for a long time).
_MAX_BYTES = 5 * 1024 * 1024
_BACKUP_COUNT = 3
# After a Windows lock conflict, wait before retrying rename.
_ROLLOVER_RETRY_SEC = 60.0

_configured = False


class SafeRotatingFileHandler(RotatingFileHandler):
    """RotatingFileHandler that tolerates WinError 32 (file in use by another process).

    GUI, scraper, and ping jobs all write the same log files. On Windows another
    process holding the handle makes os.rename fail; skip rotation and keep writing.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._rollover_fail_until = 0.0

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        if time.monotonic() < self._rollover_fail_until:
            return False
        return super().shouldRollover(record)

    def doRollover(self) -> None:
        try:
            super().doRollover()
            self._rollover_fail_until = 0.0
        except OSError:
            # Stream is closed before rename; reopen so logging can continue.
            self._rollover_fail_until = time.monotonic() + _ROLLOVER_RETRY_SEC
            if self.stream is None:
                self.stream = self._open()


def setup_logging(*, console_level: int = logging.INFO) -> logging.Logger:
    """Configure root handlers once: console, logs/debug.log, logs/error.log."""
    global _configured
    log = logging.getLogger(LOGGER_NAME)
    if _configured:
        return log

    ensure_runtime_dirs()

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt=_CONSOLE_DATEFMT,
    )

    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(console_formatter)
    root.addHandler(console)

    debug_handler = SafeRotatingFileHandler(
        LOGS_DIR / "debug.log",
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(formatter)
    root.addHandler(debug_handler)

    error_handler = SafeRotatingFileHandler(
        LOGS_DIR / "error.log",
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root.addHandler(error_handler)

    logging.getLogger("telethon").setLevel(logging.WARNING)

    _configured = True
    return log
