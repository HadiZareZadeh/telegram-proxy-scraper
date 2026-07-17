"""Project and package path helpers."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent

SESSIONS_DIR = PROJECT_ROOT / "sessions"
LOGS_DIR = PROJECT_ROOT / "logs"
XRAY_DIR = PROJECT_ROOT / "xray"


def ensure_runtime_dirs() -> None:
    """Create folders for runtime artifacts (sessions, logs, data)."""
    for path in (SESSIONS_DIR, LOGS_DIR, PROJECT_ROOT / "data" / "mtproto", PROJECT_ROOT / "data" / "v2ray"):
        path.mkdir(parents=True, exist_ok=True)


def session_path(name: str) -> Path:
    """Return the Telethon session base path under sessions/, migrating legacy root files."""
    ensure_runtime_dirs()
    stem = Path(name).name
    if stem.endswith(".session"):
        stem = stem[: -len(".session")]
    dest = SESSIONS_DIR / stem
    for suffix in (".session", ".session-journal"):
        old = PROJECT_ROOT / f"{stem}{suffix}"
        new = SESSIONS_DIR / f"{stem}{suffix}"
        if old.is_file() and not new.is_file():
            old.rename(new)
    return dest
