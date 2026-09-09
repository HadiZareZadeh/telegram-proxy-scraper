"""Check the local Xray binary against the pinned release."""

from __future__ import annotations

import re
import subprocess

from fetch_mtproto.process_tree import hide_console_kwargs
from fetch_mtproto.v2ray.pool_ports import PINNED_XRAY_VERSION


def read_xray_version(bin_path: str) -> str | None:
    try:
        result = subprocess.run(
            [bin_path, "version"],
            capture_output=True,
            text=True,
            timeout=5.0,
            **hide_console_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = (result.stdout or result.stderr or "").strip()
    match = re.search(r"Xray\s+(\d+\.\d+\.\d+)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"\b(\d+\.\d+\.\d+)\b", text)
    return match.group(1) if match else None


def xray_version_warning(bin_path: str) -> str | None:
    found = read_xray_version(bin_path)
    if found is None:
        return f"could not read Xray version from {bin_path}"
    if found != PINNED_XRAY_VERSION:
        return (
            f"Xray {found} is not the pinned release {PINNED_XRAY_VERSION}; "
            "balancer/routing behavior is only regression-tested on the pin"
        )
    return None
