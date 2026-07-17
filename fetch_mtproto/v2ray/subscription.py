"""Build a standard Base64 V2Ray subscription from share links."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Iterable, Protocol


class ShareLink(Protocol):
    def to_link(self) -> str: ...


def write_subscription(
    servers: Iterable[ShareLink],
    output_path: str | Path,
) -> int:
    """Write unique share links as a Base64 subscription, returning the link count."""
    links: list[str] = []
    seen: set[str] = set()
    for server in servers:
        link = server.to_link().strip()
        if not link or link in seen:
            continue
        seen.add(link)
        links.append(link)

    plain = "\n".join(links) + ("\n" if links else "")
    encoded = base64.b64encode(plain.encode("utf-8")).decode("ascii")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(encoded, encoding="ascii")
    temporary.replace(path)
    return len(links)
