"""Parse, normalize, and persist MTProto proxy links (SQLite-backed)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlparse

from fetch_mtproto.db import CatalogDB

# tg://proxy?...  or  https://t.me/proxy?...  (also telegram.me)
_PROXY_URL_RE = re.compile(
    r"(?:"
    r"tg://proxy\?[^\s<>\"']+"
    r"|"
    r"https?://(?:t\.me|telegram\.me)/(?:proxy|socks)\?[^\s<>\"']+"
    r")",
    re.IGNORECASE,
)

# Bare server:port:secret triples sometimes pasted without a link
_BARE_PROXY_RE = re.compile(
    r"(?P<server>(?:\d{1,3}\.){3}\d{1,3}|[a-zA-Z0-9](?:[a-zA-Z0-9\-]*[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9\-]*[a-zA-Z0-9])?)+)"
    r":(?P<port>\d{1,5})"
    r":(?P<secret>[0-9a-fA-F]{32,})",
)


@dataclass(frozen=True, slots=True)
class MTProtoProxy:
    server: str
    port: int
    secret: str

    @property
    def key(self) -> str:
        return f"{self.server.lower()}:{self.port}:{self.secret.lower()}"

    @property
    def is_fake_tls(self) -> bool:
        return self.secret.lower().startswith("ee")

    @property
    def is_dd(self) -> bool:
        return self.secret.lower().startswith("dd")

    def to_link(self) -> str:
        return (
            f"tg://proxy?server={self.server}"
            f"&port={self.port}&secret={self.secret}"
        )

    def as_telethon_tuple(self) -> tuple[str, int, str]:
        """Tuple for stock Telethon (dd / plain) or TelethonFakeTLS (ee without prefix)."""
        secret = self.secret
        if self.is_fake_tls:
            # TelethonFakeTLS expects hex without the leading "ee"
            secret = secret[2:]
        return (self.server, self.port, secret)

    def as_db_row(self) -> tuple[str, str, str, int, str]:
        return (self.key, self.to_link(), self.server, self.port, self.secret)


def _normalize_secret(secret: str) -> str | None:
    secret = secret.strip()
    if not secret:
        return None
    # ee / dd secrets (Fake TLS / Simple) may be longer hex strings
    if re.fullmatch(r"[0-9a-fA-F]+", secret) and len(secret) >= 32 and len(secret) % 2 == 0:
        return secret.lower()
    return None


def parse_proxy_url(url: str) -> MTProtoProxy | None:
    raw = url.strip().rstrip(").,>;']\"")
    if not raw:
        return None

    if raw.lower().startswith("tg://"):
        # urlparse treats tg://proxy?... oddly; parse query manually
        if "?" not in raw:
            return None
        query = raw.split("?", 1)[1]
        params = parse_qs(query, keep_blank_values=False)
    else:
        parsed = urlparse(raw)
        path = (parsed.path or "").lower()
        if path not in ("/proxy", "/socks", "proxy", "socks"):
            # t.me/proxy?... has path /proxy
            if not path.endswith("/proxy") and not path.endswith("/socks"):
                if "proxy" not in path and "socks" not in path:
                    return None
        params = parse_qs(parsed.query, keep_blank_values=False)

    server = (params.get("server") or params.get("host") or [None])[0]
    port_s = (params.get("port") or [None])[0]
    secret = (params.get("secret") or [None])[0]

    if not server or not port_s or not secret:
        return None

    try:
        port = int(port_s)
    except ValueError:
        return None
    if not (1 <= port <= 65535):
        return None

    secret = _normalize_secret(unquote(secret))
    if not secret:
        return None

    server = unquote(server).strip().lower()
    return MTProtoProxy(server=server, port=port, secret=secret)


def parse_proxy_line(line: str) -> MTProtoProxy | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    proxy = parse_proxy_url(line) if "://" in line else None
    if proxy is not None:
        return proxy
    if ":" not in line:
        return None
    parts = line.split(":")
    if len(parts) < 3:
        return None
    secret = _normalize_secret(parts[-1])
    try:
        port = int(parts[-2])
    except ValueError:
        return None
    if not secret:
        return None
    return MTProtoProxy(
        server=":".join(parts[:-2]).lower(),
        port=port,
        secret=secret,
    )


def extract_proxies_from_text(text: str) -> list[MTProtoProxy]:
    if not text:
        return []

    found: dict[str, MTProtoProxy] = {}

    for match in _PROXY_URL_RE.finditer(text):
        proxy = parse_proxy_url(match.group(0))
        if proxy:
            found[proxy.key] = proxy

    for match in _BARE_PROXY_RE.finditer(text):
        secret = _normalize_secret(match.group("secret"))
        if not secret:
            continue
        try:
            port = int(match.group("port"))
        except ValueError:
            continue
        if not (1 <= port <= 65535):
            continue
        proxy = MTProtoProxy(
            server=match.group("server").lower(),
            port=port,
            secret=secret,
        )
        found[proxy.key] = proxy

    return list(found.values())


def _proxy_from_row(row) -> MTProtoProxy:
    return MTProtoProxy(
        server=str(row["server"]),
        port=int(row["port"]),
        secret=str(row["secret"]),
    )


class _MtprotoView:
    """Working or failed slice over the shared SQLite catalog."""

    def __init__(self, db: CatalogDB, status: str) -> None:
        self.db = db
        self.status = status
        # Kept for log messages that previously used path.name
        self.path = Path(f"mtproto_{status}")

    def all(self) -> list[MTProtoProxy]:
        return [_proxy_from_row(row) for row in self.db.mtproto_list(self.status)]

    def __contains__(self, key: str) -> bool:
        return self.db.mtproto_has(key, self.status)

    def __len__(self) -> int:
        return self.db.mtproto_count(self.status)


class ProxyCatalog:
    """Working + failed MTProto proxies stored in SQLite."""

    def __init__(self, db: CatalogDB, *, max_working: int | None = None) -> None:
        self.db = db
        self.max_working = max_working
        self.working = _MtprotoView(db, "working")
        self.failed = _MtprotoView(db, "failed")
        self.enforce_max_working()

    def enforce_max_working(self) -> int:
        """Trim the working set to max_working; returns count demoted to failed."""
        if self.max_working is None:
            return 0
        return self.db.mtproto_trim_working(self.max_working)

    def all_unique(self) -> list[MTProtoProxy]:
        """Union of both statuses (working wins on key clash)."""
        merged: dict[str, MTProtoProxy] = {}
        for row in self.db.mtproto_list("failed"):
            proxy = _proxy_from_row(row)
            merged[proxy.key] = proxy
        for row in self.db.mtproto_list("working"):
            proxy = _proxy_from_row(row)
            merged[proxy.key] = proxy
        return list(merged.values())

    def probe_queue(
        self,
        *,
        respect_backoff: bool = True,
        limit: int | None = None,
    ) -> list[MTProtoProxy]:
        """Ordered probe list: most successful / freshest / unexplored first."""
        return [
            _proxy_from_row(row)
            for row in self.db.mtproto_probe_queue(
                respect_backoff=respect_backoff, limit=limit
            )
        ]

    def add(self, proxies: Iterable[MTProtoProxy]) -> int:
        """Add newly discovered proxies as working (and drop from failed)."""
        rows = [p.as_db_row() for p in proxies]
        added = self.db.mtproto_upsert_working(rows)
        if added:
            self.enforce_max_working()
        return added

    def apply_ping_results(self, results: Iterable) -> tuple[int, int]:
        """
        Persist ping outcomes with health counters.

        Each result needs `.proxy`, `.ok`, `.latency`, and optional `.error`.
        """
        outcomes = []
        ok_n = 0
        fail_n = 0
        for result in results:
            proxy = result.proxy
            identity = (proxy.to_link(), proxy.server, proxy.port, proxy.secret)
            if result.ok and result.latency is not None:
                outcomes.append((proxy.key, True, result.latency, None, identity))
                ok_n += 1
            else:
                outcomes.append(
                    (
                        proxy.key,
                        False,
                        None,
                        getattr(result, "error", None),
                        identity,
                    )
                )
                fail_n += 1
        self.db.mtproto_record_results(outcomes)
        self.enforce_max_working()
        return ok_n, fail_n

    def reorganize(
        self,
        ok: Iterable[MTProtoProxy],
        failed: Iterable[MTProtoProxy],
    ) -> None:
        self.db.mtproto_reorganize(
            [p.as_db_row() for p in ok],
            [p.as_db_row() for p in failed],
        )
        self.enforce_max_working()

    def __len__(self) -> int:
        return len(self.working)


def load_mtproto_from_text_file(path: Path) -> list[MTProtoProxy]:
    if not path.exists():
        return []
    found: dict[str, MTProtoProxy] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        proxy = parse_proxy_line(line)
        if proxy:
            found[proxy.key] = proxy
    return list(found.values())
