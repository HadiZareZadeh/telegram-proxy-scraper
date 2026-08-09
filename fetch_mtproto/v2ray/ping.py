"""Test V2Ray / Xray share links with a lightweight HTTP ping through local Xray SOCKS."""

from __future__ import annotations

import asyncio
import shutil
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from python_socks.async_.asyncio import Proxy

from fetch_mtproto.paths import PROJECT_ROOT, XRAY_DIR
from fetch_mtproto.v2ray.port_cleanup import (
    DEFAULT_PING_BASE_PORT,
    cleanup_ping_xray,
    wait_ping_ports_free,
)
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer, XRAY_SCHEMES
from fetch_mtproto.v2ray.xray import link_to_xray_outbound
from fetch_mtproto.v2ray.xray_session import (
    XrayLiveSession,
    ping_api_port,
    ping_live_slots,
)

ROOT = PROJECT_ROOT

# Empty 204 response — connectivity / latency only (no large download).
DEFAULT_TEST_URL = "http://www.gstatic.com/generate_204"
DEFAULT_TEST_BYTES = 0
DEFAULT_TEST_TIMEOUT = 8.0
DEFAULT_PING_CONCURRENCY = 20


@dataclass(slots=True)
class V2RayPingResult:
    server: V2RayServer
    latency: float | None
    error: str | None = None
    bytes_read: int = 0

    @property
    def ok(self) -> bool:
        return self.latency is not None


def resolve_xray_bin(explicit: str | None = None) -> str | None:
    """Resolve Xray binary: explicit config, then PATH, then xray/ folder."""
    if explicit:
        path = Path(explicit)
        if path.is_file():
            return str(path.resolve())
        found = shutil.which(explicit)
        if found:
            return found

    for name in ("xray.exe", "xray"):
        found = shutil.which(name)
        if found:
            return found

    for name in ("xray.exe", "xray"):
        path = XRAY_DIR / name
        if path.is_file():
            return str(path.resolve())

    # Legacy locations (older setups installed to project root or bin/)
    for rel in ("xray.exe", "xray", "bin/xray.exe", "bin/xray"):
        path = ROOT / rel
        if path.is_file():
            return str(path.resolve())

    return None


def clamp_ping_concurrency(value: int) -> int:
    """Keep batch size (ports per single Xray process) in a sane range."""
    try:
        concurrency = int(value)
    except (TypeError, ValueError):
        concurrency = DEFAULT_PING_CONCURRENCY
    return max(1, min(concurrency, 64))


def _port_is_open(host: str, port: int, *, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


async def _ping_via_socks(
    *,
    socks_port: int,
    url: str,
    timeout: float,
    max_bytes: int,
) -> tuple[float, int]:
    """SOCKS connect + HTTP GET; latency is TTFB (headers). Body read is optional."""
    parsed = urlparse(url)
    if parsed.scheme.lower() != "http":
        raise RuntimeError(f"Only http:// test URLs are supported (got {parsed.scheme})")
    host = parsed.hostname
    if not host:
        raise RuntimeError("Invalid test URL host")
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    started = time.perf_counter()
    proxy = Proxy.from_url(f"socks5://127.0.0.1:{socks_port}")
    sock = await asyncio.wait_for(
        proxy.connect(dest_host=host, dest_port=port),
        timeout=timeout,
    )
    reader, writer = await asyncio.open_connection(sock=sock)
    try:
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: fetch-mtproto/1.0\r\n"
            f"Accept: */*\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode()
        writer.write(request)
        await writer.drain()

        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=timeout)
        latency = time.perf_counter() - started
        status_line = header.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
        parts = status_line.split(" ", 2)
        if len(parts) < 2 or not parts[1].isdigit() or not (
            parts[1].startswith("2") or parts[1].startswith("3")
        ):
            raise RuntimeError(f"HTTP {status_line}")

        total = 0
        remaining = max(0, max_bytes)
        while remaining > 0:
            chunk = await asyncio.wait_for(
                reader.read(min(65536, remaining)),
                timeout=timeout,
            )
            if not chunk:
                break
            total += len(chunk)
            remaining -= len(chunk)

        return latency, total
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _probe_batch_on_session(
    session: XrayLiveSession,
    servers: list[V2RayServer],
    *,
    timeout: float,
    test_url: str,
    test_bytes: int,
) -> list[V2RayPingResult]:
    """Load outbounds onto a live session and probe each SOCKS slot in parallel."""
    if not servers:
        return []
    if len(servers) > len(session.slots):
        raise ValueError(
            f"batch of {len(servers)} exceeds session slots ({len(session.slots)})"
        )

    results: list[V2RayPingResult | None] = [None] * len(servers)
    outbounds: list[dict | None] = [None] * len(servers)
    prepared: list[tuple[int, V2RayServer]] = []

    for index, server in enumerate(servers):
        outbound = link_to_xray_outbound(server)
        if outbound is None:
            results[index] = V2RayPingResult(
                server=server,
                latency=None,
                error=(
                    f"unsupported scheme for Xray test: {server.scheme}"
                    if server.scheme not in XRAY_SCHEMES
                    else "invalid Xray outbound (e.g. REALITY missing pbk/password)"
                ),
            )
            continue
        outbounds[index] = outbound
        prepared.append((index, server))

    loop = asyncio.get_running_loop()
    try:
        swap_errors = await loop.run_in_executor(
            None, session.set_outbounds, outbounds
        )
    except Exception as exc:
        detail = str(exc) or type(exc).__name__
        for index, server in prepared:
            results[index] = V2RayPingResult(
                server=server, latency=None, error=f"outbound swap failed: {detail}"
            )
        return [result for result in results if result is not None]

    ready: list[tuple[int, V2RayServer]] = []
    for index, server in prepared:
        err = swap_errors[index] if index < len(swap_errors) else None
        if err:
            results[index] = V2RayPingResult(
                server=server,
                latency=None,
                error=f"outbound swap failed: {err}",
            )
            continue
        ready.append((index, server))

    async def _one(index: int, server: V2RayServer) -> V2RayPingResult:
        port = session.socks_port(index)
        if port is None:
            return V2RayPingResult(
                server=server, latency=None, error="missing SOCKS port for slot"
            )
        try:
            latency, nbytes = await _ping_via_socks(
                socks_port=port,
                url=test_url,
                timeout=timeout,
                max_bytes=test_bytes,
            )
            return V2RayPingResult(server=server, latency=latency, bytes_read=nbytes)
        except Exception as exc:
            detail = str(exc) or type(exc).__name__
            return V2RayPingResult(server=server, latency=None, error=detail)

    if ready:
        probed = await asyncio.gather(*(_one(index, server) for index, server in ready))
        for (index, _server), result in zip(ready, probed):
            results[index] = result

    return [result for result in results if result is not None]


async def ping_v2ray(
    server: V2RayServer,
    *,
    socks_port: int,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
) -> V2RayPingResult:
    """Probe one server (implemented as a single-entry batch)."""
    results = await ping_v2ray_batch(
        [server],
        base_port=socks_port,
        timeout=timeout,
        test_url=test_url,
        test_bytes=test_bytes,
        xray_bin=xray_bin,
    )
    return results[0]


async def ping_v2ray_batch(
    servers: list[V2RayServer],
    *,
    base_port: int,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
    session: XrayLiveSession | None = None,
) -> list[V2RayPingResult]:
    """Probe a batch through a live session (one SOCKS port each)."""
    if not servers:
        return []

    if session is not None:
        return await _probe_batch_on_session(
            session,
            servers,
            timeout=timeout,
            test_url=test_url,
            test_bytes=test_bytes,
        )

    bin_path = resolve_xray_bin(xray_bin)
    if not bin_path:
        err = (
            "xray binary not found (set xray.bin in config.yaml, install xray on PATH, "
            "or run setup to install it in xray/)"
        )
        return [
            V2RayPingResult(server=server, latency=None, error=err)
            for server in servers
        ]

    batch_size = len(servers)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: cleanup_ping_xray(base_port=base_port, concurrency=batch_size),
    )
    await loop.run_in_executor(
        None,
        lambda: wait_ping_ports_free(
            base_port=base_port, concurrency=batch_size, timeout=5.0
        ),
    )
    own = XrayLiveSession(
        bin_path=bin_path,
        slots=ping_live_slots(base_port, batch_size),
        api_port=ping_api_port(base_port, batch_size),
        prefix="xray-ping-batch",
    )
    try:
        await loop.run_in_executor(None, own.start)
        return await _probe_batch_on_session(
            own,
            servers,
            timeout=timeout,
            test_url=test_url,
            test_bytes=test_bytes,
        )
    except Exception as exc:
        detail = str(exc) or type(exc).__name__
        return [
            V2RayPingResult(server=server, latency=None, error=detail)
            for server in servers
        ]
    finally:
        await loop.run_in_executor(None, own.stop)


async def ping_v2ray_servers(
    servers: list[V2RayServer],
    *,
    concurrency: int = DEFAULT_PING_CONCURRENCY,
    base_port: int = DEFAULT_PING_BASE_PORT,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
    on_result=None,
    max_working: int | None = None,
    initial_working_keys: set[str] | None = None,
    cancel_event: asyncio.Event | None = None,
) -> list[V2RayPingResult]:
    """Probe all servers in batches on one long-lived Xray process.

    ``max_working`` is accepted for API compatibility but ignored: every server
    in ``servers`` is probed. Working-set trimming happens when results are applied.
    """
    del max_working, initial_working_keys  # probe everyone in the given list
    if not servers:
        return []

    batch_size = clamp_ping_concurrency(concurrency)
    bin_path = resolve_xray_bin(xray_bin)
    if not bin_path:
        err = (
            "xray binary not found (set xray.bin in config.yaml, install xray on PATH, "
            "or run setup to install it in xray/)"
        )
        results = [
            V2RayPingResult(server=server, latency=None, error=err)
            for server in servers
        ]
        if on_result:
            for done, result in enumerate(results, start=1):
                on_result(done, len(servers), result)
        return results

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: cleanup_ping_xray(base_port=base_port, concurrency=batch_size),
    )
    await loop.run_in_executor(
        None,
        lambda: wait_ping_ports_free(
            base_port=base_port, concurrency=batch_size, timeout=5.0
        ),
    )

    session = XrayLiveSession(
        bin_path=bin_path,
        slots=ping_live_slots(base_port, batch_size),
        api_port=ping_api_port(base_port, batch_size),
        prefix="xray-ping",
    )
    results: list[V2RayPingResult] = []
    total = len(servers)
    done = 0
    next_index = 0

    try:
        try:
            await loop.run_in_executor(None, session.start)
        except Exception:
            # One-shot fallback: per-batch short-lived sessions if shell won't start.
            while next_index < total:
                if cancel_event and cancel_event.is_set():
                    break
                batch = servers[next_index : next_index + batch_size]
                next_index += len(batch)
                batch_results = await ping_v2ray_batch(
                    batch,
                    base_port=base_port,
                    timeout=timeout,
                    test_url=test_url,
                    test_bytes=test_bytes,
                    xray_bin=xray_bin,
                )
                for result in batch_results:
                    done += 1
                    results.append(result)
                    if on_result:
                        on_result(done, total, result)
                if cancel_event and cancel_event.is_set():
                    break
            return results

        while next_index < total:
            if cancel_event and cancel_event.is_set():
                break
            batch = servers[next_index : next_index + batch_size]
            next_index += len(batch)
            batch_results = await _probe_batch_on_session(
                session,
                batch,
                timeout=timeout,
                test_url=test_url,
                test_bytes=test_bytes,
            )
            for result in batch_results:
                done += 1
                results.append(result)
                if on_result:
                    on_result(done, total, result)
            if cancel_event and cancel_event.is_set():
                break
    finally:
        await loop.run_in_executor(None, session.stop)

    return results


@dataclass(slots=True)
class V2RayReorganizeStats:
    ok: int
    failed: int
    fastest: tuple[V2RayServer, float] | None
    checked: int = 0
    total: int = 0
    cancelled: bool = False
    cleaned_pids: list[int] | None = None


async def check_and_reorganize_v2ray(
    catalog: V2RayCatalog,
    *,
    concurrency: int = DEFAULT_PING_CONCURRENCY,
    base_port: int = DEFAULT_PING_BASE_PORT,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
    on_result=None,
    respect_backoff: bool = True,
    failed_limit: int | None = None,
    cancel_event: asyncio.Event | None = None,
) -> V2RayReorganizeStats:
    """Probe the full V2Ray catalog on one long-lived Xray; update health stats.

    ``respect_backoff`` / ``failed_limit`` are accepted for call-site compatibility
    but ignored — every unique catalog server is tested.
    """
    del respect_backoff, failed_limit
    workers = clamp_ping_concurrency(concurrency)
    cleaned = cleanup_ping_xray(base_port=base_port, concurrency=workers)

    servers = catalog.all_unique()
    if not servers:
        return V2RayReorganizeStats(0, 0, None, cleaned_pids=cleaned)

    total = len(servers)
    results = await ping_v2ray_servers(
        servers,
        concurrency=workers,
        base_port=base_port,
        timeout=timeout,
        test_url=test_url,
        test_bytes=test_bytes,
        xray_bin=xray_bin,
        on_result=on_result,
        max_working=None,
        cancel_event=cancel_event,
    )
    cancelled = bool(cancel_event and cancel_event.is_set())
    checked = len(results)

    if hasattr(catalog, "apply_ping_results"):
        if results:
            catalog.apply_ping_results(results)
        ok_ranked = [
            (r.server, r.latency)
            for r in results
            if r.ok and r.latency is not None
        ]
        ok_ranked.sort(key=lambda item: item[1])
        failed_n = sum(1 for r in results if not r.ok or r.latency is None)
        fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
        return V2RayReorganizeStats(
            len(ok_ranked),
            failed_n,
            fastest,
            checked=checked,
            total=total,
            cancelled=cancelled,
            cleaned_pids=cleaned,
        )

    ok_ranked = []
    failed = []
    for result in results:
        if result.ok and result.latency is not None:
            ok_ranked.append((result.server, result.latency))
        else:
            failed.append(result.server)
    ok_ranked.sort(key=lambda item: item[1])
    ok = [server for server, _ in ok_ranked]
    if results:
        catalog.reorganize(ok, failed)
    fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
    return V2RayReorganizeStats(
        len(ok),
        len(failed),
        fastest,
        checked=checked,
        total=total,
        cancelled=cancelled,
        cleaned_pids=cleaned,
    )
