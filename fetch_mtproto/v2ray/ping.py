"""Test V2Ray / Xray share links: TCP prefilter + HTTP probe through local Xray."""

from __future__ import annotations

import asyncio
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from python_socks.async_.asyncio import Proxy

from fetch_mtproto.paths import PROJECT_ROOT, XRAY_DIR
from fetch_mtproto.v2ray.pool_ports import (
    DEFAULT_PING_API_PORT,
    DEFAULT_PING_BASE_PORT,
    DEFAULT_PING_CONCURRENCY,
    clamp_ping_concurrency,
)
from fetch_mtproto.v2ray.port_cleanup import cleanup_ping_xray, wait_ping_ports_free
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer, XRAY_SCHEMES
from fetch_mtproto.v2ray.xray import link_to_xray_outbound
from fetch_mtproto.v2ray.xray_session import (
    XrayLiveSession,
    ping_api_port,
    ping_live_slots,
)

ROOT = PROJECT_ROOT

DEFAULT_TEST_URL = "http://www.gstatic.com/generate_204"
DEFAULT_TEST_BYTES = 0
DEFAULT_TEST_TIMEOUT = 2.5
DEFAULT_TCP_TIMEOUT = 1.25
DEFAULT_TCP_CONCURRENCY = 500
DEFAULT_HTTP_TIMEOUT = 2.5
# Cap one due-queue wave so Windows ephemeral ports (especially 1024–15000) are not exhausted.
DUE_WAVE_LIMIT = 1500


@dataclass(slots=True)
class V2RayPingResult:
    server: V2RayServer
    latency: float | None
    error: str | None = None
    bytes_read: int = 0
    probe_type: str = "xray"

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

    for rel in ("xray.exe", "xray", "bin/xray.exe", "bin/xray"):
        path = ROOT / rel
        if path.is_file():
            return str(path.resolve())

    return None


async def tcp_prefilter(
    host: str, port: int, *, timeout: float
) -> tuple[bool, str | None]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)),
            timeout=timeout,
        )
    except Exception as exc:
        return False, str(exc) or type(exc).__name__
    try:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, None
    except Exception as exc:
        return False, str(exc) or type(exc).__name__


async def _http_via_socks(
    *,
    socks_port: int,
    url: str,
    timeout: float,
    max_bytes: int,
    method: str = "HEAD",
) -> tuple[float, int]:
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
    verb = "HEAD" if method.upper() == "HEAD" else "GET"

    started = time.perf_counter()
    proxy = Proxy.from_url(f"socks5://127.0.0.1:{socks_port}")
    sock = await asyncio.wait_for(
        proxy.connect(dest_host=host, dest_port=port),
        timeout=timeout,
    )
    reader, writer = await asyncio.open_connection(sock=sock)
    try:
        request = (
            f"{verb} {path} HTTP/1.1\r\n"
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
        if len(parts) < 2 or not parts[1].isdigit():
            raise RuntimeError(f"HTTP {status_line}")
        code = int(parts[1])
        if verb == "HEAD" and code in {405, 501}:
            raise RuntimeError("HEAD_NOT_ALLOWED")
        if not (parts[1].startswith("2") or parts[1].startswith("3")):
            raise RuntimeError(f"HTTP {status_line}")

        total = 0
        remaining = max(0, max_bytes) if verb == "GET" else 0
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

    try:
        swap_errors = await session.set_outbounds_async(outbounds)
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
            try:
                latency, nbytes = await _http_via_socks(
                    socks_port=port,
                    url=test_url,
                    timeout=timeout,
                    max_bytes=test_bytes,
                    method="HEAD",
                )
            except RuntimeError as exc:
                if str(exc) != "HEAD_NOT_ALLOWED":
                    raise
                latency, nbytes = await _http_via_socks(
                    socks_port=port,
                    url=test_url,
                    timeout=timeout,
                    max_bytes=test_bytes,
                    method="GET",
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
        err = "xray binary not found"
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
        try:
            await own.close_control()
        except Exception:
            pass
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
    tcp_concurrency: int = DEFAULT_TCP_CONCURRENCY,
    tcp_timeout: float = DEFAULT_TCP_TIMEOUT,
) -> list[V2RayPingResult]:
    """TCP prefilter then batched Xray HTTP probes on one long-lived process."""
    del max_working, initial_working_keys
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

    total = len(servers)
    results: list[V2RayPingResult] = []
    done = 0
    tcp_chunk = max(1, int(tcp_concurrency))

    async def _tcp(server: V2RayServer) -> V2RayPingResult | V2RayServer:
        if cancel_event and cancel_event.is_set():
            return V2RayPingResult(
                server=server, latency=None, error="cancelled", probe_type="tcp"
            )
        ok, err = await tcp_prefilter(
            server.host, server.port, timeout=tcp_timeout
        )
        if not ok:
            return V2RayPingResult(
                server=server,
                latency=None,
                error=err or "tcp unreachable",
                probe_type="tcp",
            )
        return server

    session: XrayLiveSession | None = None
    try:
        for offset in range(0, len(servers), tcp_chunk):
            if cancel_event and cancel_event.is_set():
                break
            chunk = servers[offset : offset + tcp_chunk]
            tcp_out = await asyncio.gather(*(_tcp(server) for server in chunk))
            survivors: list[V2RayServer] = []
            for item in tcp_out:
                if isinstance(item, V2RayPingResult):
                    done += 1
                    results.append(item)
                    if on_result:
                        on_result(done, total, item)
                else:
                    survivors.append(item)
            if not survivors:
                continue
            if session is not None and not session.running:
                try:
                    await session.close_control()
                except Exception:
                    pass
                await loop.run_in_executor(None, session.stop)
                session = None
            if session is None:
                session = XrayLiveSession(
                    bin_path=bin_path,
                    slots=ping_live_slots(base_port, batch_size),
                    api_port=ping_api_port(base_port, batch_size),
                    prefix="xray-ping",
                )
                try:
                    await loop.run_in_executor(None, session.start)
                except Exception:
                    session = None
                    for server in survivors:
                        done += 1
                        fail = V2RayPingResult(
                            server=server,
                            latency=None,
                            error="outbound swap failed: probe xray did not start",
                        )
                        results.append(fail)
                        if on_result:
                            on_result(done, total, fail)
                    continue
            next_index = 0
            while next_index < len(survivors):
                if cancel_event and cancel_event.is_set():
                    break
                batch = survivors[next_index : next_index + batch_size]
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
    finally:
        if session is not None:
            try:
                await session.close_control()
            except Exception:
                pass
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
    tcp_concurrency: int = DEFAULT_TCP_CONCURRENCY,
    tcp_timeout: float = DEFAULT_TCP_TIMEOUT,
    due_only: bool = True,
) -> V2RayReorganizeStats:
    """Probe due (or all) catalog servers; update health without trimming working set."""
    del respect_backoff, failed_limit
    workers = clamp_ping_concurrency(concurrency)
    cleaned = cleanup_ping_xray(base_port=base_port, concurrency=workers)

    if due_only and hasattr(catalog, "due_servers"):
        servers = catalog.due_servers(limit=DUE_WAVE_LIMIT)
    else:
        servers = catalog.all_unique()[:DUE_WAVE_LIMIT]
    if not servers:
        return V2RayReorganizeStats(0, 0, None, cleaned_pids=cleaned)

    total = len(servers)
    pending: list[V2RayPingResult] = []

    def _flush_pending() -> None:
        if pending and hasattr(catalog, "apply_ping_results"):
            catalog.apply_ping_results(pending)
            pending.clear()

    def _on_result(done: int, total_n: int, result: V2RayPingResult) -> None:
        pending.append(result)
        if len(pending) >= 100:
            _flush_pending()
        if on_result:
            on_result(done, total_n, result)

    results = await ping_v2ray_servers(
        servers,
        concurrency=workers,
        base_port=base_port,
        timeout=timeout,
        test_url=test_url,
        test_bytes=test_bytes,
        xray_bin=xray_bin,
        on_result=_on_result,
        cancel_event=cancel_event,
        tcp_concurrency=tcp_concurrency,
        tcp_timeout=tcp_timeout,
    )
    _flush_pending()
    cancelled = bool(cancel_event and cancel_event.is_set())
    checked = len(results)
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
