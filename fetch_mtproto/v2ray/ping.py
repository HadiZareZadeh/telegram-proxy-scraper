"""Test V2Ray / Xray share links with a lightweight HTTP ping through local Xray SOCKS."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from python_socks.async_.asyncio import Proxy

from fetch_mtproto.paths import PROJECT_ROOT, XRAY_DIR
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer
from fetch_mtproto.v2ray.xray import build_xray_config, dumps_config, link_to_xray_outbound

ROOT = PROJECT_ROOT

# Empty 204 response — connectivity / latency only (no large download).
DEFAULT_TEST_URL = "http://www.gstatic.com/generate_204"
DEFAULT_TEST_BYTES = 0
DEFAULT_TEST_TIMEOUT = 8.0


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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_port(host: str, port: int, timeout: float) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=0.25,
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            _ = reader
            return
        except Exception:
            await asyncio.sleep(0.05)
    raise TimeoutError(f"Xray SOCKS port {port} did not open")


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


async def ping_v2ray(
    server: V2RayServer,
    *,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
) -> V2RayPingResult:
    outbound = link_to_xray_outbound(server)
    if outbound is None:
        return V2RayPingResult(
            server=server,
            latency=None,
            error=f"unsupported scheme for Xray test: {server.scheme}",
        )

    bin_path = resolve_xray_bin(xray_bin)
    if not bin_path:
        return V2RayPingResult(
            server=server,
            latency=None,
            error="xray binary not found (set xray.bin in config.yaml, install xray on PATH, or run setup to install it in xray/)",
        )

    socks_port = _free_port()
    config = build_xray_config(outbound, socks_port)
    cfg_path = None
    proc = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
            prefix="xray-",
        ) as handle:
            handle.write(dumps_config(config))
            cfg_path = handle.name

        proc = await asyncio.create_subprocess_exec(
            bin_path,
            "run",
            "-c",
            cfg_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        await _wait_port("127.0.0.1", socks_port, timeout=min(8.0, timeout))
        latency, nbytes = await _ping_via_socks(
            socks_port=socks_port,
            url=test_url,
            timeout=timeout,
            max_bytes=test_bytes,
        )
        return V2RayPingResult(server=server, latency=latency, bytes_read=nbytes)
    except Exception as exc:
        detail = str(exc) or type(exc).__name__
        if proc is not None and proc.stderr is not None:
            try:
                err = await asyncio.wait_for(proc.stderr.read(500), timeout=0.2)
                if err:
                    detail = f"{detail}; xray: {err.decode(errors='replace').strip()}"
            except Exception:
                pass
        return V2RayPingResult(server=server, latency=None, error=detail)
    finally:
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except Exception:
                pass
        if cfg_path:
            try:
                os.unlink(cfg_path)
            except OSError:
                pass


async def ping_v2ray_servers(
    servers: list[V2RayServer],
    *,
    concurrency: int = 10,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
    on_result=None,
) -> list[V2RayPingResult]:
    if not servers:
        return []

    sem = asyncio.Semaphore(concurrency)
    results: list[V2RayPingResult] = []
    lock = asyncio.Lock()
    done = 0
    total = len(servers)

    async def _one(server: V2RayServer) -> None:
        nonlocal done
        async with sem:
            result = await ping_v2ray(
                server,
                timeout=timeout,
                test_url=test_url,
                test_bytes=test_bytes,
                xray_bin=xray_bin,
            )
        async with lock:
            done += 1
            results.append(result)
            if on_result:
                on_result(done, total, result)

    await asyncio.gather(*(_one(s) for s in servers))
    return results


@dataclass(slots=True)
class V2RayReorganizeStats:
    ok: int
    failed: int
    fastest: tuple[V2RayServer, float] | None


async def check_and_reorganize_v2ray(
    catalog: V2RayCatalog,
    *,
    concurrency: int = 10,
    timeout: float = DEFAULT_TEST_TIMEOUT,
    test_url: str = DEFAULT_TEST_URL,
    test_bytes: int = DEFAULT_TEST_BYTES,
    xray_bin: str | None = None,
    on_result=None,
    respect_backoff: bool = True,
    limit: int | None = None,
) -> V2RayReorganizeStats:
    if hasattr(catalog, "probe_queue"):
        servers = catalog.probe_queue(
            respect_backoff=respect_backoff, limit=limit
        )
    else:
        servers = catalog.all_unique()
    if not servers:
        return V2RayReorganizeStats(0, 0, None)

    results = await ping_v2ray_servers(
        servers,
        concurrency=concurrency,
        timeout=timeout,
        test_url=test_url,
        test_bytes=test_bytes,
        xray_bin=xray_bin,
        on_result=on_result,
    )

    if hasattr(catalog, "apply_ping_results"):
        catalog.apply_ping_results(results)
        ok_ranked = [
            (r.server, r.latency)
            for r in results
            if r.ok and r.latency is not None
        ]
        ok_ranked.sort(key=lambda item: item[1])
        failed_n = sum(1 for r in results if not r.ok or r.latency is None)
        fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
        return V2RayReorganizeStats(len(ok_ranked), failed_n, fastest)

    ok_ranked = []
    failed = []
    for result in results:
        if result.ok and result.latency is not None:
            ok_ranked.append((result.server, result.latency))
        else:
            failed.append(result.server)
    ok_ranked.sort(key=lambda item: item[1])
    ok = [server for server, _ in ok_ranked]
    catalog.reorganize(ok, failed)
    fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
    return V2RayReorganizeStats(len(ok), len(failed), fastest)
