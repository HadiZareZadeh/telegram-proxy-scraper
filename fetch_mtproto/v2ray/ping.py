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
from fetch_mtproto.process_tree import hide_console_kwargs, kill_pid_tree
from fetch_mtproto.v2ray.port_cleanup import (
    DEFAULT_PING_BASE_PORT,
    cleanup_ping_xray,
    wait_ping_ports_free,
)
from fetch_mtproto.v2ray.store import V2RayCatalog, V2RayServer
from fetch_mtproto.v2ray.xray import (
    build_xray_ping_batch_config,
    dumps_config,
    link_to_xray_outbound,
)

ROOT = PROJECT_ROOT

# Empty 204 response — connectivity / latency only (no large download).
DEFAULT_TEST_URL = "http://www.gstatic.com/generate_204"
DEFAULT_TEST_BYTES = 0
DEFAULT_TEST_TIMEOUT = 8.0
DEFAULT_PING_CONCURRENCY = 20


class BatchStartupError(Exception):
    """Raised when the shared batch Xray process fails before probes can run."""

    def __init__(self, message: str, *, stderr: str = "") -> None:
        super().__init__(message)
        self.stderr = stderr


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


def _read_xray_stderr(path: str | None, *, max_chars: int = 400) -> str:
    if not path:
        return ""
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""
    if len(text) > max_chars:
        return text[-max_chars:]
    return text


async def _wait_batch_ports(
    proc: asyncio.subprocess.Process,
    ports: list[int],
    *,
    timeout: float,
    stderr_path: str | None = None,
) -> set[int]:
    """Return the subset of SOCKS ports Xray opened within the timeout."""
    loop = asyncio.get_running_loop()
    ready: set[int] = set()
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if proc.returncode is not None:
            detail = _read_xray_stderr(stderr_path)
            msg = f"Xray exited early (code {proc.returncode})"
            if detail:
                msg = f"{msg}: {detail}"
            raise RuntimeError(msg)

        for port in ports:
            if port not in ready and await loop.run_in_executor(
                None, _port_is_open, "127.0.0.1", port
            ):
                ready.add(port)
        if len(ready) == len(ports):
            return ready
        await asyncio.sleep(0.05)
    return ready


async def _validate_xray_config(bin_path: str, cfg_path: str) -> str | None:
    """Return an error string when Xray rejects the config, else None."""
    proc = await asyncio.create_subprocess_exec(
        bin_path,
        "run",
        "-test",
        "-c",
        cfg_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        **hide_console_kwargs(),
    )
    _stdout, stderr = await proc.communicate()
    if proc.returncode == 0:
        return None
    detail = stderr.decode("utf-8", errors="replace").strip() if stderr else ""
    if detail:
        return detail
    return f"Xray config test failed (code {proc.returncode})"


async def _release_ping_ports(
    *,
    base_port: int,
    concurrency: int,
) -> None:
    """Kill the batch Xray and wait until its SOCKS ports are free for reuse."""
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: wait_ping_ports_free(
                base_port=base_port,
                concurrency=concurrency,
                timeout=3.0,
            ),
        )
    except TimeoutError:
        await loop.run_in_executor(
            None,
            lambda: cleanup_ping_xray(base_port=base_port, concurrency=concurrency),
        )
        await asyncio.sleep(0.25)
        await loop.run_in_executor(
            None,
            lambda: wait_ping_ports_free(
                base_port=base_port,
                concurrency=concurrency,
                timeout=2.0,
            ),
        )


async def _kill_xray_proc(proc: asyncio.subprocess.Process | None) -> None:
    if proc is None or proc.returncode is not None:
        return
    try:
        kill_pid_tree(proc.pid, timeout=3.0)
    except Exception:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=3.0)
    except Exception:
        pass


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


async def _probe_prepared_batch(
    *,
    bin_path: str,
    prepared: list[tuple[int, V2RayServer, dict]],
    port_by_index: dict[int, int],
    base_port: int,
    timeout: float,
    test_url: str,
    test_bytes: int,
) -> list[tuple[int, V2RayPingResult]]:
    """Run one shared Xray process for a batch config."""
    batch_size = len(prepared)
    cfg_path = None
    stderr_path = None
    proc = None
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: wait_ping_ports_free(
                base_port=base_port,
                concurrency=batch_size,
                timeout=5.0,
            ),
        )

        config = build_xray_ping_batch_config(
            [outbound for _index, _server, outbound in prepared],
            base_port=base_port,
        )
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
            prefix="xray-ping-",
        ) as handle:
            handle.write(dumps_config(config))
            cfg_path = handle.name

        config_err = await _validate_xray_config(bin_path, cfg_path)
        if config_err:
            raise BatchStartupError(
                f"invalid Xray config: {config_err}",
                stderr=config_err,
            )

        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".log", prefix="xray-ping-err-"
        ) as err_handle:
            stderr_path = err_handle.name

        with open(stderr_path, "wb") as stderr_file:
            proc = await asyncio.create_subprocess_exec(
                bin_path,
                "run",
                "-c",
                cfg_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=stderr_file,
                **hide_console_kwargs(),
            )

        wait_timeout = min(12.0, max(8.0, timeout))
        ready_ports = await _wait_batch_ports(
            proc,
            list(port_by_index.values()),
            timeout=wait_timeout,
            stderr_path=stderr_path,
        )

        async def _one(index: int, server: V2RayServer) -> V2RayPingResult:
            port = port_by_index[index]
            if port not in ready_ports:
                detail = f"Xray SOCKS port {port} did not open"
                if not ready_ports:
                    xray_err = _read_xray_stderr(stderr_path)
                    if xray_err:
                        detail = f"{detail} ({xray_err})"
                return V2RayPingResult(server=server, latency=None, error=detail)
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

        probed = await asyncio.gather(
            *(_one(index, server) for index, server, _outbound in prepared)
        )
        return list(zip((index for index, _server, _outbound in prepared), probed))
    except RuntimeError as exc:
        detail = str(exc) or type(exc).__name__
        stderr = _read_xray_stderr(stderr_path)
        raise BatchStartupError(detail, stderr=stderr) from exc
    finally:
        await _kill_xray_proc(proc)
        if cfg_path:
            try:
                os.unlink(cfg_path)
            except OSError:
                pass
        if stderr_path:
            try:
                os.unlink(stderr_path)
            except OSError:
                pass
        try:
            await _release_ping_ports(base_port=base_port, concurrency=batch_size)
        except Exception:
            pass


async def _fallback_probe_servers(
    servers: list[V2RayServer],
    *,
    base_port: int,
    timeout: float,
    test_url: str,
    test_bytes: int,
    xray_bin: str | None,
) -> list[V2RayPingResult]:
    """Probe servers one at a time when a shared batch config cannot start."""
    results: list[V2RayPingResult] = []
    for server in servers:
        results.extend(
            await ping_v2ray_batch(
                [server],
                base_port=base_port,
                timeout=timeout,
                test_url=test_url,
                test_bytes=test_bytes,
                xray_bin=xray_bin,
            )
        )
    return results


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
) -> list[V2RayPingResult]:
    """Probe a batch of servers through one Xray process (one SOCKS port each)."""
    if not servers:
        return []

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

    prepared: list[tuple[int, V2RayServer, dict]] = []
    early: list[V2RayPingResult | None] = [None] * len(servers)
    for index, server in enumerate(servers):
        outbound = link_to_xray_outbound(server)
        if outbound is None:
            early[index] = V2RayPingResult(
                server=server,
                latency=None,
                error=f"unsupported scheme for Xray test: {server.scheme}",
            )
            continue
        prepared.append((index, server, outbound))

    if not prepared:
        return [result for result in early if result is not None]

    port_by_index = {
        index: int(base_port) + prep_i
        for prep_i, (index, _server, _outbound) in enumerate(prepared)
    }

    try:
        probed = await _probe_prepared_batch(
            bin_path=bin_path,
            prepared=prepared,
            port_by_index=port_by_index,
            base_port=base_port,
            timeout=timeout,
            test_url=test_url,
            test_bytes=test_bytes,
        )
        for index, result in probed:
            early[index] = result
    except BatchStartupError as exc:
        fallback_servers = [server for _index, server, _outbound in prepared]
        if len(fallback_servers) > 1:
            fallback_results = await _fallback_probe_servers(
                fallback_servers,
                base_port=base_port,
                timeout=timeout,
                test_url=test_url,
                test_bytes=test_bytes,
                xray_bin=xray_bin,
            )
            by_key = {result.server.key: result for result in fallback_results}
            for index, server, _outbound in prepared:
                early[index] = by_key.get(
                    server.key,
                    V2RayPingResult(server=server, latency=None, error=str(exc)),
                )
        else:
            detail = str(exc) or type(exc).__name__
            if exc.stderr and exc.stderr not in detail:
                detail = f"{detail} ({exc.stderr})"
            index, server, _outbound = prepared[0]
            early[index] = V2RayPingResult(
                server=server, latency=None, error=detail
            )

    return [result for result in early if result is not None]


def _select_batch(
    servers: list[V2RayServer],
    *,
    start: int,
    batch_size: int,
    max_working: int | None,
    working_keys: set[str],
) -> tuple[list[V2RayServer], int]:
    """Pick the next batch; after max_working, only re-check already-working keys."""
    batch: list[V2RayServer] = []
    index = start
    total = len(servers)
    while index < total and len(batch) < batch_size:
        if max_working is not None and len(working_keys) >= max_working:
            if servers[index].key not in working_keys:
                index += 1
                continue
        batch.append(servers[index])
        index += 1
    return batch, index


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
    """Probe servers in batches; each batch shares one Xray process and port window."""
    if not servers:
        return []

    batch_size = clamp_ping_concurrency(concurrency)
    results: list[V2RayPingResult] = []
    working_keys: set[str] = set(initial_working_keys or ())
    done = 0
    next_index = 0
    total = len(servers)

    while next_index < total:
        if cancel_event and cancel_event.is_set():
            break

        batch, next_index = _select_batch(
            servers,
            start=next_index,
            batch_size=batch_size,
            max_working=max_working,
            working_keys=working_keys,
        )
        if not batch:
            break

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
            if result.ok and result.latency is not None:
                working_keys.add(result.server.key)
            else:
                working_keys.discard(result.server.key)
            results.append(result)
            if on_result:
                on_result(done, total, result)
        if cancel_event and cancel_event.is_set():
            break

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
    workers = clamp_ping_concurrency(concurrency)
    cleaned = cleanup_ping_xray(base_port=base_port, concurrency=workers)

    if hasattr(catalog, "probe_queue"):
        servers = catalog.probe_queue(
            respect_backoff=respect_backoff, failed_limit=failed_limit
        )
    else:
        servers = catalog.all_unique()
    if not servers:
        return V2RayReorganizeStats(0, 0, None, cleaned_pids=cleaned)

    total = len(servers)
    max_working = catalog.max_working
    initial_working_keys = None
    if max_working is not None:
        initial_working_keys = {
            s.key for view in catalog.working.values() for s in view.all()
        }

    results = await ping_v2ray_servers(
        servers,
        concurrency=workers,
        base_port=base_port,
        timeout=timeout,
        test_url=test_url,
        test_bytes=test_bytes,
        xray_bin=xray_bin,
        on_result=on_result,
        max_working=max_working,
        initial_working_keys=initial_working_keys,
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
