"""Ping MTProto proxies (including Fake TLS / ee secrets) via req_pq_multi."""

from __future__ import annotations

import asyncio
import hashlib
import os
import struct
import time
from dataclasses import dataclass

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from fetch_mtproto.mtproto.store import MTProtoProxy

try:
    from TelethonFakeTLS.FakeTLS.FakeTLSHello import MTProxyFakeTLSClientCodec
    from TelethonFakeTLS.FakeTLS.TLSInOut import FakeTLSStreamReader
except ImportError:  # pragma: no cover
    MTProxyFakeTLSClientCodec = None  # type: ignore
    FakeTLSStreamReader = None  # type: ignore


PROTO_SECURE = b"\xdd\xdd\xdd\xdd"
PROTO_INTERMEDIATE = b"\xee\xee\xee\xee"
PROTO_ABRIDGED = b"\xef\xef\xef\xef"
CCS = b"\x14\x03\x03\x00\x01\x01"
REQ_PQ_MULTI = 0xBE7E8EF1
RES_PQ = 0x05162463


def _aes_ctr(key: bytes, iv: bytes):
    return Cipher(algorithms.AES(key), modes.CTR(iv), default_backend()).encryptor()


def _secret_key16(proxy: MTProtoProxy) -> bytes:
    secret = proxy.secret.lower()
    if secret.startswith(("ee", "dd")):
        secret = secret[2:]
    raw = bytes.fromhex(secret)
    return raw[:16]


def _obfuscated2_handshake(secret: bytes, dc_id: int = 2) -> tuple[bytes, object, object]:
    forbidden = {
        b"GET ",
        b"POST",
        b"HEAD",
        b"OPTI",
        b"\x00\x00\x00\x00",
        PROTO_SECURE,
        PROTO_INTERMEDIATE,
        PROTO_ABRIDGED,
    }
    while True:
        init = bytearray(os.urandom(64))
        if init[0] == 0xEF:
            continue
        if bytes(init[:4]) in forbidden:
            continue
        if bytes(init[4:8]) == b"\x00\x00\x00\x00":
            continue
        break

    init[56:60] = PROTO_SECURE
    init[60] = dc_id & 0xFF
    init[61:64] = b"\x00\x00\x00"

    enc_key = hashlib.sha256(bytes(init[8:40]) + secret).digest()
    enc_iv = bytes(init[40:56])
    dec_key = hashlib.sha256(bytes(init[55:23:-1]) + secret).digest()
    dec_iv = bytes(init[23:7:-1])
    enc = _aes_ctr(enc_key, enc_iv)
    dec = _aes_ctr(dec_key, dec_iv)
    init[56:64] = enc.update(bytes(init))[56:64]
    return bytes(init), enc, dec


def _make_req_pq() -> tuple[bytes, bytes]:
    nonce = os.urandom(16)
    body = struct.pack("<I", REQ_PQ_MULTI) + nonce
    msg_id = int(time.time() * (2**32)) & ~3
    packet = (
        b"\x00" * 8
        + struct.pack("<Q", msg_id)
        + struct.pack("<I", len(body))
        + body
    )
    return nonce, packet


def _frame_secure(data: bytes) -> bytes:
    pad = os.urandom(os.urandom(1)[0] % 4)
    return struct.pack("<I", len(data) + len(pad)) + data + pad


def _parse_res_pq(frame: bytes, expected_nonce: bytes) -> None:
    if len(frame) < 40 or frame[:8] != b"\x00" * 8:
        raise RuntimeError("invalid MTProto response")
    msg_len = struct.unpack("<I", frame[16:20])[0]
    body = frame[20 : 20 + msg_len]
    if len(body) < 20:
        raise RuntimeError("short resPQ body")
    ctor = struct.unpack("<I", body[:4])[0]
    if ctor != RES_PQ:
        raise RuntimeError(f"unexpected constructor 0x{ctor:08x}")
    if body[4:20] != expected_nonce:
        raise RuntimeError("nonce mismatch")


def _tls_wrap(data: bytes, *, send_ccs: bool) -> bytes:
    out = bytearray()
    if send_ccs:
        out += CCS
    out += b"\x17\x03\x03" + struct.pack(">H", len(data)) + data
    return bytes(out)


async def _ping_fake_tls(proxy: MTProtoProxy, timeout: float) -> float:
    if MTProxyFakeTLSClientCodec is None or FakeTLSStreamReader is None:
        raise RuntimeError("TelethonFakeTLS is required for ee proxies")

    secret = _secret_key16(proxy)
    codec = MTProxyFakeTLSClientCodec(proxy.secret[2:] if proxy.is_fake_tls else proxy.secret)
    started = time.perf_counter()

    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy.server, proxy.port),
        timeout=timeout,
    )
    try:
        writer.write(codec.build_new_client_hello_packet())
        await writer.drain()

        ft_reader = FakeTLSStreamReader(reader)
        hello = await asyncio.wait_for(ft_reader.read_server_hello(), timeout=timeout)
        if not codec.verify_server_hello(hello):
            raise RuntimeError("FakeTLS server hello verification failed")

        header, enc, dec = _obfuscated2_handshake(secret)
        writer.write(_tls_wrap(header, send_ccs=True))
        await writer.drain()

        nonce, packet = _make_req_pq()
        framed = enc.update(_frame_secure(packet))
        writer.write(_tls_wrap(framed, send_ccs=False))
        await writer.drain()

        # Prefer FakeTLSStreamReader (already may hold buffered plaintext)
        length_b = await asyncio.wait_for(ft_reader.readexactly(4), timeout=timeout)
        length = struct.unpack("<I", dec.update(length_b))[0]
        if length > 0x80000000:
            length -= 0x80000000
        if length <= 0 or length > 2 * 1024 * 1024:
            raise RuntimeError(f"bad frame length {length}")
        payload = dec.update(
            await asyncio.wait_for(ft_reader.readexactly(length), timeout=timeout)
        )
        _parse_res_pq(payload, nonce)
        return time.perf_counter() - started
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _ping_classic(proxy: MTProtoProxy, timeout: float) -> float:
    """Ping dd / plain secrets over raw TCP with secure obfuscated2."""
    secret = _secret_key16(proxy)
    started = time.perf_counter()
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy.server, proxy.port),
        timeout=timeout,
    )
    try:
        header, enc, dec = _obfuscated2_handshake(secret)
        writer.write(header)
        await writer.drain()

        # Detect immediate close (wrong protocol / dead proxy)
        try:
            await asyncio.wait_for(reader._wait_for_data("proxy"), 0.8)  # type: ignore[attr-defined]
            if reader.at_eof():
                raise RuntimeError("proxy closed after handshake")
        except asyncio.TimeoutError:
            pass

        nonce, packet = _make_req_pq()
        writer.write(enc.update(_frame_secure(packet)))
        await writer.drain()

        length_b = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
        length = struct.unpack("<I", dec.update(length_b))[0]
        if length > 0x80000000:
            length -= 0x80000000
        payload = dec.update(
            await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
        )
        _parse_res_pq(payload, nonce)
        return time.perf_counter() - started
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@dataclass(slots=True)
class PingResult:
    proxy: MTProtoProxy
    latency: float | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.latency is not None


async def ping_proxy(proxy: MTProtoProxy, timeout: float = 8.0) -> PingResult:
    try:
        if proxy.is_fake_tls:
            latency = await _ping_fake_tls(proxy, timeout)
        else:
            latency = await _ping_classic(proxy, timeout)
        return PingResult(proxy=proxy, latency=latency)
    except Exception as exc:
        return PingResult(proxy=proxy, latency=None, error=str(exc) or type(exc).__name__)


async def ping_proxies(
    proxies: list[MTProtoProxy],
    *,
    concurrency: int = 20,
    timeout: float = 8.0,
    on_result=None,
) -> list[PingResult]:
    if not proxies:
        return []

    sem = asyncio.Semaphore(concurrency)
    results: list[PingResult] = []
    lock = asyncio.Lock()
    done = 0
    total = len(proxies)

    async def _one(proxy: MTProtoProxy) -> None:
        nonlocal done
        async with sem:
            result = await ping_proxy(proxy, timeout)
        async with lock:
            done += 1
            results.append(result)
            if on_result:
                on_result(done, total, result)

    await asyncio.gather(*(_one(p) for p in proxies))
    return results


async def find_first_working_proxy(
    proxies: list[MTProtoProxy],
    *,
    timeout: float = 8.0,
    on_result=None,
) -> tuple[MTProtoProxy, float] | None:
    """Ping proxies in order; return the first that responds."""
    total = len(proxies)
    for index, proxy in enumerate(proxies, start=1):
        result = await ping_proxy(proxy, timeout)
        if on_result:
            on_result(index, total, result)
        if result.ok and result.latency is not None:
            return proxy, result.latency
    return None


async def find_fastest_proxy(
    proxies: list[MTProtoProxy],
    *,
    concurrency: int = 20,
    timeout: float = 8.0,
    on_result=None,
) -> tuple[MTProtoProxy, float] | None:
    results = await ping_proxies(
        proxies,
        concurrency=concurrency,
        timeout=timeout,
        on_result=on_result,
    )
    best: tuple[MTProtoProxy, float] | None = None
    for result in results:
        if result.ok and result.latency is not None:
            if best is None or result.latency < best[1]:
                best = (result.proxy, result.latency)
    return best


@dataclass(slots=True)
class ReorganizeStats:
    ok: int
    failed: int
    fastest: tuple[MTProtoProxy, float] | None


async def check_and_reorganize(
    catalog,
    *,
    concurrency: int = 20,
    timeout: float = 8.0,
    on_result=None,
    respect_backoff: bool = True,
    limit: int | None = None,
) -> ReorganizeStats:
    """Ping the adaptive probe queue and update health stats + working/failed."""
    if hasattr(catalog, "probe_queue"):
        proxies = catalog.probe_queue(
            respect_backoff=respect_backoff, limit=limit
        )
    else:
        proxies = catalog.all_unique()
    if not proxies:
        return ReorganizeStats(0, 0, None)

    results = await ping_proxies(
        proxies,
        concurrency=concurrency,
        timeout=timeout,
        on_result=on_result,
    )

    if hasattr(catalog, "apply_ping_results"):
        catalog.apply_ping_results(results)
        ok_ranked = [
            (r.proxy, r.latency)
            for r in results
            if r.ok and r.latency is not None
        ]
        ok_ranked.sort(key=lambda item: item[1])
        failed_n = sum(1 for r in results if not r.ok or r.latency is None)
        fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
        return ReorganizeStats(len(ok_ranked), failed_n, fastest)

    ok_ranked = []
    failed = []
    for result in results:
        if result.ok and result.latency is not None:
            ok_ranked.append((result.proxy, result.latency))
        else:
            failed.append(result.proxy)
    ok_ranked.sort(key=lambda item: item[1])
    ok = [proxy for proxy, _ in ok_ranked]
    catalog.reorganize(ok, failed)
    fastest = (ok_ranked[0][0], ok_ranked[0][1]) if ok_ranked else None
    return ReorganizeStats(len(ok), len(failed), fastest)


def patch_telethon_faketls() -> None:
    """Make TelethonFakeTLS send ChangeCipherSpec before first app-data (required)."""
    try:
        from TelethonFakeTLS.FakeTLS.TLSInOut import FakeTLSStreamWriter
    except ImportError:
        return

    if getattr(FakeTLSStreamWriter.write, "_mtproto_ccs_patched", False):
        return

    _orig_write = FakeTLSStreamWriter.write
    # FakeTLSStreamWriter uses __slots__ = (), so no instance attrs — track by id.
    sent_ccs: set[int] = set()

    def write(self, data, extra=None):  # type: ignore[no-untyped-def]
        if extra is None:
            extra = {}
        oid = id(self)
        if oid not in sent_ccs:
            self.upstream.write(CCS)
            sent_ccs.add(oid)
        return _orig_write(self, data, extra)

    async def wait_closed(self) -> None:  # type: ignore[no-untyped-def]
        upstream = getattr(self, "upstream", None)
        if upstream is not None and hasattr(upstream, "wait_closed"):
            await upstream.wait_closed()

    write._mtproto_ccs_patched = True  # type: ignore[attr-defined]
    FakeTLSStreamWriter.write = write  # type: ignore[method-assign]
    if not hasattr(FakeTLSStreamWriter, "wait_closed"):
        FakeTLSStreamWriter.wait_closed = wait_closed  # type: ignore[attr-defined]
