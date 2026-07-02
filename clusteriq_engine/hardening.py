"""Request-hardening primitives for the public API.

Everything here is dependency-free and single-process. That matches the
deployment (one uvicorn worker on Railway); if the service ever scales to
multiple replicas, per-IP limits become per-replica and should move to a
shared store (e.g. Redis) — noted in README.
"""
from __future__ import annotations

import math
import threading
import time
from contextlib import contextmanager
from typing import Callable, Dict, Optional, Tuple

from .errors import UserError


class ServerBusy(Exception):
    """Raised when all clustering job slots are occupied."""


class JobSlots:
    """Bound the number of CPU-heavy jobs running at once.

    The pipeline runs in a threadpool so the event loop stays responsive,
    but unbounded threads would still let two big uploads starve the CPU.
    Beyond `max_jobs`, callers get an immediate ServerBusy instead of
    queueing forever.
    """

    def __init__(self, max_jobs: int):
        self.max_jobs = int(max_jobs)
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active(self) -> int:
        return self._active

    @contextmanager
    def acquire(self):
        if self.max_jobs > 0:
            with self._lock:
                if self._active >= self.max_jobs:
                    raise ServerBusy()
                self._active += 1
        try:
            yield
        finally:
            if self.max_jobs > 0:
                with self._lock:
                    self._active -= 1


class RateLimiter:
    """Per-key token bucket: `per_minute` requests sustained, same burst cap.

    `per_minute <= 0` disables limiting. The clock is injectable for tests.
    """

    _PRUNE_THRESHOLD = 5000
    _IDLE_SECONDS = 600.0

    def __init__(self, per_minute: int, time_func: Callable[[], float] = time.monotonic):
        self.capacity = float(per_minute)
        self.rate = self.capacity / 60.0
        self._time = time_func
        self._buckets: Dict[str, Tuple[float, float]] = {}  # key -> (tokens, last_ts)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        if self.capacity <= 0:
            return True
        now = self._time()
        with self._lock:
            tokens, last = self._buckets.get(key, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - last) * self.rate)
            allowed = tokens >= 1.0
            self._buckets[key] = (tokens - 1.0 if allowed else tokens, now)
            if len(self._buckets) > self._PRUNE_THRESHOLD:
                cutoff = now - self._IDLE_SECONDS
                self._buckets = {k: v for k, v in self._buckets.items() if v[1] >= cutoff}
        return allowed

    def retry_after(self, key: str) -> int:
        """Seconds until the key earns its next token (>= 1, for Retry-After)."""
        if self.capacity <= 0 or self.rate <= 0:
            return 1
        with self._lock:
            tokens, _ = self._buckets.get(key, (self.capacity, self._time()))
        deficit = max(0.0, 1.0 - tokens)
        return max(1, int(math.ceil(deficit / self.rate)))


def client_ip(request, trust_proxy_headers: bool = True) -> str:
    """Best-effort client IP for rate-limit keying.

    Behind Railway's edge proxy the socket peer is the proxy, and the real
    client is the RIGHTMOST entry of X-Forwarded-For (the one appended by
    the trusted proxy; leftmost entries are client-supplied and spoofable).
    With `trust_proxy_headers` off, only the socket address is used.
    """
    if trust_proxy_headers:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            last = forwarded.split(",")[-1].strip()
            if last:
                return last
        real_ip = request.headers.get("x-real-ip", "")
        if real_ip.strip():
            return real_ip.strip()
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


async def read_upload_limited(file, max_bytes: int, declared_length: Optional[str] = None) -> bytes:
    """Read an UploadFile in chunks, aborting as soon as the limit is passed.

    The previous `await file.read()` buffered the entire body before any
    size check ran, so a multi-GB POST was fully consumed first. Now an
    over-declared Content-Length is rejected before reading at all, and an
    undeclared oversized stream is cut off at the first excess chunk.
    """
    limit_mb = max_bytes // 1_048_576
    if declared_length:
        try:
            declared = int(declared_length)
        except ValueError:
            declared = None  # malformed header: rely on chunked enforcement
        if declared is not None and declared > max_bytes + 4096:  # multipart framing slack
            raise UserError(
                "FILE_TOO_LARGE",
                f"The upload is larger than the {limit_mb} MB limit.",
            )
    chunks = []
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise UserError(
                "FILE_TOO_LARGE",
                f"The upload is larger than the {limit_mb} MB limit.",
            )
        chunks.append(chunk)
    return b"".join(chunks)
