"""In-process LRU cache for clustering results.

Practical cost control: re-clustering an identical file with the same column
mapping is pure wasted CPU — the pipeline is deterministic (fixed
``random_state``), so the same input always yields the same output. Caching the
assembled response body means a repeat request (a user re-opening a dataset, a
retry after a network blip, two teammates clustering the same export) returns
instantly and spends nothing.

Design notes:
- Keyed on a SHA-256 of the raw file bytes plus the effective column mapping and
  the pipeline config that affect the result. Different mappings/configs of the
  same file are cached separately.
- Bounded by entry count AND total bytes, so a run of large results can't grow
  memory without limit. Least-recently-used eviction.
- Single-process and thread-safe. This matches the single-uvicorn-worker Railway
  deployment; with multiple replicas each keeps its own cache (still correct,
  just a lower hit rate) — move to Redis if you scale out and want a shared one.
- ``maxsize=0`` (env CLUSTER_CACHE_SIZE=0) disables caching entirely.
"""
from __future__ import annotations

import hashlib
import json
import sys
import threading
from collections import OrderedDict
from typing import Any, Dict, Optional


def result_cache_key(
    file_bytes: bytes,
    column_mapping: Dict[str, Optional[str]],
    config_fingerprint: Dict[str, Any],
) -> str:
    """Stable key for a (file, mapping, config) triple."""
    h = hashlib.sha256()
    h.update(file_bytes)
    # Sort keys so mapping/config order never changes the digest.
    h.update(json.dumps(column_mapping, sort_keys=True, default=str).encode("utf-8"))
    h.update(json.dumps(config_fingerprint, sort_keys=True, default=str).encode("utf-8"))
    return h.hexdigest()


class ResultCache:
    """Thread-safe LRU cache bounded by entry count and total payload bytes."""

    def __init__(self, max_entries: int, max_total_bytes: int):
        self.max_entries = int(max_entries)
        self.max_total_bytes = int(max_total_bytes)
        self._store: "OrderedDict[str, tuple[Dict[str, Any], int]]" = OrderedDict()
        self._total_bytes = 0
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    @property
    def enabled(self) -> bool:
        return self.max_entries > 0

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        with self._lock:
            item = self._store.get(key)
            if item is None:
                self.misses += 1
                return None
            self._store.move_to_end(key)  # mark most-recently-used
            self.hits += 1
            return item[0]

    def put(self, key: str, value: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            size = _approx_size(value)
        except Exception:
            size = 0
        # Never cache a single item larger than the whole budget.
        if self.max_total_bytes > 0 and size > self.max_total_bytes:
            return
        with self._lock:
            if key in self._store:
                old_size = self._store[key][1]
                self._total_bytes -= old_size
                del self._store[key]
            self._store[key] = (value, size)
            self._total_bytes += size
            self._evict_locked()

    def _evict_locked(self) -> None:
        while self._store and (
            len(self._store) > self.max_entries
            or (self.max_total_bytes > 0 and self._total_bytes > self.max_total_bytes)
        ):
            _, (_, evicted_size) = self._store.popitem(last=False)  # LRU end
            self._total_bytes -= evicted_size

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "entries": len(self._store),
                "total_bytes": self._total_bytes,
                "hits": self.hits,
                "misses": self.misses,
            }


def _approx_size(value: Any) -> int:
    """Cheap byte estimate for budgeting (JSON length is a good proxy)."""
    try:
        return len(json.dumps(value, default=str).encode("utf-8"))
    except Exception:
        return sys.getsizeof(value)
