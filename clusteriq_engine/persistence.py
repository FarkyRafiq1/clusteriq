"""Direct-to-Supabase persistence for clustering results.

Why this exists: results used to travel engine -> edge function -> database.
The edge function (a free-tier Deno isolate with ~2s of CPU) had to parse an
~18 MB result body, insert ~20k+ cluster rows (with a `.select()` echoing them
all back), then ~26k keyword rows in ~53 sequential calls. Supabase killed it
mid-persist with WORKER_LIMIT ("not enough compute resources") — silently on
the old sync path, loudly on v1.4.0. The engine, by contrast, has already
proven it can chew through this workload (full clustering of the same file:
353 MB / 31 s). So the engine now writes results to the database itself, and
the edge function shrinks to what an isolate is actually sized for: tiny
orchestration calls.

This is a faithful port of the edge function's `persistResults` — same tables,
same column names, same fallbacks — with three deliberate improvements:
1. **Client-side UUIDs** for cluster rows, so inserts use `Prefer:
   return=minimal` instead of echoing thousands of rows back.
2. **Chunked inserts for clusters too** (the TS version chunked only keywords).
3. **Idempotent re-runs**: existing clusters for the upload are deleted before
   inserting, so a retry after a partial failure can't duplicate data.
   (Assumes the `cluster_keywords.cluster_id` FK cascades on delete — the
   standard Lovable/Supabase schema shape. If it doesn't, orphaned keyword
   rows from a *previous partial* run may remain; the legacy edge flow had the
   same exposure.)

Configuration (Railway -> Variables):
    SUPABASE_URL               e.g. https://<project-ref>.supabase.co
    SUPABASE_SERVICE_ROLE_KEY  the service-role secret (server-side only!)

If either is unset, `configured()` is False and the engine falls back to
v1.4.0 behaviour (full result body served to the edge function, which then
persists it — the legacy path). That makes deploy order safe.
"""
from __future__ import annotations

import logging
import math
import os
import uuid
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("clusteriq.persistence")

# Rows per insert request. 500 keeps request bodies ~300-500 KB — comfortable
# for PostgREST, and ~100 sequential calls total for a 25k-keyword file takes
# single-digit seconds from the worker thread (which has no CPU ceiling).
CHUNK = 500

_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


class PersistError(Exception):
    """A persistence step failed. Message carries enough context to debug."""


def configured() -> bool:
    return bool(os.getenv("SUPABASE_URL")) and bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY"))


def _num(v: Any) -> Optional[float]:
    """Port of the TS numOrNull: numbers pass through, NaN/None/non-numeric -> None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return None if (isinstance(v, float) and math.isnan(v)) else v
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


class SupabasePersistence:
    """Thin PostgREST client scoped to exactly what result persistence needs."""

    def __init__(self, base_url: Optional[str] = None, service_key: Optional[str] = None,
                 client: Optional[httpx.Client] = None):
        self.base_url = (base_url or os.getenv("SUPABASE_URL", "")).rstrip("/")
        self.key = service_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
        if not self.base_url or not self.key:
            raise PersistError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured")
        self._client = client or httpx.Client(timeout=_TIMEOUT)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    # ---- HTTP helpers ---------------------------------------------------- #

    def _headers(self) -> Dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def _request(self, method: str, path: str, **kw) -> httpx.Response:
        url = f"{self.base_url}/rest/v1/{path}"
        resp = self._client.request(method, url, headers=self._headers(), **kw)
        if resp.status_code >= 300:
            raise PersistError(
                f"{method} {path} -> {resp.status_code}: {resp.text[:300]}"
            )
        return resp

    # ---- The persist operation ------------------------------------------- #

    def persist_results(
        self,
        result: Dict[str, Any],
        upload_id: str,
        project_id: str,
        job_id: Optional[str] = None,
    ) -> int:
        """Write clusters + cluster_keywords, mark the upload complete.

        Returns the number of cluster rows written. Raises PersistError on any
        failed step (the caller marks the job failed; a re-submit is a cache
        hit on the engine, so retrying costs almost nothing).
        """
        clusters: List[Dict[str, Any]] = list(result.get("clusters") or [])
        rows: List[Dict[str, Any]] = list(result.get("rows") or [])

        # Idempotency: a retry after a partial failure must not duplicate.
        self._request("DELETE", f"clusters?upload_id=eq.{upload_id}")

        # ---- clusters ---- #
        clusters_payload: List[Dict[str, Any]] = []
        id_by_backend_index: Dict[int, str] = {}
        for i, c in enumerate(clusters):
            row_uuid = str(uuid.uuid4())
            backend_id = c.get("cluster_id") if isinstance(c.get("cluster_id"), int) else i
            id_by_backend_index[backend_id] = row_uuid
            clusters_payload.append({
                "id": row_uuid,  # client-side id: no `.select()` echo needed
                "upload_id": upload_id,
                "project_id": project_id,
                "job_id": job_id,
                "cluster_index": backend_id,
                "cluster_slug": c.get("cluster_slug") or c.get("topic_label") or f"cluster-{i}",
                "topic_label": c.get("topic_label") or f"Cluster {i + 1}",
                "canonical_keyword": c.get("canonical_keyword") or "",
                "intent": c.get("intent") or "",
                "page_type": c.get("page_type") or "",
                "is_clustered": True if c.get("is_clustered") is None else bool(c.get("is_clustered")),
                "keyword_count": _num(c.get("keyword_count")) or 0,
                "total_volume": _num(c.get("total_volume")) or 0,
                "avg_difficulty": _num(c.get("avg_difficulty")),
                "avg_position": _num(c.get("avg_rank", c.get("avg_position"))),
                "opportunity_score": _num(c.get("opportunity_score")) or 0,
                "quality_score": _num(c.get("cluster_quality", c.get("quality_score"))) or 0,
                "quality_label": c.get("quality_label") or "Fair",
                "keywords": c.get("keywords") or [],
                "urls": c.get("urls") or [],
            })

        for start in range(0, len(clusters_payload), CHUNK):
            self._request("POST", "clusters", json=clusters_payload[start:start + CHUNK])

        # ---- cluster_keywords ---- #
        # Prefer the per-row `rows` array (the engine always provides it);
        # fall back to cluster.keywords[] for fidelity with the TS original.
        keywords_payload: List[Dict[str, Any]] = []
        if rows:
            for r in rows:
                backend_id = r.get("cluster_id") if isinstance(r.get("cluster_id"), int) else -1
                cid = id_by_backend_index.get(backend_id)
                if cid is None:
                    continue
                keywords_payload.append({
                    "cluster_id": cid,
                    "keyword": r.get("keyword") or "",
                    "volume": _num(r.get("volume")),
                    "difficulty": _num(r.get("difficulty")),
                    "position": _num(r.get("position")),
                    "url": r.get("url") or "",
                    "intent": r.get("intent"),
                    "page_type": r.get("page_type"),
                    "canonical_keyword": r.get("canonical_keyword"),
                    "topic_label": r.get("topic_label"),
                    "cluster_quality": _num(r.get("cluster_quality")),
                    "opportunity_score": _num(r.get("opportunity_score")),
                    "is_clustered": True if r.get("is_clustered") is None else bool(r.get("is_clustered")),
                    "is_canonical": r.get("canonical_keyword") == r.get("keyword"),
                })
        else:
            for i, c in enumerate(clusters):
                backend_id = c.get("cluster_id") if isinstance(c.get("cluster_id"), int) else i
                cid = id_by_backend_index.get(backend_id)
                if cid is None:
                    continue
                for kw in (c.get("keywords") or []):
                    if not isinstance(kw, dict):
                        continue
                    keywords_payload.append({
                        "cluster_id": cid,
                        "keyword": kw.get("keyword") or "",
                        "volume": _num(kw.get("volume")),
                        "difficulty": _num(kw.get("difficulty")),
                        "position": _num(kw.get("position", kw.get("rank"))),
                        "url": kw.get("url") or "",
                        "is_canonical": bool(kw.get("is_canonical")),
                    })

        for start in range(0, len(keywords_payload), CHUNK):
            self._request("POST", "cluster_keywords", json=keywords_payload[start:start + CHUNK])

        # ---- upload status ---- #
        self._request("PATCH", f"uploads?id=eq.{upload_id}", json={"status": "complete"})

        logger.info(
            "persisted %d clusters / %d keywords for upload %s",
            len(clusters_payload), len(keywords_payload), upload_id,
        )
        return len(clusters_payload)


def persist_results(
    result: Dict[str, Any],
    upload_id: str,
    project_id: str,
    job_id: Optional[str] = None,
    client: Optional[httpx.Client] = None,
    base_url: Optional[str] = None,
    service_key: Optional[str] = None,
) -> int:
    """Module-level convenience wrapper (also the seam tests use).

    `base_url`/`service_key`, when given, take precedence over the env vars —
    this is the per-request credential path used with Lovable Cloud, where
    the service-role key is only ever available inside the edge function.
    SECURITY: callers must never log these values.
    """
    p = SupabasePersistence(base_url=base_url, service_key=service_key, client=client)
    try:
        return p.persist_results(result, upload_id, project_id, job_id)
    finally:
        p.close()
