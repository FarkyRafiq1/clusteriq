from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from .cache import ResultCache, result_cache_key
from .errors import UserError
from .hardening import JobSlots, RateLimiter, ServerBusy, client_ip, read_upload_limited
from .ingest import MAX_UPLOAD_BYTES, resolve_columns
from . import persistence
from .jobs import CanceledError, JobStatus, JobStore
from .pipeline import ClusterPipeline
from .schemas import PipelineConfig
from .utils import df_records_json_safe

logger = logging.getLogger("clusteriq")

# --------------------------------------------------------------------- #
# Environment-driven settings (set these in Railway -> Variables)
# --------------------------------------------------------------------- #
# ALLOWED_ORIGINS         comma-separated frontend origins; "*" = any (dev only)
# RATE_LIMIT_PER_MINUTE   per-IP requests/min across /preview + /cluster; 0 = off
#                         NOTE: when the frontend calls this backend server-to-
#                         server (via the Supabase edge function) EVERY request
#                         shares ONE egress IP, so this is effectively a GLOBAL
#                         limit. The default is set high for that reason; drop it
#                         only if clients hit the backend directly per-user.
# MAX_CONCURRENT_JOBS     simultaneous clustering jobs; 0 = unlimited
# MAX_UPLOAD_MB           hard upload cap; keep in sync with the storage bucket
# HEAVY_JOB_ROW_LIMIT     rows above which /cluster is refused synchronously with
#                         a clear 413 (protects CPU/time budget). 0 = allow up to
#                         the pipeline's absolute MAX_ROWS.
# CLUSTER_CACHE_SIZE      cached result entries (identical re-runs are free); 0=off
# CLUSTER_CACHE_MB        total cache budget in MB
# TRUST_PROXY_HEADERS     "1" behind Railway's proxy (default); "0" if exposed directly
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
] or ["*"]
# High default because all traffic arrives from one edge-function IP (see note).
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
HEAVY_JOB_ROW_LIMIT = int(os.getenv("HEAVY_JOB_ROW_LIMIT", "50000"))
_CACHE_ENTRIES = int(os.getenv("CLUSTER_CACHE_SIZE", "64"))
_CACHE_BYTES = int(os.getenv("CLUSTER_CACHE_MB", "64")) * 1024 * 1024
TRUST_PROXY_HEADERS = os.getenv("TRUST_PROXY_HEADERS", "1") != "0"


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Startup: warn once if CORS is wide open (harmless for the server-to-server
    # call path, but worth flagging before a public launch).
    if "*" in ALLOWED_ORIGINS:
        logger.warning(
            "CORS is open to all origins. Set ALLOWED_ORIGINS to your frontend "
            "origin (e.g. https://yourapp.lovable.app) before public launch."
        )
    logger.info(
        "ClusterIQ Engine up: rate_limit/min=%s max_jobs=%s cache_entries=%s heavy_row_limit=%s",
        RATE_LIMIT_PER_MINUTE, MAX_CONCURRENT_JOBS, _CACHE_ENTRIES, HEAVY_JOB_ROW_LIMIT,
    )
    yield
    # Shutdown: nothing to clean up (in-process state is GC'd).


app = FastAPI(title="ClusterIQ Engine", version="1.4.2", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

_rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)
_job_slots = JobSlots(MAX_CONCURRENT_JOBS)
_result_cache = ResultCache(_CACHE_ENTRIES, _CACHE_BYTES)

# ---- Async job store ---------------------------------------------------- #
# JOB_STORE_MAX          Max concurrent tracked jobs (mostly terminal, TTL-swept).
# JOB_TTL_SECONDS        How long completed/failed job records survive.
# JOB_WORKER_THREADS     Concurrent background workers. Kept in lockstep with
#                        MAX_CONCURRENT_JOBS by default so admission control
#                        (job slots) remains the single throttle.
_JOB_STORE_MAX = int(os.getenv("JOB_STORE_MAX", "1000"))
_JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "3600"))
_JOB_WORKER_THREADS = int(os.getenv("JOB_WORKER_THREADS", str(max(1, MAX_CONCURRENT_JOBS))))

_job_store = JobStore(max_jobs=_JOB_STORE_MAX, ttl_seconds=_JOB_TTL_SECONDS)
# Bounded worker pool: separate from FastAPI's default threadpool so background
# clustering can't starve the request-serving path (uploads, polls, health).
_job_executor_semaphore = threading.BoundedSemaphore(_JOB_WORKER_THREADS)

# Effective upload cap: env override (MB) or the pipeline's built-in default.
# Keep this aligned with the storage bucket limit on the frontend (20 MB today)
# so a file can't pass here and then fail the bucket upload.
_env_upload_mb = os.getenv("MAX_UPLOAD_MB")
EFFECTIVE_MAX_UPLOAD_BYTES = (
    int(_env_upload_mb) * 1024 * 1024 if _env_upload_mb else MAX_UPLOAD_BYTES
)


def _build_config(
    keyword_column: str,
    volume_column: Optional[str],
    difficulty_column: Optional[str],
    rank_column: Optional[str],
    url_column: Optional[str],
) -> PipelineConfig:
    return PipelineConfig(
        keyword_column=keyword_column,
        volume_column=volume_column,
        difficulty_column=difficulty_column,
        rank_column=rank_column,
        url_column=url_column,
    )


def _user_error_response(exc: UserError) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": exc.to_payload()})


def _internal_error_response(exc: Exception) -> HTTPException:
    reference = uuid.uuid4().hex[:12]
    logger.exception("Unhandled error [ref=%s]", reference)
    return HTTPException(
        status_code=500,
        detail={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": f"Something went wrong on our side (ref {reference}).",
            }
        },
    )


def _enforce_rate_limit(request: Request) -> None:
    key = client_ip(request, TRUST_PROXY_HEADERS)
    if not _rate_limiter.allow(key):
        raise HTTPException(
            status_code=429,
            detail={
                "error": {
                    "code": "RATE_LIMITED",
                    "message": "Too many requests. Please slow down and retry.",
                }
            },
            headers={"Retry-After": str(_rate_limiter.retry_after(key))},
        )


def _busy_response() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "error": {
                "code": "SERVER_BUSY",
                "message": "All clustering slots are in use. Retry shortly.",
            }
        },
        headers={"Retry-After": "30"},
    )


def _too_many_rows_response(row_count: int, limit: int) -> HTTPException:
    return HTTPException(
        status_code=413,
        detail={
            "error": {
                "code": "TOO_MANY_ROWS",
                "message": (
                    f"This file has {row_count:,} rows; synchronous clustering is "
                    f"capped at {limit:,} to keep response times fast. Split the "
                    f"file into smaller batches, or contact support for large-batch "
                    f"processing."
                ),
                "row_count": row_count,
                "limit": limit,
            }
        },
    )


def build_cluster_response(pipeline: ClusterPipeline, df: pd.DataFrame) -> Dict[str, Any]:
    """Run the pipeline and assemble a strictly-JSON-safe response body.

    Every DataFrame passes through df_records_json_safe so a NaN can never
    reach json.dumps (bare NaN literals are invalid JSON: Python tolerates
    them, browsers' JSON.parse does not).
    """
    result = pipeline.run(df)
    return {
        "summary": result["summary"],
        "column_mapping": result["column_mapping"],
        "columns_detected": [str(c) for c in df.columns],
        "clusters": df_records_json_safe(result["clusters"]),
        "rows": df_records_json_safe(pipeline.export_rows(result)),
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    # Lightweight liveness + at-a-glance runtime stats (safe to expose: no data).
    return {
        "status": "ok",
        "version": app.version,
        "active_jobs": _job_slots.active,
        "max_jobs": _job_slots.max_jobs,
        "cache": _result_cache.stats(),
        "jobs": _job_store.stats(),
        "persistence": persistence.configured(),
    }


@app.post("/preview")
async def preview_file(
    request: Request,
    file: UploadFile = File(...),
    keyword_column: str = Form("keyword"),
    volume_column: Optional[str] = Form("volume"),
    difficulty_column: Optional[str] = Form("difficulty"),
    rank_column: Optional[str] = Form("position"),
    url_column: Optional[str] = Form("url"),
) -> JSONResponse:
    """Parse the file and report detected columns + proposed mapping.

    Powers a "map columns" screen: the frontend can show the mapping and
    sample rows before kicking off the full clustering job.
    """
    _enforce_rate_limit(request)
    try:
        payload = await read_upload_limited(
            file, EFFECTIVE_MAX_UPLOAD_BYTES, request.headers.get("content-length")
        )
        pipeline = ClusterPipeline(
            _build_config(keyword_column, volume_column, difficulty_column, rank_column, url_column)
        )
        df = await run_in_threadpool(pipeline.read_table, payload, file.filename or "upload")
        mapping = resolve_columns(
            df,
            {
                "keyword": keyword_column,
                "volume": volume_column,
                "difficulty": difficulty_column,
                "position": rank_column,
                "url": url_column,
            },
        )
        return JSONResponse(
            {
                "columns_detected": [str(c) for c in df.columns],
                "column_mapping": mapping,
                "row_count": int(len(df)),
                "sample_rows": df_records_json_safe(df.head(20)),
            }
        )
    except HTTPException:
        raise
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc


@app.post("/cluster")
async def cluster_file(
    request: Request,
    file: UploadFile = File(...),
    keyword_column: str = Form("keyword"),
    volume_column: Optional[str] = Form("volume"),
    difficulty_column: Optional[str] = Form("difficulty"),
    rank_column: Optional[str] = Form("position"),
    url_column: Optional[str] = Form("url"),
    upload_id: Optional[str] = Form(None),
    project_id: Optional[str] = Form(None),
    # Per-request persistence credentials (v1.4.2). Lovable Cloud never
    # exposes the service-role key to humans — only to edge functions — so
    # the edge function hands it to the engine with each submit. SECURITY:
    # these must never be logged, stored in job records, or echoed back.
    supabase_url: Optional[str] = Form(None),
    supabase_key: Optional[str] = Form(None),
) -> JSONResponse:
    """Submit a clustering job for asynchronous processing.

    Always returns immediately with `{"job_id": "...", "status": "queued"}`.
    The actual pipeline runs in a background thread; poll `GET /jobs/{id}`
    for status and `GET /jobs/{id}/result` for the completed body.

    Rationale: synchronous responses were killed by the Supabase edge function's
    150s wall clock even when clustering succeeded on the engine — the result
    was computed and then thrown away because nothing was still listening.
    Making submission async decouples "clustering finished" from "HTTP request
    still alive," so no work is lost to network/proxy timeouts.

    Note: job state lives in-process. On Railway container restart, in-flight
    jobs are lost — the poll endpoint returns 404 for their ids, which the
    edge function surfaces to the frontend as a clear failure the user can
    retry (rather than a spinner that hangs forever).
    """
    # When this function is invoked directly (tests, scripts) instead of via
    # FastAPI's request pipeline, unfilled Form(...) defaults arrive as the
    # sentinel objects themselves — which are truthy. Normalise to real
    # strings-or-None before any of them participate in logic.
    upload_id = upload_id if isinstance(upload_id, str) and upload_id else None
    project_id = project_id if isinstance(project_id, str) and project_id else None
    supabase_url = supabase_url if isinstance(supabase_url, str) and supabase_url else None
    supabase_key = supabase_key if isinstance(supabase_key, str) and supabase_key else None

    _enforce_rate_limit(request)
    try:
        # Read the upload BEFORE returning: if the file is malformed or over the
        # size cap, we surface the error synchronously (400/413) rather than
        # queuing a job that will fail two seconds later. Upload read is fast
        # and cheap; the expensive work is downstream.
        payload = await read_upload_limited(
            file, EFFECTIVE_MAX_UPLOAD_BYTES, request.headers.get("content-length")
        )
        config = _build_config(
            keyword_column, volume_column, difficulty_column, rank_column, url_column
        )
        column_mapping = {
            "keyword": keyword_column,
            "volume": volume_column,
            "difficulty": difficulty_column,
            "position": rank_column,
            "url": url_column,
        }

        # Cache lookup happens synchronously: a repeat run is free, so it would
        # be silly to make the caller poll for something we can hand back now.
        # We still route the response through the async contract (create a
        # completed job, return job_id) so the frontend's polling code path is
        # exercised uniformly — no special sync case for the edge function.
        cache_key = result_cache_key(
            payload,
            column_mapping,
            {"engine_version": app.version, "backend": config.semantic_backend},
        )
        can_persist = bool(
            upload_id and project_id
            and ((supabase_url and supabase_key) or persistence.configured())
        )

        cached = _result_cache.get(cache_key)
        if cached is not None:
            job = _job_store.create()
            cached_body = {**cached, "cached": True}
            if can_persist:
                # Recovery path: a repeat submit after a failed persist must
                # persist again. Cheap — computation is skipped entirely.
                loop = asyncio.get_running_loop()
                loop.run_in_executor(
                    None, _run_clustering_job,
                    job.id, payload, file.filename or "upload", config,
                    column_mapping, cache_key, upload_id, project_id,
                    supabase_url, supabase_key, cached_body,
                )
            else:
                _job_store.mark_processing(job.id, stage="cache")
                _job_store.mark_completed(job.id, cached_body)
            logger.info("cache hit — job %s", job.id)
            return JSONResponse(
                {"job_id": job.id, "status": "queued", "cached": True},
                status_code=202,
            )

        # Create the job record BEFORE launching the worker so a poll racing
        # the response can always find the id.
        job = _job_store.create()
        filename = file.filename or "upload"

        # Fire-and-forget the background worker. We deliberately don't use
        # BackgroundTasks here — it runs after the response is sent, which is
        # fine, but tying the worker to the request lifecycle makes cleanup
        # semantics fuzzy. A plain thread with the store as its comms channel
        # is simpler to reason about and to test.
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            _run_clustering_job,
            job.id, payload, filename, config, column_mapping, cache_key,
            upload_id, project_id, supabase_url, supabase_key, None,
        )

        return JSONResponse(
            {"job_id": job.id, "status": "queued"},
            status_code=202,
        )
    except HTTPException:
        raise
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc


def _run_clustering_job(
    job_id: str,
    payload: bytes,
    filename: str,
    config: PipelineConfig,
    column_mapping: Dict[str, Optional[str]],
    cache_key: str,
    upload_id: Optional[str] = None,
    project_id: Optional[str] = None,
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
    cached_body: Optional[Dict[str, Any]] = None,
) -> None:
    """Runs in a background thread. Marshals the pipeline through the JobStore.

    Cooperative cancellation: we check `is_cancel_requested` between stages.
    Once we've entered the CPU-bound C code (SVD, k-NN) we can't interrupt —
    this is the honest semantic. Users see "canceled" as soon as the current
    stage finishes.
    """
    # Bounded worker slot: prevents unbounded concurrency separately from the
    # request-serving pool. Blocks briefly if we're at capacity — acceptable,
    # since the job is already queued from the caller's perspective.
    with _job_executor_semaphore:
        pipeline = ClusterPipeline(config)
        try:
            if _job_store.is_cancel_requested(job_id):
                raise CanceledError()

            if cached_body is not None:
                # Cache hit with a persistence context: skip computation,
                # persist the cached result (recovery after a failed persist).
                _job_store.mark_processing(job_id, stage="cache")
                _finish_job(job_id, dict(cached_body), upload_id, project_id,
                            supabase_url, supabase_key)
                return

            _job_store.mark_processing(job_id, stage="reading")

            df = pipeline.read_table(payload, filename)
            # Fast-fail oversized jobs BEFORE the expensive vectorize/
            # cluster stages, so a huge file can't hog CPU forever.
            if HEAVY_JOB_ROW_LIMIT > 0 and len(df) > HEAVY_JOB_ROW_LIMIT:
                _job_store.mark_failed(
                    job_id,
                    code="TOO_MANY_ROWS",
                    message=(
                        f"This file has {len(df):,} rows; synchronous "
                        f"clustering is capped at {HEAVY_JOB_ROW_LIMIT:,} "
                        f"to keep response times fast. Split the file "
                        f"into smaller batches, or contact support for "
                        f"large-batch processing."
                    ),
                )
                return

            _job_store.update_progress(job_id, 25, "vectorizing")
            if _job_store.is_cancel_requested(job_id):
                raise CanceledError()

            started = time.monotonic()
            body = build_cluster_response(pipeline, df)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            body["timing_ms"] = elapsed_ms
            body["cached"] = False

            if _job_store.is_cancel_requested(job_id):
                raise CanceledError()

            _result_cache.put(cache_key, body)
            _finish_job(job_id, body, upload_id, project_id,
                        supabase_url, supabase_key)
            logger.info(
                "job %s clustered %d rows -> %d clusters in %d ms",
                job_id, len(df), len(body.get("clusters", [])), elapsed_ms,
            )
        except persistence.PersistError as exc:
            _job_store.mark_failed(
                job_id,
                code="PERSIST_FAILED",
                message=f"Clustering succeeded but saving results failed: {exc}. "
                        f"Re-submit the file to retry (the result is cached).",
            )
            logger.exception("job %s persist failed", job_id)
        except CanceledError:
            _job_store.mark_failed(job_id, code="CANCELED", message="Canceled by user")
            logger.info("job %s canceled", job_id)
        except UserError as exc:
            _job_store.mark_failed(job_id, code=exc.code, message=str(exc))
            logger.info("job %s failed with user error %s: %s", job_id, exc.code, exc)
        except Exception as exc:  # noqa: BLE001 - worker boundary
            logger.exception("job %s failed unexpectedly", job_id)
            _job_store.mark_failed(job_id, code="INTERNAL_ERROR", message=str(exc) or "Clustering failed")


def _finish_job(
    job_id: str,
    body: Dict[str, Any],
    upload_id: Optional[str],
    project_id: Optional[str],
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
) -> None:
    """Complete a job: persist directly to Supabase when configured and a
    persistence context was supplied; otherwise keep v1.4.0 behaviour (full
    body stored, consume-once, edge function persists it).

    When the engine persists, the stored result is a compact summary — the
    ~18 MB body never travels through the edge-function isolate again, which
    is the whole point of v1.4.1.
    """
    has_request_creds = bool(supabase_url and supabase_key)
    if upload_id and project_id and (has_request_creds or persistence.configured()):
        _job_store.update_progress(job_id, 85, "persisting")
        # Per-request credentials (edge-function supplied) take precedence:
        # they are always current, so a platform-side key rotation can never
        # strand the engine with a stale copy. NEVER log these values.
        cluster_count = persistence.persist_results(
            body, upload_id, project_id, job_id,
            base_url=supabase_url, service_key=supabase_key,
        )  # raises PersistError on failure -> handled by the worker
        compact = {
            "success": True,
            "persisted": True,
            "cluster_count": cluster_count,
            "row_count": len(body.get("rows") or []),
            "summary": body.get("summary"),
            "timing_ms": body.get("timing_ms"),
            "cached": bool(body.get("cached")),
        }
        _job_store.mark_completed(job_id, compact)
    else:
        _job_store.mark_completed(job_id, body)


# =========================================================================== #
# Job status / result / cancel — the endpoints the cluster-proxy edge function
# calls. Contract locked to what supabase/functions/cluster-proxy/index.ts
# expects on `poll`, `result`, and `cancel` actions.
# =========================================================================== #

@app.get("/jobs/{job_id}")
def get_job_status(job_id: str) -> JSONResponse:
    """Return current job status. Body shape matches what the edge function's
    `poll` handler feeds to the frontend."""
    job = _job_store.get(job_id)
    if job is None:
        # Distinct 404 code so the edge function can surface "job was lost —
        # please retry" instead of leaving the frontend polling forever.
        raise HTTPException(
            status_code=404,
            detail={
                "error": {
                    "code": "JOB_NOT_FOUND",
                    "message": (
                        "Job not found. This can happen if the server "
                        "restarted while the job was in flight. Please "
                        "re-submit the file."
                    ),
                }
            },
        )
    return JSONResponse(job.snapshot())


@app.get("/jobs/{job_id}/result")
def get_job_result(job_id: str) -> JSONResponse:
    """Return the completed clustering body. Same shape as the pre-async
    `POST /cluster` response, so `persistResults` in the edge function needs
    no changes."""
    job = _job_store.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "JOB_NOT_FOUND", "message": "Job not found."}},
        )
    if job.status is JobStatus.PROCESSING or job.status is JobStatus.QUEUED:
        raise HTTPException(
            status_code=409,
            detail={
                "error": {
                    "code": "JOB_NOT_READY",
                    "message": f"Job is still {job.status.value}.",
                    "status": job.status.value,
                    "progress": job.progress,
                }
            },
        )
    if job.status is JobStatus.FAILED or job.status is JobStatus.CANCELED:
        raise HTTPException(
            status_code=422,
            detail={
                "error": {
                    "code": job.error_code or "JOB_FAILED",
                    "message": job.error_message or "Job failed.",
                    "status": job.status.value,
                }
            },
        )
    # COMPLETED. pop_result gives us consume-once semantics so the (potentially
    # large) result payload is released after retrieval — the job record stays
    # so subsequent polls still 200 with status=completed, they just can't
    # re-fetch the body. The edge function only fetches it once anyway
    # (persistResults writes to the DB, then it's the source of truth).
    body = _job_store.pop_result(job_id)
    if body is None:
        # Already consumed. Fall back to a minimal completed marker so callers
        # can still see "completed" even if they retried the fetch.
        raise HTTPException(
            status_code=410,
            detail={
                "error": {
                    "code": "RESULT_ALREADY_FETCHED",
                    "message": "This job's result has already been retrieved.",
                }
            },
        )
    return JSONResponse(body)


@app.post("/jobs/{job_id}/cancel")
@app.delete("/jobs/{job_id}")
def cancel_job(job_id: str) -> JSONResponse:
    """Best-effort cancel. Queued jobs cancel immediately; processing jobs stop
    at the next stage boundary (cannot interrupt sklearn C-code mid-call).
    The edge function tries DELETE first, then POST /cancel — both are wired
    to the same handler."""
    ok = _job_store.request_cancel(job_id)
    if not ok:
        job = _job_store.get(job_id)
        if job is None:
            raise HTTPException(
                status_code=404,
                detail={"error": {"code": "JOB_NOT_FOUND", "message": "Job not found."}},
            )
        # Already terminal — idempotent success.
        return JSONResponse({"canceled": False, "status": job.status.value})
    return JSONResponse({"canceled": True})
