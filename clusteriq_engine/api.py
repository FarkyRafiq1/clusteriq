from __future__ import annotations

import logging
import os
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


app = FastAPI(title="ClusterIQ Engine", version="1.3.1", lifespan=_lifespan)

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
) -> JSONResponse:
    _enforce_rate_limit(request)
    try:
        payload = await read_upload_limited(
            file, EFFECTIVE_MAX_UPLOAD_BYTES, request.headers.get("content-length")
        )
        config = _build_config(
            keyword_column, volume_column, difficulty_column, rank_column, url_column
        )
        pipeline = ClusterPipeline(config)

        # ---- Cache lookup ------------------------------------------------ #
        # Deterministic pipeline (fixed random_state) => identical (file, mapping)
        # always yields the same body. A hit returns instantly and spends no CPU.
        cache_key = result_cache_key(
            payload,
            {
                "keyword": keyword_column,
                "volume": volume_column,
                "difficulty": difficulty_column,
                "position": rank_column,
                "url": url_column,
            },
            {"engine_version": app.version, "backend": config.semantic_backend},
        )
        cached = _result_cache.get(cache_key)
        if cached is not None:
            logger.info("cache hit for cluster request (rows served from cache)")
            return JSONResponse({**cached, "cached": True})

        def _run_job() -> Dict[str, Any]:
            # The whole CPU-heavy path runs inside one threadpool worker and one
            # job slot, so the event loop (and /health) stay responsive.
            with _job_slots.acquire():
                df = pipeline.read_table(payload, file.filename or "upload")

                # Fast-fail oversized jobs BEFORE the expensive vectorize/cluster
                # stages, so a huge file can't monopolise CPU or blow the request
                # timeout. Return a clear, actionable 413 instead.
                if HEAVY_JOB_ROW_LIMIT > 0 and len(df) > HEAVY_JOB_ROW_LIMIT:
                    raise _too_many_rows_response(len(df), HEAVY_JOB_ROW_LIMIT)

                started = time.monotonic()
                body = build_cluster_response(pipeline, df)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                body["timing_ms"] = elapsed_ms
                body["cached"] = False
                logger.info(
                    "clustered %d rows -> %d clusters in %d ms",
                    len(df),
                    len(body.get("clusters", [])),
                    elapsed_ms,
                )
                return body

        body = await run_in_threadpool(_run_job)
        _result_cache.put(cache_key, body)
        return JSONResponse(body)
    except HTTPException:
        raise
    except ServerBusy:
        raise _busy_response() from None
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc
