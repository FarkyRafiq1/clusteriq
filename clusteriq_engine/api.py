from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

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
# MAX_CONCURRENT_JOBS     simultaneous clustering jobs; 0 = unlimited
# TRUST_PROXY_HEADERS     "1" behind Railway's proxy (default); "0" if exposed directly
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
] or ["*"]
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "10"))
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
TRUST_PROXY_HEADERS = os.getenv("TRUST_PROXY_HEADERS", "1") != "0"

app = FastAPI(title="ClusterIQ Engine", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

_rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)
_job_slots = JobSlots(MAX_CONCURRENT_JOBS)


@app.on_event("startup")
def _warn_if_open_cors() -> None:
    if "*" in ALLOWED_ORIGINS:
        logger.warning(
            "CORS is open to all origins. Set ALLOWED_ORIGINS to your frontend "
            "origin (e.g. https://yourapp.lovable.app) before public launch."
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
def health() -> Dict[str, str]:
    return {"status": "ok"}


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
            file, MAX_UPLOAD_BYTES, request.headers.get("content-length")
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
            file, MAX_UPLOAD_BYTES, request.headers.get("content-length")
        )
        pipeline = ClusterPipeline(
            _build_config(keyword_column, volume_column, difficulty_column, rank_column, url_column)
        )

        def _run_job() -> Dict[str, Any]:
            # The whole CPU-heavy path runs inside one threadpool worker and
            # one job slot, so the event loop (and /health) stay responsive.
            with _job_slots.acquire():
                df = pipeline.read_table(payload, file.filename or "upload")
                return build_cluster_response(pipeline, df)

        body = await run_in_threadpool(_run_job)
        return JSONResponse(body)
    except HTTPException:
        raise
    except ServerBusy:
        raise _busy_response() from None
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc
