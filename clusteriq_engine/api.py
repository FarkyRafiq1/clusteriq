from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .errors import UserError
from .ingest import resolve_columns
from .pipeline import ClusterPipeline
from .schemas import PipelineConfig
from .utils import df_records_json_safe

logger = logging.getLogger("clusteriq")

app = FastAPI(title="ClusterIQ Engine", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # MVP setting: restrict to the frontend origin before launch.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
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
    try:
        payload = await file.read()
        pipeline = ClusterPipeline(
            _build_config(keyword_column, volume_column, difficulty_column, rank_column, url_column)
        )
        df = pipeline.read_table(payload, file.filename or "upload")
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
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc


@app.post("/cluster")
async def cluster_file(
    file: UploadFile = File(...),
    keyword_column: str = Form("keyword"),
    volume_column: Optional[str] = Form("volume"),
    difficulty_column: Optional[str] = Form("difficulty"),
    rank_column: Optional[str] = Form("position"),
    url_column: Optional[str] = Form("url"),
) -> JSONResponse:
    try:
        payload = await file.read()
        pipeline = ClusterPipeline(
            _build_config(keyword_column, volume_column, difficulty_column, rank_column, url_column)
        )
        df = pipeline.read_table(payload, file.filename or "upload")
        return JSONResponse(build_cluster_response(pipeline, df))
    except UserError as exc:
        raise _user_error_response(exc) from exc
    except Exception as exc:  # noqa: BLE001 - boundary handler
        raise _internal_error_response(exc) from exc
