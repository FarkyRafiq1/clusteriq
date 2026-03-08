from __future__ import annotations

from typing import Dict, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .pipeline import ClusterPipeline
from .schemas import PipelineConfig

app = FastAPI(title="ClusterIQ Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


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
            _build_config(
                keyword_column,
                volume_column,
                difficulty_column,
                rank_column,
                url_column,
            )
        )
        df = pipeline.read_table(payload, file.filename or "upload.csv")
        result = pipeline.run(df)
        response = {
            "summary": result["summary"],
            "clusters": result["clusters"].to_dict(orient="records"),
            "rows": pipeline.export_rows(result).to_dict(orient="records"),
        }
        return JSONResponse(response)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
