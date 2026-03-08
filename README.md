# ClusterIQ Engine

Production-ready Python core for a keyword clustering SaaS.

## What it does
- Reads CSV/XLSX datasets
- Normalizes and deduplicates keywords
- Tags intent and page-type hints
- Builds hybrid lexical + semantic vectors
- Creates a nearest-neighbor similarity graph
- Clusters keywords into topic groups
- Applies SEO-aware post-splitting rules
- Scores cluster quality and opportunity
- Exposes a FastAPI endpoint

## Quickstart
```bash
pip install -r requirements.txt
uvicorn clusteriq_engine.api:app --reload
```

## API
POST `/cluster`

Multipart form fields:
- `file`
- `keyword_column`
- `volume_column`
- `difficulty_column`
- `rank_column`
- `url_column`

## Example local run
```bash
python -m clusteriq_engine.pipeline
```

## Lovable handoff guidance
Use the `/cluster` endpoint as the engine behind these flows:
- upload CSV/XLSX
- map columns
- run clustering job
- show cluster list
- show cluster detail
- show opportunity cards
- export cluster rows and cluster summaries
