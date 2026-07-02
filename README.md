# ClusterIQ Engine

Production-ready Python core for a keyword clustering SaaS.

## What it does
- Reads CSV / TSV / XLSX / legacy XLS exports — detected by file *content*
  (magic bytes), with automatic encoding detection (UTF-8, UTF-8 BOM,
  UTF-16 Ahrefs exports, latin-1) and delimiter sniffing (comma, tab,
  semicolon, pipe)
- Resolves columns case-insensitively with aliases for common SEO tools
  (Ahrefs `Keyword`/`KD`/`Current position`, GSC `Top queries`/`Impressions`,
  Semrush, etc.) — an explicit mapping always wins
- Cleans numerics ("1,900", "34%", "£1 200", "n/a", "-") without silently
  zeroing them; missing values stay missing
- Normalizes and deduplicates keywords (NaN keywords are dropped, never
  clustered as the literal string "nan")
- Tags intent and page-type hints
- Builds hybrid lexical + semantic vectors
- Creates a sparse nearest-neighbor similarity graph (vectorized — handles
  50k+ keyword files)
- Clusters keywords into topic groups and flags groups below
  `min_cluster_size` via `is_clustered`
- Applies SEO-aware post-splitting rules
- Scores cluster quality and opportunity
- Exposes a FastAPI service whose responses are strict-JSON safe
  (no bare `NaN` literals — browser `JSON.parse` always succeeds)

## Quickstart
```bash
pip install -r requirements.txt
uvicorn clusteriq_engine.api:app --reload
```

## API

### POST `/preview`
Same multipart fields as `/cluster`. Parses the file and returns
`columns_detected`, the proposed `column_mapping`, `row_count` and 20
`sample_rows` — use it to power a "map columns" screen before running the job.

### POST `/cluster`
Multipart form fields:
- `file` (required)
- `keyword_column` (default `keyword`; auto-detected via aliases if absent)
- `volume_column`, `difficulty_column`, `rank_column`, `url_column`
  (optional; auto-detected via aliases)

Response body:
```json
{
  "summary": { "keywords": 0, "clusters": 0, "clustered_keywords": 0,
                "unclustered_keywords": 0, "avg_cluster_quality": 0,
                "top_cluster": {} },
  "column_mapping": { "keyword": "Keyword", "volume": "Volume", "...": "..." },
  "columns_detected": ["..."],
  "clusters": [ { "...": "...", "is_clustered": true } ],
  "rows": [ { "...": "..." } ]
}
```

### Errors
User-input problems return HTTP 400 with a structured payload:
```json
{ "detail": { "error": { "code": "KEYWORD_COLUMN_NOT_FOUND",
                          "message": "...", "columns_detected": ["..."] } } }
```
Codes include `EMPTY_FILE`, `FILE_TOO_LARGE`, `TOO_MANY_ROWS`,
`UNPARSEABLE_FILE`, `UNPARSEABLE_XLSX`, `UNPARSEABLE_XLS`,
`KEYWORD_COLUMN_NOT_FOUND`, `NO_KEYWORDS`, `NO_ROWS`.
Internal bugs return HTTP 500 with a reference id and are logged — they are
never disguised as 400s.

Limits: 50 MB upload, 100,000 rows.

## Example local run
```bash
python -m clusteriq_engine.pipeline
```

## Tests
```bash
python -m pytest clusteriq_engine/tests/ -q
```
The suite is a regression net over real-world failure modes (UTF-16 Ahrefs
exports, BOM headers, semicolon CSVs, `.xls`, thousands separators, NaN
keywords/urls/metrics, stopword-only clusters, strict-JSON responses) and
runs in CI against both pandas 2.x and 3.x.

## Lovable handoff guidance
Use `/preview` for the upload + map-columns flow, then `/cluster` as the
engine behind:
- upload CSV/TSV/XLSX/XLS
- map columns (pre-filled from `column_mapping`)
- run clustering job
- show cluster list (filter on `is_clustered` to separate strays)
- show cluster detail
- show opportunity cards
- export cluster rows and cluster summaries
