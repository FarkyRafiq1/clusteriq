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
Abuse controls use their own statuses: HTTP 429 `RATE_LIMITED` (with a
`Retry-After` header) and HTTP 503 `SERVER_BUSY` when all clustering slots
are taken. Internal bugs return HTTP 500 with a reference id and are
logged — they are never disguised as 400s.

Limits: 50 MB upload (enforced while streaming, plus a decompressed-size
guard for xlsx), 100,000 rows.

## Security & operations

Configure these environment variables (Railway -> service -> Variables):

- `ALLOWED_ORIGINS` — comma-separated frontend origins, e.g.
  `https://yourapp.lovable.app`. Defaults to `*` for development and logs a
  startup warning; set it before public launch.
- `RATE_LIMIT_PER_MINUTE` — per-IP requests/minute shared across `/preview`
  and `/cluster` (default 10; `0` disables).
- `MAX_CONCURRENT_JOBS` — simultaneous clustering jobs (default 2); beyond
  this, clients get an immediate 503 instead of piling onto the CPU.
- `TRUST_PROXY_HEADERS` — leave at the default `1` behind Railway's proxy so
  rate limits key on the real client IP (rightmost `X-Forwarded-For` entry);
  set `0` only if the app is ever exposed directly.

Design notes: the pipeline runs in a threadpool, so `/health` and new
requests stay responsive while jobs run. Rate limiting and job slots are
in-process — correct for a single Railway instance; if you scale to
multiple replicas, move them to a shared store (e.g. Redis). CORS protects
browser users from other websites, not scripted callers — the rate limiter
covers those; per-user API access needs real authentication (e.g. Supabase
JWTs), which is a product feature rather than a config flag. Frontend note:
when exporting clusters to CSV, prefix cell values starting with `=`, `+`,
`-` or `@` to neutralise spreadsheet formula injection — keyword text is
arbitrary user input.

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


### GET `/health`
Liveness plus lightweight runtime stats: `active_jobs`, `max_jobs`, and cache
hit/miss counts. Wire this as the platform healthcheck (already set in
`railway.toml`).

## Operations / cost controls

Tunable via environment variables (see `railway.toml` for recommended values):

| Variable | Default | Purpose |
|----------|---------|---------|
| `RATE_LIMIT_PER_MINUTE` | 120 | Per-IP request cap. All frontend traffic arrives from one edge-function egress IP, so treat this as a **global** limit. `0` disables. |
| `MAX_CONCURRENT_JOBS` | 2 | Simultaneous clustering jobs; excess gets `503 SERVER_BUSY`. Size to CPU. |
| `MAX_UPLOAD_MB` | 50 | Hard upload cap. **Keep in sync with the storage bucket (20 MB).** |
| `HEAVY_JOB_ROW_LIMIT` | 50000 | Rows above which `/cluster` fast-fails with `413 TOO_MANY_ROWS` before the expensive stages. `0` = up to the pipeline's absolute `MAX_ROWS`. |
| `CLUSTER_CACHE_SIZE` | 64 | Cached result entries; identical re-runs are free. `0` disables. |
| `CLUSTER_CACHE_MB` | 128 | Total cache byte budget. |
| `ALLOWED_ORIGINS` | `*` | CORS allow-list (not load-bearing for the server-to-server call path, but set before public launch). |

`/cluster` responses include `cached` (bool) and, on a fresh run, `timing_ms`.
The clustering pipeline is deterministic (`random_state`), so caching is safe.
