## v1.3.0 — practical cost & throughput controls

- **Result cache**: identical (file + column mapping) re-runs are served from an
  in-process LRU cache (SHA-256 keyed), returning instantly and spending no CPU.
  Bounded by entry count and total bytes; `CLUSTER_CACHE_SIZE=0` disables it.
  Responses gain a `cached` boolean.
- **Heavy-job guard**: `/cluster` fast-fails files over `HEAVY_JOB_ROW_LIMIT`
  (default 50k) rows with a clear `413 TOO_MANY_ROWS` *before* the expensive
  vectorize/cluster stages, protecting CPU and request-timeout budget.
- **Timing/observability**: `/cluster` responses include `timing_ms`; `/health`
  now reports `active_jobs`, `max_jobs` and cache stats.
- **Deploy defaults for the real call pattern**: `RATE_LIMIT_PER_MINUTE` default
  raised to 120 (all traffic shares one edge-function egress IP, so the per-IP
  limit is effectively global); `MAX_UPLOAD_MB` env override added and documented
  to stay in sync with the 20 MB storage bucket; `/health` wired as the Railway
  healthcheck.
- **Modernisation**: replaced the deprecated `@app.on_event("startup")` with a
  `lifespan` handler (no behaviour change; removes the DeprecationWarning).
- Response contract for the frontend is unchanged; new fields are additive and
  never include `job_id`.

# Changes — parsing & NaN overhaul (July 2026)

All items below correspond to defects reproduced against the previous code
(see clusteriq-improvement-plan.md) and are locked in by
`clusteriq_engine/tests/`.

## Fixed
- Ahrefs UTF-16 tab-separated exports parsed as garbage (latin-1 fallback
  can never fail) -> content-based detection: magic bytes for xlsx/xls,
  charset-normalizer for text, delimiter sniffing restricted to , \t ; |
- UTF-8 BOM stuck to the first header (`\ufeffkeyword`) -> BOM stripped
- Semicolon CSVs read as one column -> sniffed
- `Keyword` / `KD` / `Top queries` etc. rejected by exact-match mapping ->
  case-insensitive resolution + alias table; explicit mapping still wins
- `.xls` unreadable -> xlrd engine, detected by OLE2 magic bytes
- Volumes like "1,900" silently became 0 -> numeric cleaning
  (thousands separators, %, currency, nbsp, n/a, dashes); missing stays NaN
- NaN keywords clustered as the literal string "nan" on pandas 2.x ->
  NaN-first masking before any string conversion; junk strings dropped
- TypeError crash on pandas 3.x when duplicate keywords had missing URLs
- Bare `NaN` literals in API JSON (invalid for browser JSON.parse) ->
  single json-safe chokepoint for every DataFrame + scalar; ±inf also mapped
  to null; opportunity score is NaN-proof
- "empty vocabulary" (all-stopword cluster, single-letter tokens) crashed
  the whole request -> guarded with fallbacks
- `min_cluster_size` was a no-op -> rows/clusters now carry `is_clustered`;
  summary reports clustered vs unclustered counts
- Blanket `except Exception -> 400` -> structured 400s (error codes) for
  user input, logged 500s with reference ids for bugs

## Performance
- Similarity graph: dense n x n + one sklearn cosine call per edge ->
  fully vectorized sparse graph (L2-normalized dot products).
  4k keywords: 56s -> 3s. 50k keywords: previously ~20 GB (OOM) -> 81s.
- Dedupe, tagging, post-split, per-cluster scoring vectorized;
  singleton fast paths for topic labels and canonical keywords.

## Added
- `POST /preview` (columns detected, proposed mapping, sample rows)
- `column_mapping` + `columns_detected` in `/cluster` responses
- Upload limits: 50 MB / 100,000 rows with clear error codes
- Pinned requirements (incl. xlrd, charset-normalizer); code verified on
  pandas 2.2.x and 3.0.x; CI workflow runs the suite on both
- `.gitignore`; committed `__pycache__` removed

# Changes — API hardening (July 2026)

- Clustering now runs in a threadpool with a concurrency cap
  (`MAX_CONCURRENT_JOBS`, default 2): the event loop stays responsive
  during jobs (worst stall measured: 41ms during a 4k-keyword run) and
  excess jobs get HTTP 503 `SERVER_BUSY` + Retry-After instead of
  stacking up on the CPU.
- Per-IP token-bucket rate limiting on `/preview` and `/cluster`
  (`RATE_LIMIT_PER_MINUTE`, default 10) -> HTTP 429 `RATE_LIMITED` +
  Retry-After. Keys on the proxy-appended client IP (rightmost
  X-Forwarded-For; `TRUST_PROXY_HEADERS=0` to use socket addresses).
- Upload size enforced while streaming: over-declared Content-Length is
  rejected before reading; undeclared oversized bodies are cut off at the
  first excess chunk (previously the whole body was buffered first).
- xlsx decompression-bomb guard: workbooks declaring > 300 MB uncompressed
  are rejected before openpyxl parses them.
- CORS origins now come from `ALLOWED_ORIGINS` (comma-separated); `*`
  remains the dev default and logs a startup warning.
- 15 new tests (44 total), still verified on pandas 2.x and 3.x.
