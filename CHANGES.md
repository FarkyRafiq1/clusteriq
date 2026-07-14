## v1.4.2 — per-request persistence credentials (the Lovable Cloud path)

Lovable Cloud never exposes the Supabase service-role key to humans — it
exists only inside the edge-function environment. So instead of an operator
copying the key into Railway (impossible on Lovable Cloud), the edge function
now hands `supabase_url` + `supabase_key` to the engine with each submit,
alongside `upload_id`/`project_id`. Server-to-server over TLS; the engine
holds them in memory for the job only, never logs, stores, or echoes them
(leak-tested against every observable surface). Per-request credentials take
precedence over the env vars, which remain as an optional fallback for
standard/external Supabase setups — so a platform-side key rotation can never
strand the engine with a stale copy.

Also hardened: optional Form fields are normalised at the endpoint boundary
(direct invocation passes truthy Form sentinels, not None — caught by the
hardening suite). 91 tests (5 new).
## v1.4.1 — engine-direct persistence: results written straight to Supabase

Fixes the WORKER_LIMIT kill ("Function failed due to not having enough compute
resources") that destroyed results during persistence. Root cause: the edge
function — a free-tier Deno isolate with ~2 s of CPU — was parsing an ~18 MB
result body, inserting ~20k+ cluster rows in ONE call with a `.select()`
echoing them all back, then ~26k keyword rows in ~53 sequential inserts. The
same code path killed v1.3.x runs silently (browser saw a hung request die
with no error) and v1.4.0 runs loudly (failed job with an id).

Now: when the edge function passes `upload_id` + `project_id` on submit and
`SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` are set on Railway, the engine's
worker persists results directly (new `clusteriq_engine/persistence.py` —
faithful port of the edge function's persistResults: same tables/columns,
plus client-side UUIDs so no insert echo, chunked cluster inserts, and an
idempotent delete-before-insert so retries can't duplicate). The job's stored
result becomes a compact summary (`persisted: true, cluster_count, row_count,
summary`) — the 18 MB payload never enters the isolate again. Cache hits with
a persistence context re-persist from the cached body (the recovery path
after a failed persist). Persist failures mark the job `PERSIST_FAILED` with
a clear retry message.

Unset the variables (or omit the context) and behaviour is exactly v1.4.0 —
full body served, edge function persists — so deploy order is safe.

Companion edge-function v2 (frontend repo): passes the context through,
handles compact results with a legacy-persist fallback, and restores workspace
usage metering (previously only wired on the unreachable sync branch, so
async jobs ran quota-free).

Also: `railway.toml` builder corrected to RAILPACK (matches the service);
`/health` reports `"persistence": true/false`. 86 tests (12 new).
## v1.4.0 — async clustering: POST /cluster always returns a job

Fixes the 150 s Supabase edge-function wall clock killing long jobs: the engine
computed results and threw them away because nothing was still listening.
Submission now returns in <0.5 s regardless of file size.

**New contract** (matches what cluster-proxy's poll/result/cancel actions
already call):
- `POST /cluster` -> `202 {"job_id": "...", "status": "queued"}` — always.
  Upload validation (size cap, malformed file) still fails synchronously.
- `GET /jobs/{id}` -> `{job_id, status, progress, stage, error_*}`;
  404 `JOB_NOT_FOUND` for unknown ids (e.g. after an engine restart).
- `GET /jobs/{id}/result` -> the exact pre-1.4 response body (clusters/rows/
  summary/timing_ms/cached). 409 `JOB_NOT_READY` while running; 422 with the
  job's error code on failure; 410 `RESULT_ALREADY_FETCHED` on re-fetch
  (consume-once: the body is released after retrieval to protect the 512 MB
  box — recovery path is the result cache: re-submit is instant).
- `POST /jobs/{id}/cancel` and `DELETE /jobs/{id}` -> best-effort cancel.
  Queued jobs cancel immediately; processing jobs stop at the next stage
  boundary (sklearn C-code cannot be interrupted mid-call).

**Behaviour changes to know about:**
- `TOO_MANY_ROWS` is no longer a submission-time 413 — row count isn't known
  until the file is parsed in the worker, so it surfaces as a *failed job*
  with that code, which the poll loop already forwards to the frontend.
- Cache hits return via the same async contract (a pre-completed job) so the
  frontend polling path is uniform — no sync special case.
- In-process job state does not survive engine restarts. Unknown ids 404 with
  a recoverable message; the companion edge-function change marks the DB row
  failed so the UI stops polling.

Internals: new `clusteriq_engine/jobs.py` (thread-safe JobStore: TTL sweep,
LRU-bounded, monotonic transitions, cooperative cancel). The sync-era
`_job_slots` no longer gates workers (the bounded worker semaphore does);
kept for /health visibility. Env knobs: `JOB_TTL_SECONDS`,
`JOB_WORKER_THREADS`, `JOB_STORE_MAX`. 74 tests (17 new async-lifecycle).

## v1.3.1 — memory fixes: 25k-keyword jobs no longer OOM small containers

A real 25k-keyword clustering job OOM-killed a 512 MB Railway instance. Peak
memory through the full API path, measured on a 1-vCPU box:

| | peak RSS | time |
|---|---|---|
| v1.3.0 (crashed) | **2,324+ MB** | ~45 s |
| algorithm fixes only | 631 MB | 34 s |
| **v1.3.1 (final)** | **353 MB** | 31 s |
| v1.3.1 @ 50k rows | 453 MB | 80 s |

Cluster output identical across all runs (11,013 clusters at 25k).

Root causes, in order of discovery:
- **Candidate-pair gather** duplicated n*k sparse rows at once (~hundreds of MB
  at 25k x k=15). Now scored in bounded blocks (`SIM_PAIR_BLOCK`, default
  20,000) — identical arithmetic, bounded peak.
- **Lean mode** for inputs >= `MEMORY_LEAN_ROW_THRESHOLD` (default 10,000):
  float32 embeddings and SVD input, TF-IDF `min_df=2`, eager frees + gc.
  Smaller inputs are byte-identical to previous versions.
- **The headline bug:** sklearn's config is **thread-local**, so an import-time
  `set_config(working_memory=...)` did not reach the worker thread the API runs
  jobs in — which silently used sklearn's 1,024 MB default for k-NN distance
  chunks (~1.4 GB spike, thread-only). The bound now lives in a
  `config_context` inside `_hybrid_similarity_graph`, correct in any thread.
- glibc mallopt hygiene (arena cap + pinned mmap threshold) keeps steady-state
  RSS low between jobs; env-gated via `MALLOC_TUNING=0`.

Also: `CLUSTER_CACHE_MB` default 128 -> 64; `railway.toml` now documents sized
variable profiles for 512 MB / 1 GB / 2 GB instances; new
`tests/test_memory_behaviour.py` locks in block-size result-identity and lean
activation. Version bump also invalidates previously cached results.

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
