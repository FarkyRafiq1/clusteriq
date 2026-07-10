# ClusterIQ Engine v1.4.0 — Async Clustering (changelog)

Solves the 150 s Supabase edge-function wall clock killing long jobs: the
engine finished clustering, but the HTTP request holding the result had
already been terminated, so the work was thrown away. `POST /cluster` now
returns in **under half a second** regardless of file size; the pipeline runs
in the background and the frontend's existing poll loop picks it up.

## Verified end-to-end — with your real file

Booted via the exact Railway start command and driven through the same call
sequence the cluster-proxy edge function makes, using your actual
`plumbworld_co_uk-organic-keywords…csv` (26,511 rows, UTF-16 TSV):

| Step | Result |
|---|---|
| `POST /cluster` (26.5k rows) | **202 in 0.35 s** → `{job_id, status: queued}` |
| Poll progression | `processing@5%(reading)` → `processing@25%(vectorizing)` → `completed@100%` |
| `GET /jobs/{id}/result` | 200 — 26,333 rows, 23,712 clusters, 26.9 s compute |
| Re-fetch result | 410 `RESULT_ALREADY_FETCHED` (consume-once, by design) |
| Re-submit same file | 202 in **0.11 s**, `cached: true`, result served instantly |
| Cancel mid-job | 200 → terminal `failed / CANCELED / "Canceled by user"` |
| Poll unknown id | 404 `JOB_NOT_FOUND` (restart-loss contract) |

Test suite: **74/74 pass** (48 pre-existing, 9 rewritten for the async
contract, 17 new async-lifecycle tests). Patch verified to apply cleanly to a
fresh extract of the deployed v1.3.1 repo.

## What changed (backend — 8 files, 2 new)

- **`clusteriq_engine/jobs.py` (new)** — thread-safe in-process JobStore:
  monotonic status transitions, monotonic progress, TTL sweep of finished
  jobs, LRU bound (terminal entries evicted first), cooperative cancel flag.
- **`clusteriq_engine/api.py`** — `/cluster` always async (202 + job_id;
  upload validation still fails synchronously so bad files get an immediate
  400/413-class error). New `GET /jobs/{id}`, `GET /jobs/{id}/result`,
  `POST /jobs/{id}/cancel` + `DELETE /jobs/{id}`. Cache hits return a
  pre-completed job so the polling path is uniform. `/health` now reports job
  stats. Version → 1.4.0.
- **Tests** — cost-control + hardening suites rewritten for the async
  contract; new `test_async_jobs.py`.
- **`railway.toml` / `README.md` / `CHANGES.md`** — docs + new env knobs
  (`JOB_TTL_SECONDS`, `JOB_WORKER_THREADS`, `JOB_STORE_MAX`).

**No frontend or edge-function changes are required for the happy path** —
the contract was built to match what `cluster-proxy`'s `submit`/`poll`/
`result`/`cancel` actions already call.

## Companion (frontend repo, optional but recommended)

`frontend-companion/supabase/functions/cluster-proxy/index.ts` — one addition
to the `poll` action: when the engine 404s a job id (Railway restarted
mid-job), the edge function now marks the Supabase `jobs` row **failed** with
"engine restarted — please re-submit", so the UI stops polling a ghost. Upload
this to the *frontend* repo and redeploy the edge function.

## Behaviour changes to be aware of

1. **`TOO_MANY_ROWS` is no longer a submission-time 413.** Row count isn't
   known until the worker parses the file, so an oversized file is *accepted*
   (202) and then fails as a job with code `TOO_MANY_ROWS`. The poll loop
   already forwards job failures to the UI — but the error now appears a few
   seconds after submit rather than instantly. If you preferred the instant
   413, that's recoverable by adding a cheap pre-parse row estimate — ask.
2. **Results are consume-once.** After the edge function fetches a result and
   persists it to the DB, the in-memory body is released (memory protection on
   the 512 MB box). If persist ever failed mid-way, re-submitting the same
   file is the recovery path — it's a cache hit, so it completes instantly.
3. **Job state does not survive engine restarts.** A restart mid-job loses the
   in-flight job; polls 404 with a clear message, and (with the companion
   change) the DB row is marked failed. This is the accepted trade-off of
   option A (in-process state, zero new infrastructure). If restart-surviving
   jobs become a real need, the next step is persisting job state to Supabase
   (option B) — a 1–2 day change, not a rewrite.
4. **Cancel is stage-boundary, not instant.** sklearn's C code can't be
   interrupted mid-SVD; a cancel during heavy compute takes effect at the next
   checkpoint. Cancelled jobs surface as `failed` with code `CANCELED`.

## Deploy

1. **Backend:** upload (complete zip contents, or the 8 files in
   `engine-v140-changed-files/`, or `git apply clusteriq-engine-v1.4.0-async.patch`).
   CI should show 74 passing. Redeploy on Railway; `/health` must say
   `"version":"1.4.0"` and include a `"jobs"` block.
2. **Frontend (optional companion):** replace
   `supabase/functions/cluster-proxy/index.ts` with the companion file and
   redeploy the edge function.
3. **Variables:** nothing new is required. Optional: `JOB_TTL_SECONDS`
   (default 3600), `JOB_WORKER_THREADS` (default = MAX_CONCURRENT_JOBS — keep
   at 1 on the 512 MB box).
4. **Verify:** run your 26.5k file from the UI. Submission should be instant,
   the progress UI should tick `reading → vectorizing → completed`, and — the
   actual point of all this — a job longer than 150 s can no longer lose its
   results, because nothing is waiting on a long-lived request any more.

## Honest caveats

- E2E verification here used a 1-vCPU sandbox and a local uvicorn — the same
  code path as Railway but not Railway itself. The `/health` version check and
  one real UI run are the final confirmation.
- Progress granularity is coarse (3 stages). Real percent-complete would need
  instrumentation inside the pipeline — doable later if the UI wants it; the
  current stages are honest rather than a fake smooth bar.
- The stray `ENGINE-v1.3.1-MEMORY-FIXES-CHANGELOG.md` committed to the repo
  root earlier is harmless; delete it whenever convenient.
