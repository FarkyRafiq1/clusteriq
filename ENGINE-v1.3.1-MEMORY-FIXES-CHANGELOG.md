# ClusterIQ Engine v1.3.1 — Memory Fixes (changelog)

Fixes the OOM that killed the 25k-keyword clustering job on the 512 MB Railway
instance. Measured through the **full API path** on a 1-vCPU box (same CPU
class as the Railway instance):

| Scenario (25k keywords) | Peak RAM | Time | On 512 MB |
|---|---|---|---|
| v1.3.0 — what crashed | **2,324+ MB** | ~45 s | OOM ✗ |
| algorithm fixes only | 631 MB | 34 s | still over ✗ |
| **v1.3.1 — final** | **353 MB** | 31 s | fits, ~⅓ used ✓ |
| v1.3.1 @ 50k keywords | 453 MB | 80 s | fits ✓ |

Clustering output was **identical in every run** (11,013 clusters at 25k) —
these are memory fixes, not behaviour changes, for this data. All **54 tests
pass** (48 existing + 6 new memory-behaviour tests).

## What was actually wrong (three layers)

1. **Pair-scoring gathered n×k duplicated sparse rows at once** — at 25k
   keywords × 15 neighbours, ~375k row copies, hundreds of MB transient. Now
   scored in bounded blocks (`SIM_PAIR_BLOCK`, default 20,000). Same arithmetic
   per pair ⇒ results identical at any block size (locked in by a test).
2. **No lean mode at scale.** Inputs ≥ `MEMORY_LEAN_ROW_THRESHOLD` (default
   10,000 rows) now use float32 embeddings + float32 SVD input, TF-IDF
   `min_df=2`, and eager frees. Below the threshold, behaviour is
   **byte-identical** to previous versions (so all existing outputs/tests are
   untouched).
3. **The headline bug — sklearn's config is thread-local.** The API runs jobs
   in a worker thread (`run_in_threadpool`, to keep `/health` responsive). An
   import-time `set_config(working_memory=64)` only bound the *main* thread;
   the worker thread silently used sklearn's **1,024 MB default** for k-NN
   distance chunks. Identical code measured **~350 MB on the main thread vs
   ~1.8 GB in a worker thread**. The bound now lives in a `config_context`
   *inside* `_hybrid_similarity_graph`, so it holds in whichever thread runs
   the job. This is why the crash survived the "obvious" fixes.

Also included: glibc mallopt hygiene (arena cap + pinned mmap threshold) so
freed buffers return to the OS between jobs — steady-state hygiene, explicitly
*not* the peak fix (the in-code comment says so); disable with
`MALLOC_TUNING=0`. Cache default `CLUSTER_CACHE_MB` 128 → 64. Version bump to
1.3.1 also invalidates previously cached results (cache keys include the
engine version).

## Files changed (6 — 1 new, 5 edited; nothing deleted)

- `clusteriq_engine/pipeline.py` — all memory work above
- `clusteriq_engine/api.py` — version 1.3.1, cache default 64 MB
- `clusteriq_engine/tests/test_memory_behaviour.py` — **new**: block-size
  result-identity, lean activation + float64-equivalence, small-input
  unchanged, working-memory bound
- `railway.toml` — sized variable profiles for 512 MB / 1 GB / 2 GB instances
- `README.md` — ops table updated (new knobs, corrected default)
- `CHANGES.md` — v1.3.1 entry

## Apply

**GitHub web UI (your usual route):** unzip `clusteriq-engine-complete.zip`
locally and drag the contents over the repo (branch → PR → Squash and merge),
**or** just upload the 6 files in `engine-memory-changed-files/` preserving
paths. Nothing is deleted this round, so no manual removals.

**Local git:** `git apply clusteriq-engine-memory-fixes.patch` — verified to
apply cleanly to the current repo state (v1.3.0 + the httpx fix).

## After deploying — set these Railway Variables (512 MB instance)

| Variable | Value | Why |
|---|---|---|
| `MAX_CONCURRENT_JOBS` | **1** | Two concurrent jobs double peak RAM — the one setting that can still OOM a 512 MB box |
| `HEAVY_JOB_ROW_LIMIT` | **25000** | Bigger files get a clear 413 instead of a slow crash |
| `CLUSTER_CACHE_MB` | 32 | Keep the cache small on a small box |
| `SKLEARN_WORKING_MEMORY_MB` | 64 | Already the default; set explicitly for clarity |
| `MAX_UPLOAD_MB` | 20 | Match the storage bucket |

(1 GB / 2 GB profiles are documented in `railway.toml` — on 1 GB you can set
`HEAVY_JOB_ROW_LIMIT=50000`; the 50k benchmark peaked at 453 MB.)

## Verify after deploy

1. `/health` shows `"version":"1.3.1"`.
2. Re-run the exact 25k job that crashed. Watch Railway metrics: expect peak
   RAM ≈ 350–400 MB and ~30–60 s runtime (CPU pinned at 1.0 during the job is
   normal — it's compute, not a fault).
3. Run it a second time with the same file: it should return in ~1 s with
   `"cached": true`.

## Honest caveats

- These numbers come from a 1-vCPU sandbox measuring peak RSS — the same metric
  Railway's OOM killer enforces — but it isn't a cgroup-limited container.
  Step 2 above is the real-world confirmation.
- For files ≥ 10k rows, lean mode (`min_df=2`, float32) can in principle alter
  borderline clustering decisions vs v1.3.0. In both the 25k and 50k
  benchmarks the outputs were **identical**; results remain fully
  deterministic run-to-run.
- Synchronous timing grows with size (~80 s at 50k on 1 vCPU). If you later
  want 100k-row jobs, that's the point to either size up the instance or
  revisit the async-jobs question from the integration review.
