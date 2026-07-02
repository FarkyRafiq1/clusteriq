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
