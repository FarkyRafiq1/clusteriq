# ClusterIQ Engine Repository Guidance

## Repository identity

- Authoritative local checkout: `C:\Users\shafa\OneDrive\Documents\GitHub\clusteriq`
- GitHub repository: `https://github.com/FarkyRafiq1/clusteriq`
- Production branch: `main`
- Deployment: Railway
- Production entry point: `clusteriq_engine.api:app`
- Consumer: the `clusteriq-front` Supabase `cluster-proxy` Edge Function

This repository is the source of truth for the Python keyword-clustering engine.
Lovable frontend versions are not authoritative for this service.

## Before making changes

1. Confirm the working directory is this `clusteriq` checkout.
2. Confirm `origin` points to `FarkyRafiq1/clusteriq`.
3. Fetch the latest remote state and compare the current branch with `origin/main`.
4. Report any existing uncommitted changes and preserve changes that belong to the user.
5. Create a focused branch rather than committing directly to `main` unless the user explicitly requests otherwise.
6. Never use or edit `C:\Users\shafa\OneDrive\Documents\ChatGPT\ClusterIQ` as the application source.
7. Do not edit the React/Supabase application here. Its checkout is `C:\Users\shafa\OneDrive\Documents\GitHub\clusteriq-front`.

## Architecture boundaries

- `clusteriq_engine/api.py` is the production FastAPI application.
- `clusteriq_engine/pipeline.py` is the keyword-clustering pipeline.
- `clusteriq_engine/ingest.py` handles uploaded SEO data files.
- `clusteriq_engine/jobs.py` manages in-process asynchronous jobs.
- `clusteriq_engine/persistence.py` writes results to Supabase when configured.
- `railway.toml` defines the production start command and health check.

Root-level historical or duplicate files are not automatically production entry
points. Confirm imports and the Railway start command before editing them.

## Validation

Run checks appropriate to the change. Before publishing a normal engine change,
prefer running:

```sh
python -m pytest clusteriq_engine/tests/ -q
```

For changes to the HTTP contract, also verify the corresponding client and proxy in:

```text
C:\Users\shafa\OneDrive\Documents\GitHub\clusteriq-front\src\lib\cluster-proxy.ts
C:\Users\shafa\OneDrive\Documents\GitHub\clusteriq-front\supabase\functions\cluster-proxy\index.ts
```

Do not assume clustering configuration fields sent by the frontend are supported;
confirm that FastAPI declares them and that they are applied to `PipelineConfig`.

## Publishing and deployment

- Review the final diff before staging.
- Stage only files that belong to the requested change.
- Use concise commits and push the working branch to GitHub.
- Prefer a pull request into `main` so tests run before Railway deployment.
- Treat job state, rate limits, and result caching as in-process state unless the
  implementation explicitly moves them to durable shared infrastructure.
- A frontend or Netlify deployment does not deploy this Railway service.
