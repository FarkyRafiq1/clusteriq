"""Tests for cost-control / practical-usage features updated for v1.4.0:
async job submission (POST /cluster -> {job_id, status}), plus the contract
that the poll+result endpoints preserve the shape persistResults consumes.
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from clusteriq_engine.cache import ResultCache, result_cache_key
from clusteriq_engine.jobs import JobStore


CSV = (
    b"keyword,volume,difficulty,position,url\n"
    b"best running shoes,5400,45,8,https://ex.com/a\n"
    b"buy running shoes online,3200,52,12,https://ex.com/b\n"
    b"how to choose running shoes,1900,30,15,https://ex.com/c\n"
    b"trail running shoes review,720,41,9,https://ex.com/d\n"
    b"cheap running shoes,2400,48,18,https://ex.com/e\n"
)


@pytest.fixture()
def client():
    import clusteriq_engine.api as api
    # Fresh cache AND fresh job store per test so cross-test contamination
    # doesn't hide bugs (cache hits mask guard behaviour, terminal jobs from
    # a prior test would still be enumerable via /health).
    api._result_cache = ResultCache(api._CACHE_ENTRIES, api._CACHE_BYTES)
    api._job_store = JobStore(max_jobs=api._JOB_STORE_MAX, ttl_seconds=api._JOB_TTL_SECONDS)
    return TestClient(api.app)


def _submit(client: TestClient, data=None):
    return client.post(
        "/cluster",
        files={"file": ("kw.csv", CSV, "text/csv")},
        data=data or {"keyword_column": "keyword", "volume_column": "volume",
                      "difficulty_column": "difficulty", "rank_column": "position",
                      "url_column": "url"},
    )


def _run_to_completion(client: TestClient, submit_body: dict, timeout_s: float = 30.0):
    """Poll a job until terminal, then fetch its result. Mirrors the edge
    function's flow. Returns the result body."""
    job_id = submit_body["job_id"]
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        r = client.get(f"/jobs/{job_id}")
        assert r.status_code == 200, r.text
        status = r.json()["status"]
        if status in ("completed", "failed", "canceled"):
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"job {job_id} did not terminate within {timeout_s}s")
    result = client.get(f"/jobs/{job_id}/result")
    return result


# --------------------------- submission contract -------------------------- #

def test_submission_returns_job_id_and_202(client):
    """POST /cluster must return {job_id, status:queued} with 202 — never the
    old sync body. This is THE change; if this ever regresses, the edge
    function's timeout problem is back."""
    resp = _submit(client)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert set(body.keys()) >= {"job_id", "status"}
    assert body["status"] == "queued"
    assert isinstance(body["job_id"], str) and len(body["job_id"]) >= 8
    # The old sync fields must NOT be in the submission response.
    assert "clusters" not in body
    assert "rows" not in body
    assert "summary" not in body


def test_completed_result_preserves_frontend_contract(client):
    """After poll->completed, /jobs/{id}/result must return the exact shape
    persistResults expects. This is the contract that used to be tested
    against the sync body."""
    submit = _submit(client).json()
    result = _run_to_completion(client, submit)
    assert result.status_code == 200, result.text
    body = result.json()
    assert set(["summary", "column_mapping", "columns_detected", "clusters", "rows"]).issubset(body)

    cluster_keys = {
        "cluster_id", "cluster_slug", "topic_label", "canonical_keyword", "intent",
        "page_type", "is_clustered", "keyword_count", "total_volume", "avg_difficulty",
        "avg_rank", "cluster_quality", "opportunity_score", "keywords", "urls",
    }
    row_keys = {
        "keyword", "volume", "difficulty", "position", "url", "cluster_id", "intent",
        "page_type", "canonical_keyword", "topic_label", "cluster_quality",
        "opportunity_score", "is_clustered",
    }
    assert cluster_keys.issubset(body["clusters"][0].keys())
    assert row_keys.issubset(body["rows"][0].keys())
    assert body["cached"] is False
    assert isinstance(body["timing_ms"], int)


def test_poll_reports_status_progression(client):
    """Poll must return job snapshots the edge function can update the DB
    from — status, progress, and a job_id echoed back."""
    submit = _submit(client).json()
    job_id = submit["job_id"]

    # Immediate poll: queued or already-processing (fast tests can race).
    first = client.get(f"/jobs/{job_id}").json()
    assert first["job_id"] == job_id
    assert first["status"] in ("queued", "processing", "completed")
    assert "progress" in first

    # Drive to completion and confirm terminal snapshot shape.
    _run_to_completion(client, submit)
    final = client.get(f"/jobs/{job_id}").json()
    assert final["status"] == "completed"
    assert final["progress"] == 100


def test_unknown_job_returns_404_with_recoverable_code(client):
    """A missing job (e.g. after a Railway restart) must 404 with a code the
    edge function can surface as 'please re-submit', not hang the frontend."""
    resp = client.get("/jobs/nope-does-not-exist")
    assert resp.status_code == 404
    err = resp.json()["detail"]["error"]
    assert err["code"] == "JOB_NOT_FOUND"


# ------------------------------- caching ---------------------------------- #

def test_cache_hit_short_circuits_via_job_id(client):
    """A cached repeat still returns via the async contract — a completed job
    the caller can poll and fetch immediately. Keeps the frontend polling
    path uniform (no special sync case)."""
    first_submit = _submit(client).json()
    first_result = _run_to_completion(client, first_submit).json()
    assert first_result["cached"] is False

    second_submit = _submit(client)
    assert second_submit.status_code == 202
    body = second_submit.json()
    assert body.get("cached") is True  # flag on the submission itself
    second_result = _run_to_completion(client, body).json()
    assert second_result["cached"] is True
    assert second_result["summary"] == first_result["summary"]


def test_cache_key_varies_with_mapping():
    k1 = result_cache_key(CSV, {"keyword": "keyword"}, {"v": 1})
    k2 = result_cache_key(CSV, {"keyword": "term"}, {"v": 1})
    k3 = result_cache_key(CSV, {"keyword": "keyword"}, {"v": 1})
    assert k1 != k2
    assert k1 == k3


def test_result_cache_lru_and_byte_bounds():
    cache = ResultCache(max_entries=1, max_total_bytes=0)
    cache.put("a", {"x": 1})
    cache.put("b", {"y": 2})
    assert cache.get("a") is None
    assert cache.get("b") == {"y": 2}

    disabled = ResultCache(max_entries=0, max_total_bytes=0)
    disabled.put("a", {"x": 1})
    assert disabled.get("a") is None


# --------------------------- heavy-job guard ------------------------------ #

def test_heavy_job_row_limit_fails_the_job(monkeypatch):
    """A file over the row limit no longer 413s at submission (the request
    was accepted); instead the JOB terminates as failed with TOO_MANY_ROWS.
    The edge function surfaces this via the poll loop."""
    import clusteriq_engine.api as api

    api._result_cache = ResultCache(api._CACHE_ENTRIES, api._CACHE_BYTES)
    api._job_store = JobStore(max_jobs=api._JOB_STORE_MAX, ttl_seconds=api._JOB_TTL_SECONDS)
    monkeypatch.setattr(api, "HEAVY_JOB_ROW_LIMIT", 3)  # our CSV has 5 rows
    client = TestClient(api.app)

    submit = _submit(client)
    assert submit.status_code == 202  # accepted — we don't know row count until we parse
    job_id = submit.json()["job_id"]

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = client.get(f"/jobs/{job_id}").json()["status"]
        if status in ("completed", "failed", "canceled"):
            break
        time.sleep(0.05)
    final = client.get(f"/jobs/{job_id}").json()
    assert final["status"] == "failed"
    assert final["error_code"] == "TOO_MANY_ROWS"
    # /result on a failed job must 422 with the same code — this is what the
    # edge function forwards to the frontend.
    r = client.get(f"/jobs/{job_id}/result")
    assert r.status_code == 422
    assert r.json()["detail"]["error"]["code"] == "TOO_MANY_ROWS"


# ------------------------------- health ----------------------------------- #

def test_health_reports_runtime_stats(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert "active_jobs" in body and "max_jobs" in body
    assert "cache" in body and "enabled" in body["cache"]
    # New in v1.4.0: job store stats visible for ops-at-a-glance.
    assert "jobs" in body
    assert set(body["jobs"].keys()) >= {"total", "by_status", "max_jobs", "ttl_seconds"}
