"""Tests for the async job store and endpoints added in v1.4.0.

These verify the contract the cluster-proxy edge function relies on:
- submission returns {job_id, status, 202}
- poll returns snapshots with status/progress and 404s for unknown ids
- cancel is idempotent, works on queued jobs, tolerates already-terminal
- restart-loss (unknown id) surfaces a recoverable error code
- job store enforces its bounds (max_jobs, TTL)
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from clusteriq_engine.jobs import Job, JobStatus, JobStore, TERMINAL


CSV = (
    b"keyword,volume\n"
    b"best running shoes,5400\n"
    b"buy running shoes online,3200\n"
    b"how to choose running shoes,1900\n"
    b"trail running shoes review,720\n"
)


@pytest.fixture()
def client():
    import clusteriq_engine.api as api
    from clusteriq_engine.cache import ResultCache
    api._result_cache = ResultCache(api._CACHE_ENTRIES, api._CACHE_BYTES)
    api._job_store = JobStore(max_jobs=api._JOB_STORE_MAX, ttl_seconds=api._JOB_TTL_SECONDS)
    return TestClient(api.app)


# ==================================================================== #
# JobStore unit tests
# ==================================================================== #

def test_store_create_get_snapshot():
    s = JobStore()
    j = s.create(row_count=42)
    snap = s.get(j.id).snapshot()
    assert snap["job_id"] == j.id
    assert snap["status"] == "queued"
    assert snap["progress"] == 0
    assert snap["row_count"] == 42


def test_store_status_transitions_are_monotonic():
    s = JobStore()
    j = s.create()
    s.mark_processing(j.id)
    assert s.get(j.id).status is JobStatus.PROCESSING
    s.mark_completed(j.id, {"summary": {}})
    assert s.get(j.id).status is JobStatus.COMPLETED
    # A late failure on a completed job must be ignored (no going back).
    s.mark_failed(j.id, "X", "should be ignored")
    assert s.get(j.id).status is JobStatus.COMPLETED
    assert s.get(j.id).error_message is None


def test_progress_is_monotonic_and_capped():
    s = JobStore()
    j = s.create()
    s.mark_processing(j.id)
    s.update_progress(j.id, 40, "svd")
    s.update_progress(j.id, 20, "reading")  # regression — ignored
    s.update_progress(j.id, 150, "??")  # over-100 — clamped
    j2 = s.get(j.id)
    assert j2.progress == 99  # clamped just below 100 until mark_completed
    assert j2.stage == "??"


def test_pop_result_is_consume_once():
    s = JobStore()
    j = s.create()
    s.mark_processing(j.id)
    s.mark_completed(j.id, {"body": "x"})
    assert s.pop_result(j.id) == {"body": "x"}
    assert s.pop_result(j.id) is None  # already consumed
    # But the job record itself is still there in COMPLETED.
    assert s.get(j.id).status is JobStatus.COMPLETED


def test_cancel_queued_is_immediate():
    s = JobStore()
    j = s.create()
    assert s.request_cancel(j.id) is True
    assert s.get(j.id).status is JobStatus.CANCELED


def test_cancel_processing_sets_flag_only():
    s = JobStore()
    j = s.create()
    s.mark_processing(j.id)
    assert s.request_cancel(j.id) is True
    # Still processing until the worker checks the flag.
    assert s.get(j.id).status is JobStatus.PROCESSING
    assert s.is_cancel_requested(j.id) is True


def test_cancel_terminal_job_is_noop():
    s = JobStore()
    j = s.create()
    s.mark_processing(j.id); s.mark_completed(j.id, {})
    assert s.request_cancel(j.id) is False
    assert s.get(j.id).status is JobStatus.COMPLETED


def test_ttl_sweeps_terminal_jobs():
    s = JobStore(ttl_seconds=0)  # instant expiry
    j = s.create()
    s.mark_processing(j.id); s.mark_completed(j.id, {})
    # Next access triggers a sweep.
    assert s.get(j.id) is None


def test_max_jobs_evicts_terminal_first():
    s = JobStore(max_jobs=2)
    a = s.create(); s.mark_processing(a.id); s.mark_completed(a.id, {})
    b = s.create()  # queued/live
    c = s.create()  # forces eviction; the terminal 'a' should go, not the live 'b'
    assert s.get(a.id) is None
    assert s.get(b.id) is not None
    assert s.get(c.id) is not None


# ==================================================================== #
# Endpoint contract (what cluster-proxy actually calls)
# ==================================================================== #

def _submit(client: TestClient):
    return client.post(
        "/cluster",
        files={"file": ("kw.csv", CSV, "text/csv")},
        data={"keyword_column": "keyword", "volume_column": "volume"},
    )


def _poll_until_terminal(client, job_id, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in ("completed", "failed", "canceled"):
            return body
        time.sleep(0.02)
    pytest.fail(f"job {job_id} did not terminate")


def test_submit_returns_202_with_job_id(client):
    r = _submit(client)
    assert r.status_code == 202
    assert "job_id" in r.json()


def test_poll_unknown_job_returns_job_not_found(client):
    r = client.get("/jobs/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["error"]["code"] == "JOB_NOT_FOUND"


def test_result_before_completion_returns_job_not_ready(client):
    """Fast-race case: the edge function may poll /result before the job is
    done. We must return a distinct 409 with the current status/progress so
    the poller can keep waiting rather than treating it as an error.

    We drive this by creating a job directly in the store rather than via the
    endpoint — the endpoint's background worker races the assertion and can
    complete a small test CSV faster than we can poll."""
    import clusteriq_engine.api as api
    job = api._job_store.create()
    api._job_store.mark_processing(job.id, stage="vectorizing")
    r = client.get(f"/jobs/{job.id}/result")
    assert r.status_code == 409
    err = r.json()["detail"]["error"]
    assert err["code"] == "JOB_NOT_READY"
    assert err["status"] == "processing"


def test_result_of_completed_job_matches_persistresults_shape(client):
    submit = _submit(client).json()
    _poll_until_terminal(client, submit["job_id"])
    body = client.get(f"/jobs/{submit['job_id']}/result").json()
    assert "clusters" in body and "rows" in body and "summary" in body


def test_cancel_endpoint_delete_and_post_both_work(client):
    """Edge function tries DELETE first, POST /cancel as fallback."""
    submit1 = _submit(client).json()
    r1 = client.delete(f"/jobs/{submit1['job_id']}")
    assert r1.status_code == 200

    submit2 = _submit(client).json()
    r2 = client.post(f"/jobs/{submit2['job_id']}/cancel")
    assert r2.status_code == 200


def test_cancel_unknown_job_returns_404(client):
    """DELETE on an unknown id should be a clean 404, not a false success."""
    r = client.delete("/jobs/nope")
    assert r.status_code == 404


def test_repeated_cancel_is_idempotent(client):
    """Cancel called twice on the same live job: second call reports no-op
    truthfully, doesn't error. Uses the store directly to avoid racing the
    background worker."""
    import clusteriq_engine.api as api
    job = api._job_store.create()
    api._job_store.mark_processing(job.id)
    r1 = client.delete(f"/jobs/{job.id}")
    assert r1.status_code == 200 and r1.json()["canceled"] is True
    # First cancel just sets the flag on a processing job (worker would check
    # it) — but the store's status is still PROCESSING because no worker is
    # running to promote it. Simulate the worker noticing:
    api._job_store.mark_failed(job.id, "CANCELED", "Canceled by user")
    r2 = client.delete(f"/jobs/{job.id}")
    assert r2.status_code == 200
    assert r2.json()["canceled"] is False  # already terminal


def test_health_includes_jobs_stats(client):
    _submit(client)  # populate at least one job
    body = client.get("/health").json()
    assert body["jobs"]["total"] >= 1
    assert isinstance(body["jobs"]["by_status"], dict)
