"""Tests for the cost-control / practical-usage features added in v1.3.0:
result caching, the heavy-job row guard, enriched health, and — critically —
that the response contract the frontend depends on is unchanged.
"""
from __future__ import annotations

import importlib
import os

import pytest
from fastapi.testclient import TestClient

from clusteriq_engine.cache import ResultCache, result_cache_key


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
    # Fresh cache per test so one test's cached result can't mask another's
    # first-call behaviour (cache hits are process-wide otherwise).
    api._result_cache = ResultCache(api._CACHE_ENTRIES, api._CACHE_BYTES)
    return TestClient(api.app)


def _post_cluster(client: TestClient, data=None):
    return client.post(
        "/cluster",
        files={"file": ("kw.csv", CSV, "text/csv")},
        data=data or {"keyword_column": "keyword", "volume_column": "volume",
                      "difficulty_column": "difficulty", "rank_column": "position",
                      "url_column": "url"},
    )


# --------------------------- response contract ---------------------------- #

def test_cluster_response_contract_unchanged(client):
    """Every field persistResults reads must be present (frontend depends on it)."""
    body = _post_cluster(client).json()
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
    # New fields are additive and must not be a job_id (that would trigger the
    # frontend's dormant async path).
    assert "job_id" not in body
    assert body["cached"] is False
    assert isinstance(body["timing_ms"], int)


# ------------------------------- caching ---------------------------------- #

def test_identical_request_is_served_from_cache(client):
    first = _post_cluster(client).json()
    assert first["cached"] is False
    second = _post_cluster(client).json()
    assert second["cached"] is True
    # Cached body is otherwise identical (minus the cached flag).
    assert second["summary"] == first["summary"]
    assert second["clusters"] == first["clusters"]


def test_cache_key_varies_with_mapping():
    k1 = result_cache_key(CSV, {"keyword": "keyword"}, {"v": 1})
    k2 = result_cache_key(CSV, {"keyword": "term"}, {"v": 1})
    k3 = result_cache_key(CSV, {"keyword": "keyword"}, {"v": 1})
    assert k1 != k2
    assert k1 == k3


def test_result_cache_lru_and_byte_bounds():
    # Tiny budget: only 1 entry fits -> oldest is evicted.
    cache = ResultCache(max_entries=1, max_total_bytes=0)
    cache.put("a", {"x": 1})
    cache.put("b", {"y": 2})
    assert cache.get("a") is None      # evicted
    assert cache.get("b") == {"y": 2}

    disabled = ResultCache(max_entries=0, max_total_bytes=0)
    disabled.put("a", {"x": 1})
    assert disabled.get("a") is None   # disabled never stores


# --------------------------- heavy-job guard ------------------------------ #

def test_heavy_job_row_limit_returns_413(monkeypatch):
    """A file over the row limit is refused fast with a clear 413, not run."""
    import clusteriq_engine.api as api

    # Fresh cache: a prior test may have cached this CSV, which would short-
    # circuit before the row guard (a cached result is served without reprocessing
    # — correct in prod, but here we want to exercise the guard itself).
    api._result_cache = ResultCache(api._CACHE_ENTRIES, api._CACHE_BYTES)
    monkeypatch.setattr(api, "HEAVY_JOB_ROW_LIMIT", 3)  # our CSV has 5 rows
    client = TestClient(api.app)
    resp = _post_cluster(client)
    assert resp.status_code == 413
    err = resp.json()["detail"]["error"]
    assert err["code"] == "TOO_MANY_ROWS"
    assert err["row_count"] == 5
    assert err["limit"] == 3


# ------------------------------- health ----------------------------------- #

def test_health_reports_runtime_stats(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert "active_jobs" in body and "max_jobs" in body
    assert "cache" in body and "enabled" in body["cache"]
