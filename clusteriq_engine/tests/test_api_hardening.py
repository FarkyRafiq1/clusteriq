"""Hardening tests: rate limits, concurrency slots, upload caps, client IPs."""
import asyncio
import io
import json

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile
from starlette.requests import Request

from clusteriq_engine import api, ingest
from clusteriq_engine.errors import UserError
from clusteriq_engine.hardening import (
    JobSlots,
    RateLimiter,
    ServerBusy,
    client_ip,
    read_upload_limited,
)


def _request(headers=None, client=("203.0.113.5", 1234)):
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http", "method": "POST", "path": "/cluster",
        "query_string": b"", "headers": raw, "client": client,
        "scheme": "http", "server": ("test", 80),
    }
    return Request(scope)


def _call_cluster(req, upload):
    return asyncio.run(
        api.cluster_file(
            request=req, file=upload,
            keyword_column="keyword", volume_column="volume",
            difficulty_column="difficulty", rank_column="position",
            url_column="url",
        )
    )


# --------------------------------------------------------------------- #
# RateLimiter
# --------------------------------------------------------------------- #
def test_rate_limiter_allows_burst_then_blocks_then_refills():
    clock = [0.0]
    limiter = RateLimiter(per_minute=3, time_func=lambda: clock[0])
    assert all(limiter.allow("ip1") for _ in range(3))
    assert not limiter.allow("ip1")
    assert limiter.retry_after("ip1") >= 1
    assert limiter.allow("ip2")  # other clients unaffected
    clock[0] += 20.0  # 3/min -> one token per 20s
    assert limiter.allow("ip1")
    assert not limiter.allow("ip1")


def test_rate_limiter_disabled_when_zero():
    limiter = RateLimiter(per_minute=0)
    assert all(limiter.allow("ip") for _ in range(1000))


# --------------------------------------------------------------------- #
# JobSlots
# --------------------------------------------------------------------- #
def test_job_slots_cap_and_release():
    slots = JobSlots(max_jobs=1)
    with slots.acquire():
        assert slots.active == 1
        with pytest.raises(ServerBusy):
            with slots.acquire():
                pass  # pragma: no cover
    assert slots.active == 0
    with slots.acquire():  # released slot is reusable
        assert slots.active == 1


def test_job_slots_zero_means_unlimited():
    slots = JobSlots(max_jobs=0)
    with slots.acquire(), slots.acquire(), slots.acquire():
        assert slots.active == 0  # not tracked when unlimited


# --------------------------------------------------------------------- #
# client_ip
# --------------------------------------------------------------------- #
def test_client_ip_takes_rightmost_forwarded_entry():
    req = _request({"X-Forwarded-For": "6.6.6.6, 198.51.100.7"})
    assert client_ip(req, trust_proxy_headers=True) == "198.51.100.7"


def test_client_ip_ignores_spoofable_headers_when_untrusted():
    req = _request({"X-Forwarded-For": "6.6.6.6"})
    assert client_ip(req, trust_proxy_headers=False) == "203.0.113.5"


def test_client_ip_falls_back_to_socket_then_unknown():
    assert client_ip(_request(), trust_proxy_headers=True) == "203.0.113.5"
    assert client_ip(_request(client=None), trust_proxy_headers=True) == "unknown"


# --------------------------------------------------------------------- #
# read_upload_limited
# --------------------------------------------------------------------- #
def test_oversized_stream_cut_off_at_first_excess_chunk():
    upload = UploadFile(io.BytesIO(b"x" * (3 * 1024 * 1024)), filename="big.csv")
    with pytest.raises(UserError) as err:
        asyncio.run(read_upload_limited(upload, max_bytes=2 * 1024 * 1024))
    assert err.value.code == "FILE_TOO_LARGE"


def test_over_declared_content_length_rejected_before_reading():
    upload = UploadFile(io.BytesIO(b"tiny"), filename="t.csv")
    with pytest.raises(UserError):
        asyncio.run(read_upload_limited(upload, max_bytes=1024, declared_length=str(10**9)))


def test_normal_upload_reads_fully():
    payload = b"keyword,volume\nbest showers,100\n"
    upload = UploadFile(io.BytesIO(payload), filename="ok.csv")
    assert asyncio.run(read_upload_limited(upload, max_bytes=1024)) == payload


# --------------------------------------------------------------------- #
# Endpoint behaviour
# --------------------------------------------------------------------- #
def test_endpoint_rate_limit_returns_429_with_retry_after(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, "_rate_limiter", RateLimiter(2, time_func=lambda: clock[0]))
    payload = b"keyword,volume\nbest showers,100\nshower ideas,50\n"
    for _ in range(2):
        response = _call_cluster(_request(), UploadFile(io.BytesIO(payload), filename="a.csv"))
        # v1.4.0: submission returns 202 with a job_id, not 200 with results.
        assert response.status_code == 202
        assert "job_id" in json.loads(response.body)
    with pytest.raises(HTTPException) as err:
        _call_cluster(_request(), UploadFile(io.BytesIO(payload), filename="a.csv"))
    assert err.value.status_code == 429
    assert err.value.detail["error"]["code"] == "RATE_LIMITED"
    assert int(err.value.headers["Retry-After"]) >= 1


def test_job_slots_no_longer_gate_async_workers(monkeypatch):
    """v1.4.0 note: _job_slots was the sync-era admission control (fail-fast
    503 for over-capacity requests). Under async, admission is queued via the
    worker semaphore instead, so an occupied _job_slot does NOT cause worker
    failure. This test locks in that expectation so nobody re-adds a
    _job_slots.acquire() inside the worker."""
    import time as _time
    from clusteriq_engine.jobs import JobStore
    monkeypatch.setattr(api, "_rate_limiter", RateLimiter(0))
    monkeypatch.setattr(api, "_job_slots", JobSlots(1))
    monkeypatch.setattr(api, "_job_store", JobStore(max_jobs=10, ttl_seconds=60))
    payload = b"keyword,volume\nbest showers,100\n"
    with api._job_slots.acquire():
        submit = _call_cluster(_request(), UploadFile(io.BytesIO(payload), filename="a.csv"))
        assert submit.status_code == 202
        job_id = json.loads(submit.body)["job_id"]
        deadline = _time.monotonic() + 5
        while _time.monotonic() < deadline:
            job = api._job_store.get(job_id)
            if job and job.status.value in ("completed", "failed", "canceled"):
                break
            _time.sleep(0.02)
        job = api._job_store.get(job_id)
        # The worker completes successfully despite the "held" job slot.
        assert job.status.value == "completed"


def test_endpoint_rejects_oversized_declared_upload(monkeypatch):
    monkeypatch.setattr(api, "_rate_limiter", RateLimiter(0))
    payload = b"keyword\nx\n"
    req = _request({"Content-Length": str(10**9)})
    with pytest.raises(HTTPException) as err:
        _call_cluster(req, UploadFile(io.BytesIO(payload), filename="a.csv"))
    assert err.value.status_code == 400
    assert err.value.detail["error"]["code"] == "FILE_TOO_LARGE"


def test_endpoint_success_body_still_strict_json(monkeypatch):
    import time as _time
    from clusteriq_engine.jobs import JobStore
    monkeypatch.setattr(api, "_rate_limiter", RateLimiter(0))
    monkeypatch.setattr(api, "_job_store", JobStore(max_jobs=10, ttl_seconds=60))
    payload = b"keyword,volume\nbest showers,\nshower ideas,50\n"  # missing volume
    submit = _call_cluster(_request(), UploadFile(io.BytesIO(payload), filename="a.csv"))
    assert submit.status_code == 202
    job_id = json.loads(submit.body)["job_id"]
    # Drive the job to completion via the in-process store (no HTTP client
    # needed for this — we're calling api-layer functions directly).
    deadline = _time.monotonic() + 10
    while _time.monotonic() < deadline:
        job = api._job_store.get(job_id)
        if job and job.status.value in ("completed", "failed"):
            break
        _time.sleep(0.02)
    body_dict = api._job_store.pop_result(job_id)
    assert body_dict is not None, "job did not complete successfully"
    # Round-trip through strict JSON — no bare NaN slipping in.
    body = json.loads(
        json.dumps(body_dict),
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)),
    )
    assert body["summary"]["keywords"] == 2


# --------------------------------------------------------------------- #
# xlsx zip-bomb guard
# --------------------------------------------------------------------- #
def test_xlsx_uncompressed_size_guard(monkeypatch):
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame({"Keyword": ["a"] * 50, "Volume": [1] * 50}).to_excel(buf, index=False)
    monkeypatch.setattr(ingest, "MAX_XLSX_UNCOMPRESSED_BYTES", 500)
    with pytest.raises(UserError) as err:
        ingest.read_table(buf.getvalue(), "bomb.xlsx")
    assert err.value.code == "FILE_TOO_LARGE"
