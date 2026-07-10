"""In-process async job store for clustering.

Design goals, in order:

1. **Correctness first.** State transitions are protected by a lock and are
   monotonic (queued -> processing -> completed/failed/canceled; no going back).
   A caller polling `get()` always sees a consistent snapshot.

2. **Bounded memory.** Two ceilings prevent a leak: `max_jobs` caps live entries
   (LRU eviction of terminal jobs first), and `ttl_seconds` sweeps completed
   jobs older than the retention window. A completed job's *result* is kept in
   the entry until fetched or expired.

3. **Honest cancellation.** Cancel sets a flag the worker checks between
   pipeline stages. The pipeline itself is sklearn C-code inside numpy — we
   cannot interrupt it mid-SVD. So cancel is "stops at the next checkpoint,"
   which is the honest semantic and matches what the frontend already tolerates
   (edge function marks canceled DB-side regardless).

4. **Restart-safe FAILURE, not restart-safe SUCCESS.** In-process state dies
   on Railway restart. That's a genuine limitation of option A (see the
   changelog). What we DO get: the poll endpoint returns 404 for unknown ids,
   which the edge function surfaces to the frontend as a clear failure the
   user can retry — rather than a spinner that hangs forever.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("clusteriq.jobs")


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


TERMINAL = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}


@dataclass
class Job:
    id: str
    status: JobStatus = JobStatus.QUEUED
    progress: int = 0
    stage: str = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    row_count: Optional[int] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    # A cooperative-cancel flag the worker checks between pipeline stages.
    # Not a preemptive interrupt: sklearn C-code can't be killed mid-call.
    _cancel_requested: bool = False

    def snapshot(self) -> Dict[str, Any]:
        """Return the JSON body the edge function's `poll` handler expects."""
        body: Dict[str, Any] = {
            "job_id": self.id,
            "status": self.status.value,
            "progress": self.progress,
            "stage": self.stage,
        }
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.started_at is not None:
            body["started_at"] = self.started_at
        if self.completed_at is not None:
            body["completed_at"] = self.completed_at
        if self.error_message:
            body["error_message"] = self.error_message
        if self.error_code:
            body["error_code"] = self.error_code
        return body


class JobStore:
    """Thread-safe, bounded, TTL-swept in-process job store."""

    def __init__(self, max_jobs: int = 1000, ttl_seconds: int = 3600):
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.RLock()
        self.max_jobs = int(max_jobs)
        self.ttl_seconds = int(ttl_seconds)

    # ---- create / read -------------------------------------------------- #

    def create(self, row_count: Optional[int] = None) -> Job:
        with self._lock:
            self._sweep_locked()
            job_id = uuid.uuid4().hex
            job = Job(id=job_id, row_count=row_count)
            self._jobs[job_id] = job
            self._evict_if_over_locked()
            return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            self._sweep_locked()
            return self._jobs.get(job_id)

    def pop_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Consume-once semantics for successful results: free the memory
        once the caller has retrieved it. The job entry stays (in COMPLETED)
        so subsequent polls still 200, but the (potentially large) `result`
        payload is released."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.status is not JobStatus.COMPLETED or job.result is None:
                return None
            result, job.result = job.result, None
            return result

    # ---- mutate --------------------------------------------------------- #

    def mark_processing(self, job_id: str, stage: str = "loading") -> Optional[Job]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.QUEUED:
                return None
            job.status = JobStatus.PROCESSING
            job.stage = stage
            job.progress = 5
            job.started_at = time.time()
            return job

    def update_progress(self, job_id: str, progress: int, stage: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.PROCESSING:
                return
            # Progress is monotonic — never regress.
            if progress > job.progress:
                job.progress = min(progress, 99)
            job.stage = stage

    def mark_completed(self, job_id: str, result: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status in TERMINAL:
                return  # canceled or already terminal
            job.status = JobStatus.COMPLETED
            job.progress = 100
            job.stage = "completed"
            job.completed_at = time.time()
            job.result = result

    def mark_failed(self, job_id: str, code: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status in TERMINAL:
                return
            job.status = JobStatus.FAILED
            job.completed_at = time.time()
            job.stage = "failed"
            job.error_code = code
            job.error_message = message

    def request_cancel(self, job_id: str) -> bool:
        """Return True if a cancel was queued for a live job. Idempotent."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status in TERMINAL:
                return False
            job._cancel_requested = True
            if job.status is JobStatus.QUEUED:
                # Queued jobs can be canceled immediately; the worker hasn't
                # picked them up. Processing jobs will finish their current
                # stage (see `_check_canceled` in the runner).
                job.status = JobStatus.CANCELED
                job.completed_at = time.time()
                job.stage = "canceled"
                job.error_message = "Canceled by user"
            return True

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job._cancel_requested)

    # ---- housekeeping --------------------------------------------------- #

    def _sweep_locked(self) -> None:
        """Remove terminal jobs older than TTL. Called on every read/write —
        cheap because the dict is small and we only compare timestamps."""
        if not self._jobs:
            return
        cutoff = time.time() - self.ttl_seconds
        expired = [
            jid for jid, j in self._jobs.items()
            if j.status in TERMINAL and (j.completed_at or 0) < cutoff
        ]
        for jid in expired:
            self._jobs.pop(jid, None)
        if expired:
            logger.debug("swept %d expired jobs", len(expired))

    def _evict_if_over_locked(self) -> None:
        """If we're past max_jobs, evict oldest terminal entries first, then
        the oldest queued (should never happen in practice — max_jobs is high)."""
        if len(self._jobs) <= self.max_jobs:
            return
        # Prefer to drop terminal jobs; only touch live ones as a last resort.
        by_priority = sorted(
            self._jobs.values(),
            key=lambda j: (j.status not in TERMINAL, j.created_at),
        )
        to_drop = len(self._jobs) - self.max_jobs
        for j in by_priority[:to_drop]:
            self._jobs.pop(j.id, None)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            self._sweep_locked()
            by_status: Dict[str, int] = {}
            for j in self._jobs.values():
                by_status[j.status.value] = by_status.get(j.status.value, 0) + 1
            return {
                "total": len(self._jobs),
                "by_status": by_status,
                "max_jobs": self.max_jobs,
                "ttl_seconds": self.ttl_seconds,
            }


class CanceledError(Exception):
    """Raised inside a job runner when the caller requested cancellation."""
    pass
