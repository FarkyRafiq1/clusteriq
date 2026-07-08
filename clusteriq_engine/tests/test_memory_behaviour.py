"""Memory-behaviour tests for v1.3.1.

Two guarantees worth locking in:
1. Blockwise candidate-pair scoring is arithmetic-identical to the old
   all-at-once gather (block size must not change clustering results).
2. Lean mode (float32 embeddings) activates at the row threshold and the
   pipeline still produces a valid, fully-populated result.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import clusteriq_engine.pipeline as pl
from clusteriq_engine.pipeline import ClusterPipeline
from clusteriq_engine.schemas import PipelineConfig


KEYWORDS = [
    "best running shoes", "buy running shoes", "running shoes review",
    "cheap running shoes", "trail running shoes", "crm software pricing",
    "best crm software", "crm software demo", "email marketing tools",
    "best email marketing", "email marketing pricing", "standing desk review",
]


def _df():
    return pd.DataFrame({
        "keyword": KEYWORDS,
        "volume": np.arange(100, 100 + len(KEYWORDS)) * 10,
        "difficulty": np.linspace(10, 60, len(KEYWORDS)),
        "position": np.linspace(3, 30, len(KEYWORDS)),
        "url": [f"https://ex.com/{i}" for i in range(len(KEYWORDS))],
    })


def _labels(result):
    # run() returns DataFrames; the API layer converts to record dicts.
    return result["rows"]["cluster_id"].tolist()


def test_blockwise_pair_scoring_is_result_identical(monkeypatch):
    """Tiny block size must produce byte-identical clustering to a huge one."""
    pipe = ClusterPipeline(PipelineConfig())

    monkeypatch.setattr(pl, "SIM_PAIR_BLOCK", 10**9)  # effectively one block
    big = pipe.run(_df())

    monkeypatch.setattr(pl, "SIM_PAIR_BLOCK", 2)  # pathologically small blocks
    small = pipe.run(_df())

    assert _labels(big) == _labels(small)
    assert big["summary"] == small["summary"]
    # Cluster-level numerics identical too.
    assert big["clusters"]["cluster_quality"].tolist() == \
        small["clusters"]["cluster_quality"].tolist()


def test_lean_mode_activates_and_produces_valid_output(monkeypatch):
    """Above the threshold, embeddings go float32 and the run still succeeds."""
    monkeypatch.setattr(pl, "LEAN_ROW_THRESHOLD", 3)  # our 12 rows trigger it

    captured = {}
    orig = ClusterPipeline._hybrid_similarity_graph

    def spy(self, lexical_vectors, semantic_vectors):
        captured["dtype"] = semantic_vectors.dtype
        return orig(self, lexical_vectors, semantic_vectors)

    monkeypatch.setattr(ClusterPipeline, "_hybrid_similarity_graph", spy)

    result = ClusterPipeline(PipelineConfig()).run(_df())

    assert captured["dtype"] == np.float32
    assert len(result["rows"]) == len(KEYWORDS)
    assert result["summary"]["keywords"] == len(KEYWORDS)
    # Stronger sanity: on this small, well-separated set, lean (float32) mode
    # must produce identical clustering to the float64 path.
    monkeypatch.setattr(pl, "LEAN_ROW_THRESHOLD", 10**9)  # disable lean
    baseline = ClusterPipeline(PipelineConfig()).run(_df())
    assert result["rows"]["cluster_id"].tolist() == baseline["rows"]["cluster_id"].tolist()


def test_small_inputs_stay_float64_below_threshold():
    """Below the threshold nothing changes: embeddings remain float64."""
    pipe = ClusterPipeline(PipelineConfig())
    lex = pipe._lexical_vectors([k for k in KEYWORDS])
    emb = pipe._semantic_vectors([k for k in KEYWORDS], lex)
    assert emb.dtype == np.float64  # 12 rows << LEAN_ROW_THRESHOLD default


def test_sklearn_working_memory_is_bounded():
    """The module import must cap sklearn's chunked-op scratch allocations."""
    import sklearn

    assert sklearn.get_config()["working_memory"] <= 256  # default env: 64
