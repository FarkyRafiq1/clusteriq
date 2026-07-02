"""Pipeline edge cases that previously crashed whole requests or were no-ops."""
import numpy as np
import pandas as pd

from clusteriq_engine.pipeline import ClusterPipeline, run_pipeline
from clusteriq_engine.schemas import PipelineConfig


def test_all_stopword_keywords_do_not_crash_topic_label():
    """'empty vocabulary' from TfidfVectorizer used to 400 the whole request."""
    df = pd.DataFrame({"keyword": ["how to", "what is", "the best of the"]})
    result = run_pipeline(df)
    assert len(result["clusters"]) >= 1
    assert all(isinstance(c["topic_label"], str) and c["topic_label"] for c in
               result["clusters"].to_dict(orient="records"))


def test_single_keyword_upload():
    result = run_pipeline(pd.DataFrame({"keyword": ["combi boiler"]}))
    assert result["summary"]["keywords"] == 1
    top = result["summary"]["top_cluster"]
    assert np.isfinite(top["opportunity_score"])
    assert top["avg_difficulty"] is None  # unknown, not NaN and not fake


def test_min_cluster_size_flags_singletons():
    df = pd.DataFrame(
        {
            "keyword": [
                "best electric showers",
                "electric showers guide",
                "cheap electric showers",
                "zebra wallpaper",  # unrelated singleton
            ],
            "volume": [900, 400, 300, 50],
        }
    )
    result = run_pipeline(df)
    rows = result["rows"]
    zebra = rows[rows["keyword"] == "zebra wallpaper"].iloc[0]
    assert not bool(zebra["is_clustered"])
    assert result["summary"]["unclustered_keywords"] >= 1
    assert (
        result["summary"]["clustered_keywords"] + result["summary"]["unclustered_keywords"]
        == result["summary"]["keywords"]
    )
    clusters = {c["cluster_id"]: c for c in result["clusters"].to_dict(orient="records")}
    assert clusters[int(zebra["cluster_id"])]["is_clustered"] is False


def test_post_split_preserves_every_keyword_exactly_once():
    config = PipelineConfig(post_split_on_intent=True, post_split_on_modifiers=True)
    df = pd.DataFrame(
        {
            "keyword": [
                "best crm software",
                "buy crm software",
                "crm software guide",
                "crm software pricing",
                "crm software review",
                "how to choose crm software",
            ],
            "volume": [100, 90, 80, 70, 60, 50],
        }
    )
    result = run_pipeline(df, config)
    rows = result["rows"]
    assert len(rows) == 6
    assert rows["keyword"].is_unique
    # every row's cluster_id must exist in the clusters table
    cluster_ids = set(result["clusters"]["cluster_id"])
    assert set(rows["cluster_id"]) <= cluster_ids


def test_dedupe_keeps_max_volume_and_merges_urls():
    df = pd.DataFrame(
        {
            "keyword": ["Best Showers", "best showers", "best  showers"],
            "volume": [100, "1,900", np.nan],
            "url": ["https://x.co/a", "https://x.co/b", "https://x.co/a"],
        }
    )
    result = run_pipeline(df)
    row = result["rows"].iloc[0]
    assert result["summary"]["keywords"] == 1
    assert row["volume"] == 1900.0  # "1,900" parsed, max taken — not coerced to 0
    assert row["url"] == "https://x.co/a | https://x.co/b"


def test_export_rows_columns_include_is_clustered():
    result = run_pipeline(pd.DataFrame({"keyword": ["a b", "a b c"]}))
    exported = ClusterPipeline().export_rows(result)
    assert "is_clustered" in exported.columns
