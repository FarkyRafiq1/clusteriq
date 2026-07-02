"""NaN handling + strict-JSON regression tests.

The strict parser below rejects bare NaN/Infinity literals exactly like a
browser's JSON.parse, which is what the Lovable frontend uses.
"""
import json

import numpy as np
import pandas as pd
import pytest

from clusteriq_engine.pipeline import ClusterPipeline, run_pipeline


def _strict_loads(text: str):
    def _reject(constant):  # pragma: no cover - trivial
        raise ValueError(f"strict JSON rejects {constant}")

    return json.loads(text, parse_constant=_reject)


def test_nan_keywords_never_become_nan_clusters():
    """pandas 2.x turned NaN keywords into the literal string 'nan' and
    clustered them; pandas 3.x behaved differently. Both must drop them."""
    df = pd.DataFrame(
        {
            "keyword": ["best showers", "shower ideas", np.nan, None, "  ", "nan", "NULL"],
            "volume": [100, 50, 999, 999, 1, 999, 999],
        }
    )
    result = run_pipeline(df)
    keywords = set(result["rows"]["keyword"])
    assert keywords == {"best showers", "shower ideas"}
    for cluster in result["clusters"].to_dict(orient="records"):
        assert cluster["canonical_keyword"].lower() not in {"nan", "none", "null"}


def test_nan_urls_with_duplicate_keywords_do_not_crash():
    """pandas 3.x: ' | '.join over a float NaN raised TypeError."""
    df = pd.DataFrame(
        {
            "keyword": ["best showers", "best showers", "shower ideas"],
            "volume": [100, 100, 50],
            "url": ["https://x.co/a", np.nan, np.nan],
        }
    )
    result = run_pipeline(df)
    rows = result["rows"]
    assert rows.loc[rows["keyword"] == "best showers", "url"].iloc[0] == "https://x.co/a"
    assert rows.loc[rows["keyword"] == "shower ideas", "url"].iloc[0] == ""


def test_all_nan_difficulty_gives_finite_opportunity_score():
    df = pd.DataFrame({"keyword": ["combi boiler", "combi boiler guide"], "volume": [100, 50]})
    result = run_pipeline(df)
    for cluster in result["clusters"].to_dict(orient="records"):
        assert np.isfinite(cluster["opportunity_score"])


def test_full_api_body_is_strict_json_safe():
    """The exact failure the frontend saw: bare NaN literals in the body."""
    from clusteriq_engine.api import build_cluster_response

    df = pd.DataFrame(
        {
            "keyword": ["best boilers", "boiler ideas", "combi boiler guide", "buy combi boiler"],
            "volume": [1000, np.nan, 300, np.nan],
            # no difficulty / position / url columns at all
        }
    )
    body = build_cluster_response(ClusterPipeline(), df)
    text = json.dumps(body)
    assert "NaN" not in text
    parsed = _strict_loads(text)  # would raise on any bare NaN/Infinity
    assert parsed["summary"]["keywords"] == 4
    assert {"summary", "column_mapping", "columns_detected", "clusters", "rows"} <= set(parsed)


def test_preview_style_sample_rows_are_json_safe():
    from clusteriq_engine.utils import df_records_json_safe

    df = pd.DataFrame({"keyword": ["a"], "volume": [np.nan], "difficulty": [np.inf]})
    records = df_records_json_safe(df)
    text = json.dumps(records)
    assert "NaN" not in text and "Infinity" not in text
    assert records[0]["volume"] is None and records[0]["difficulty"] is None
    from clusteriq_engine.utils import scalar_or_none

    assert scalar_or_none(np.inf) is None
    assert scalar_or_none(float("nan")) is None
    assert scalar_or_none(3.14159, 2) == pytest.approx(3.14)
