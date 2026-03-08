import pandas as pd

from clusteriq_engine.pipeline import run_pipeline


def test_pipeline_groups_related_keywords():
    df = pd.DataFrame(
        {
            "keyword": [
                "best ai writing tools",
                "free ai writing tools",
                "ai writing tools comparison",
                "crm software pricing",
                "crm software cost",
                "best crm software",
            ],
            "volume": [3200, 900, 700, 1400, 1200, 2400],
            "difficulty": [25, 18, 22, 35, 30, 42],
            "position": [14, 19, 12, 11, 10, 18],
        }
    )
    result = run_pipeline(df)
    clusters = result["clusters"]
    assert clusters.shape[0] >= 2
    ai_cluster = clusters[clusters["canonical_keyword"].str.contains("ai writing", case=False, na=False)]
    assert not ai_cluster.empty
