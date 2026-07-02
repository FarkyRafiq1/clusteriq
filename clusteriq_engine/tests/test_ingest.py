"""Regression tests for ingestion. Each test mirrors a reproduced defect."""
import io
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from clusteriq_engine import ingest
from clusteriq_engine.errors import UserError
from clusteriq_engine.pipeline import ClusterPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _prepare(df, **cfg_overrides):
    from clusteriq_engine.schemas import PipelineConfig

    pipeline = ClusterPipeline(PipelineConfig(**cfg_overrides))
    return pipeline, pipeline.prepare_dataframe(df)


# --------------------------------------------------------------------- #
# Encodings & delimiters
# --------------------------------------------------------------------- #
def test_utf16_tab_separated_ahrefs_export():
    text = (
        "Keyword\tVolume\tKD\tCurrent position\tCurrent URL\n"
        "best electric showers\t1900\t34\t12\thttps://x.co/a\n"
        "electric shower guide\t700\t21\t18\thttps://x.co/b\n"
    )
    df = ingest.read_table(text.encode("utf-16"), "ahrefs.csv")
    assert list(df.columns) == ["Keyword", "Volume", "KD", "Current position", "Current URL"]
    assert len(df) == 2


def test_utf8_bom_header_not_mangled():
    payload = "\ufeffkeyword,volume\nbest showers,1000\nshower ideas,500\n".encode("utf-8-sig")
    df = ingest.read_table(payload, "gsc.csv")
    assert "keyword" in df.columns
    _, prepared = _prepare(df)
    assert len(prepared) == 2


def test_semicolon_delimited_csv():
    df = ingest.read_table(b"keyword;volume\nbest showers;1000\nshower ideas;500\n", "eu.csv")
    assert list(df.columns) == ["keyword", "volume"]
    assert len(df) == 2


def test_plain_tsv():
    df = ingest.read_table(b"keyword\tvolume\nbest showers\t1000\n", "export.tsv")
    assert list(df.columns) == ["keyword", "volume"]


def test_single_column_csv_falls_back_when_sniffer_cannot():
    df = ingest.read_table(b"keyword\nbest showers\nshower ideas\n", "keywords.csv")
    assert list(df.columns) == ["keyword"]
    assert len(df) == 2


def test_latin1_content_still_reads():
    payload = "keyword,volume\ncaf\xe9 style bathroom,90\n".encode("latin-1")
    df = ingest.read_table(payload, "latin.csv")
    assert len(df) == 1


# --------------------------------------------------------------------- #
# Excel, by magic bytes (filenames lie)
# --------------------------------------------------------------------- #
def test_xlsx_detected_by_magic_bytes_even_with_wrong_extension():
    buf = io.BytesIO()
    pd.DataFrame({"Keyword": ["best showers", "shower ideas"], "Volume": [10, 20]}).to_excel(
        buf, index=False
    )
    df = ingest.read_table(buf.getvalue(), "renamed.csv")  # wrong extension on purpose
    assert list(df.columns) == ["Keyword", "Volume"]


def test_legacy_xls_reads():
    df = ingest.read_table((FIXTURES / "legacy.xls").read_bytes(), "legacy.xls")
    assert "Keyword" in df.columns
    assert len(df) == 3


def test_corrupt_xls_gives_user_error():
    with pytest.raises(UserError) as err:
        ingest.read_table(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 64, "broken.xls")
    assert err.value.code == "UNPARSEABLE_XLS"


def test_empty_file_gives_user_error():
    with pytest.raises(UserError) as err:
        ingest.read_table(b"", "empty.csv")
    assert err.value.code == "EMPTY_FILE"


def test_row_cap_enforced(monkeypatch):
    monkeypatch.setattr(ingest, "MAX_ROWS", 3)
    payload = ("keyword\n" + "\n".join(f"kw {i}" for i in range(5))).encode()
    with pytest.raises(UserError) as err:
        ingest.read_table(payload, "big.csv")
    assert err.value.code == "TOO_MANY_ROWS"


# --------------------------------------------------------------------- #
# Column resolution
# --------------------------------------------------------------------- #
def test_title_case_ahrefs_headers_resolve():
    df = pd.DataFrame(
        {
            "Keyword": ["best showers", "shower ideas"],
            "Volume": [100, 50],
            "KD": [30, 20],
            "Current position": [4, 9],
            "Current URL": ["https://x.co/a", "https://x.co/b"],
        }
    )
    pipeline, prepared = _prepare(df)
    assert pipeline.column_mapping_ == {
        "keyword": "Keyword",
        "volume": "Volume",
        "difficulty": "KD",
        "position": "Current position",
        "url": "Current URL",
    }
    assert prepared["volume"].tolist() == [100.0, 50.0]


def test_gsc_top_queries_header_resolves():
    df = pd.DataFrame({"Top queries": ["best showers"], "Impressions": [1200], "Position": [7.2]})
    pipeline, prepared = _prepare(df)
    assert pipeline.column_mapping_["keyword"] == "Top queries"
    assert pipeline.column_mapping_["volume"] == "Impressions"
    assert prepared["position"].iloc[0] == pytest.approx(7.2)


def test_explicit_user_mapping_wins_over_aliases():
    df = pd.DataFrame({"my kw col": ["best showers"], "keyword": ["decoy"], "sv": [10]})
    mapping = ingest.resolve_columns(df, {"keyword": "My KW Col", "volume": None})
    assert mapping["keyword"] == "my kw col"
    assert mapping["volume"] == "sv"


def test_missing_keyword_column_lists_detected_columns():
    df = pd.DataFrame({"foo": [1], "bar": [2]})
    with pytest.raises(UserError) as err:
        ingest.resolve_columns(df, {"keyword": "keyword"})
    assert err.value.code == "KEYWORD_COLUMN_NOT_FOUND"
    assert err.value.context["columns_detected"] == ["foo", "bar"]


# --------------------------------------------------------------------- #
# Numeric cleaning (the "1,900 became 0" bug)
# --------------------------------------------------------------------- #
def test_thousands_separators_percent_and_dashes():
    df = pd.DataFrame(
        {
            "keyword": ["k one", "k two", "k three"],
            "volume": ["1,900", "12,100", "880"],
            "difficulty": ["34%", "n/a", "21"],
            "position": ["3.4", "-", "7"],
        }
    )
    _, prepared = _prepare(df)
    assert prepared["volume"].tolist() == [1900.0, 12100.0, 880.0]
    assert prepared["difficulty"].iloc[0] == 34.0
    assert np.isnan(prepared["difficulty"].iloc[1])
    assert np.isnan(prepared["position"].iloc[1])


def test_currency_and_nbsp_stripped():
    df = pd.DataFrame({"keyword": ["k"], "volume": ["£1\u00a0200"]})
    _, prepared = _prepare(df)
    assert prepared["volume"].iloc[0] == 1200.0
