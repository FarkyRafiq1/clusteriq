"""File ingestion: robust parsing of user-uploaded keyword exports.

Design rules:
- Detect file type by magic bytes, never by filename (users rename files).
- Detect text encoding (Ahrefs exports UTF-16; Excel writes UTF-8 BOMs).
- Sniff the delimiter (comma / tab / semicolon) instead of assuming comma.
- Read CSV cells as strings and convert numerics ourselves, so behaviour
  does not depend on pandas' per-file type guessing.
- Resolve column names case-insensitively with an alias table covering the
  common SEO tools (Ahrefs, Semrush, GSC, Moz, ...), while an explicit user
  mapping always wins.
"""
from __future__ import annotations

import io
import re
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .errors import UserError

MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB
MAX_XLSX_UNCOMPRESSED_BYTES = 300 * 1024 * 1024  # zip-bomb guard for workbooks
MAX_ROWS = 100_000

# Strings that mean "no value" when found in keyword / url cells.
JUNK_STRINGS = {"", "nan", "none", "null", "n/a", "na", "-", "--"}

# role -> known header spellings (compared after _norm_header).
COLUMN_ALIASES: Dict[str, List[str]] = {
    "keyword": [
        "keyword", "keywords", "query", "queries", "top queries", "search term",
        "search query", "search terms", "term", "kw",
    ],
    "volume": [
        "volume", "search volume", "sv", "monthly volume", "monthly searches",
        "avg. monthly searches", "avg monthly searches", "impressions",
    ],
    "difficulty": [
        "difficulty", "kd", "kd%", "keyword difficulty", "seo difficulty",
        "difficulty%", "competition",
    ],
    "position": [
        "position", "current position", "rank", "ranking", "pos",
        "avg position", "avg. position", "average position", "serp position",
    ],
    "url": [
        "url", "current url", "page", "landing page", "top page", "ranking url",
        "page url", "final url",
    ],
}

_XLSX_MAGIC = b"PK\x03\x04"
_XLS_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def _norm_header(header: str) -> str:
    return re.sub(r"\s+", " ", str(header).replace("\ufeff", "")).strip().lower()


def _decode_text(file_bytes: bytes) -> str:
    """Decode bytes using detected charset; strip any BOM that survives."""
    try:
        from charset_normalizer import from_bytes

        best = from_bytes(file_bytes).best()
        encoding = best.encoding if best else "utf-8"
    except ImportError:  # pragma: no cover - dependency is pinned
        encoding = "utf-8"
    text = file_bytes.decode(encoding, errors="replace")
    return text.lstrip("\ufeff")


def _read_delimited(text: str) -> pd.DataFrame:
    """Read CSV/TSV text, sniffing among real delimiters only.

    csv.Sniffer is restricted to , \\t ; | — unrestricted sniffing can pick a
    letter as the delimiter on single-column files. If sniffing fails
    (e.g. one column), fall back to comma.
    """
    import csv

    read_kwargs = dict(
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        on_bad_lines="skip",
    )
    try:
        dialect = csv.Sniffer().sniff(text[:65536], delimiters=",\t;|")
        sep = dialect.delimiter
    except csv.Error:
        sep = ","
    try:
        return pd.read_csv(io.StringIO(text), sep=sep, engine="python", **read_kwargs)
    except Exception as exc:
        raise UserError(
            "UNPARSEABLE_FILE",
            "Could not parse the file as CSV/TSV. Please export as CSV, TSV or XLSX.",
        ) from exc


def read_table(file_bytes: bytes, filename: str = "upload") -> pd.DataFrame:
    """Parse an uploaded file (xlsx / xls / csv / tsv) into a DataFrame."""
    if not file_bytes or not file_bytes.strip():
        raise UserError("EMPTY_FILE", "The uploaded file is empty.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise UserError(
            "FILE_TOO_LARGE",
            f"The file is larger than the {MAX_UPLOAD_BYTES // 1_048_576} MB upload limit.",
        )

    if file_bytes[:4] == _XLSX_MAGIC:
        # Pre-check: xlsx is a zip; a crafted "bomb" can expand a tiny upload
        # into gigabytes in memory. Reject on declared uncompressed size
        # before handing anything to openpyxl.
        try:
            import zipfile

            with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
                declared = sum(info.file_size for info in zf.infolist())
            if declared > MAX_XLSX_UNCOMPRESSED_BYTES:
                raise UserError(
                    "FILE_TOO_LARGE",
                    "The workbook expands to more than "
                    f"{MAX_XLSX_UNCOMPRESSED_BYTES // 1_048_576} MB when opened. "
                    "Export the data as CSV instead.",
                )
        except UserError:
            raise
        except Exception:
            pass  # not a readable zip: let openpyxl produce the parse error
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        except Exception as exc:
            raise UserError(
                "UNPARSEABLE_XLSX",
                "The file looks like an Excel workbook but could not be read. "
                "Try re-saving it as .xlsx or exporting to CSV.",
            ) from exc
    elif file_bytes[:8] == _XLS_MAGIC:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
        except ImportError as exc:  # pragma: no cover - dependency is pinned
            raise UserError(
                "XLS_NOT_SUPPORTED",
                "Legacy .xls support is unavailable on this server. "
                "Please re-save the file as .xlsx or CSV.",
            ) from exc
        except Exception as exc:
            raise UserError(
                "UNPARSEABLE_XLS",
                "The legacy .xls file could not be read. Please re-save as .xlsx or CSV.",
            ) from exc
    else:
        df = _read_delimited(_decode_text(file_bytes))

    if df is None or df.shape[1] == 0 or df.dropna(how="all").empty:
        raise UserError("NO_ROWS", "No data rows were found in the file.")
    if len(df) > MAX_ROWS:
        raise UserError(
            "TOO_MANY_ROWS",
            f"The file has {len(df):,} rows; the limit is {MAX_ROWS:,}. "
            "Split the export and upload it in parts.",
            rows=len(df),
        )

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


def resolve_columns(
    df: pd.DataFrame, requested: Dict[str, Optional[str]]
) -> Dict[str, Optional[str]]:
    """Map roles (keyword/volume/difficulty/position/url) to real columns.

    An explicit user mapping wins when the column exists (matched
    case-insensitively); otherwise the alias table auto-detects. Only the
    keyword role is mandatory.
    """
    lookup = {_norm_header(c): c for c in df.columns}
    resolved: Dict[str, Optional[str]] = {}
    for role in COLUMN_ALIASES:
        wanted = requested.get(role)
        column: Optional[str] = None
        if wanted and _norm_header(wanted) in lookup:
            column = lookup[_norm_header(wanted)]
        else:
            for alias in COLUMN_ALIASES[role]:
                if alias in lookup:
                    column = lookup[alias]
                    break
        resolved[role] = column

    if resolved["keyword"] is None:
        raise UserError(
            "KEYWORD_COLUMN_NOT_FOUND",
            "Could not find a keyword column in the file. "
            "Pick one of the detected columns and map it explicitly.",
            columns_detected=[str(c) for c in df.columns],
        )
    return resolved


def clean_numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    """Convert a column to floats, tolerating '1,900', '34%', '£1 200', '-', 'n/a'.

    Missing/unparseable values become NaN (never silently 0).
    """
    if not col or col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    cleaned = (
        df[col]
        .astype("string")
        .str.strip()
        .str.replace(r"[,\s\u00a0£$€%]", "", regex=True)
        .str.replace(r"(?i)^(-+|n/?a|none|null)$", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce").astype(float)
