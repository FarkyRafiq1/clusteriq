from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s\-/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def stable_slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", normalize_text(text)).strip("-")[:80] or "cluster"


def first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        if str(value).strip():
            return str(value)
    return ""


def df_records_json_safe(df) -> List[dict]:
    """DataFrame -> list of dicts with NaN/NaT replaced by None.

    This is the single chokepoint that guarantees no float NaN ever reaches
    json.dumps (bare NaN literals are invalid JSON and break browsers).
    """
    import pandas as pd

    import numpy as np

    if df is None or len(df) == 0:
        return []
    cleaned = df.replace([np.inf, -np.inf], np.nan)
    safe = cleaned.astype(object).where(pd.notna(cleaned), None)
    return safe.to_dict(orient="records")


def scalar_or_none(value, ndigits: int = 4):
    """Round a numeric scalar, mapping NaN/inf/None to None for JSON safety."""
    import math

    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return round(value, ndigits)


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out
