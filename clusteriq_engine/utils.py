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


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out
