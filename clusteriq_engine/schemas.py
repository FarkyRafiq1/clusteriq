from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class IntentPatterns:
    informational: List[str] = field(default_factory=lambda: [
        r"\bwhat\b", r"\bhow\b", r"\bwhy\b", r"\bwhen\b", r"\bguide\b",
        r"\btutorial\b", r"\bideas\b", r"\bexamples\b", r"\bmeaning\b"
    ])
    commercial: List[str] = field(default_factory=lambda: [
        r"\bbest\b", r"\btop\b", r"\bcompare\b", r"\bcomparison\b", r"\breview\b",
        r"\balternatives?\b", r"\bvs\b"
    ])
    transactional: List[str] = field(default_factory=lambda: [
        r"\bbuy\b", r"\bprice\b", r"\bcost\b", r"\bpricing\b", r"\bquote\b",
        r"\bdemo\b", r"\btrial\b", r"\bdeal\b", r"\bdiscount\b"
    ])
    navigational: List[str] = field(default_factory=lambda: [
        r"\blogin\b", r"\bsign in\b", r"\bofficial\b", r"\bdocs\b", r"\bhomepage\b"
    ])


@dataclass
class PipelineConfig:
    keyword_column: str = "keyword"
    volume_column: Optional[str] = "volume"
    difficulty_column: Optional[str] = "difficulty"
    rank_column: Optional[str] = "position"
    url_column: Optional[str] = "url"

    semantic_backend: str = "svd"  # svd | sentence-transformers
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    lexical_ngram_range: tuple[int, int] = (1, 2)
    char_ngram_range: tuple[int, int] = (3, 5)
    max_features_word: int = 20000
    max_features_char: int = 30000
    semantic_components: int = 128

    hybrid_lexical_weight: float = 0.4
    hybrid_semantic_weight: float = 0.6
    neighbor_k: int = 15
    similarity_threshold: float = 0.30
    min_cluster_size: int = 2
    post_split_on_intent: bool = True
    post_split_on_modifiers: bool = False

    branded_terms: List[str] = field(default_factory=list)
    intent_patterns: IntentPatterns = field(default_factory=IntentPatterns)

    opportunity_rank_floor: int = 8
    opportunity_rank_ceiling: int = 20

    random_state: int = 42


DEFAULT_EXPORT_COLUMNS = [
    "keyword",
    "normalized_keyword",
    "cluster_id",
    "canonical_keyword",
    "topic_label",
    "intent",
    "page_type",
    "is_clustered",
    "cluster_quality",
    "opportunity_score",
    "volume",
    "difficulty",
    "position",
    "url",
]
