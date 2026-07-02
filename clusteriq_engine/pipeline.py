from __future__ import annotations

import json
import math
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix, csr_matrix, eye as sparse_eye, hstack, spmatrix
from scipy.sparse.csgraph import connected_components
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize as l2_normalize

from . import ingest
from .errors import UserError
from .schemas import DEFAULT_EXPORT_COLUMNS, PipelineConfig
from .utils import (
    df_records_json_safe,
    normalize_text,
    scalar_or_none,
    stable_slug,
    unique_preserve_order,
)


def _fast_mode(series: pd.Series) -> str:
    values, counts = np.unique(series.to_numpy(dtype=object), return_counts=True)
    return str(values[counts.argmax()])


def _nanmean_or_none(series: pd.Series):
    arr = series.to_numpy(dtype=float)
    arr = arr[~np.isnan(arr)]
    return scalar_or_none(arr.mean()) if arr.size else None


class ClusterPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        # Populated by prepare_dataframe; lets the API report what was used.
        self.column_mapping_: Dict[str, Optional[str]] = {}

    # ------------------------------------------------------------------ #
    # Ingestion
    # ------------------------------------------------------------------ #
    def read_table(self, file_bytes: bytes, filename: str = "upload") -> pd.DataFrame:
        """Parse an uploaded xlsx/xls/csv/tsv file. See ingest.read_table."""
        return ingest.read_table(file_bytes, filename)

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config
        mapping = ingest.resolve_columns(
            df,
            {
                "keyword": cfg.keyword_column,
                "volume": cfg.volume_column,
                "difficulty": cfg.difficulty_column,
                "position": cfg.rank_column,
                "url": cfg.url_column,
            },
        )
        self.column_mapping_ = mapping

        # NaN-first keyword handling: mask missing values BEFORE any string
        # conversion so they can never become literal "nan"/"None" keywords.
        keywords = df[mapping["keyword"]]
        out = df.loc[keywords.notna()].copy()
        out["keyword"] = out[mapping["keyword"]].astype(str).str.strip()
        junk = out["keyword"].str.lower().isin(ingest.JUNK_STRINGS)
        out = out.loc[~junk].copy()

        out["normalized_keyword"] = out["keyword"].map(normalize_text)
        out = out[out["normalized_keyword"] != ""].copy()
        if out.empty:
            raise UserError(
                "NO_KEYWORDS",
                "No usable keywords were found in the mapped keyword column.",
                keyword_column=str(mapping["keyword"]),
            )

        out["volume"] = ingest.clean_numeric_series(out, mapping["volume"])
        out["difficulty"] = ingest.clean_numeric_series(out, mapping["difficulty"])
        out["position"] = ingest.clean_numeric_series(out, mapping["position"])

        url_col = mapping["url"]
        if url_col:
            urls = out[url_col].fillna("").astype(str).str.strip()
            urls = urls.where(~urls.str.lower().isin(ingest.JUNK_STRINGS), "")
            out["url"] = urls
        else:
            out["url"] = ""

        out = self._dedupe_keywords(out)
        out = self._add_tags(out)
        return out.reset_index(drop=True)

    def _dedupe_keywords(self, df: pd.DataFrame) -> pd.DataFrame:
        """Collapse duplicate normalized keywords, vectorized.

        volume: max of the duplicates (NaN preserved if all missing —
        never silently coerced to 0). difficulty/position: mean.
        urls: unique non-empty, pipe-joined.
        """
        df = df.copy()
        df["__order"] = np.arange(len(df))
        grouped = (
            df.groupby("normalized_keyword", sort=False)
            .agg(
                keyword=("keyword", "first"),
                volume=("volume", "max"),
                difficulty=("difficulty", "mean"),
                position=("position", "mean"),
                url=("url", lambda s: " | ".join(unique_preserve_order([u for u in s if u]))),
                __order=("__order", "min"),
            )
            .sort_values("__order")
            .drop(columns="__order")
            .reset_index()
        )
        return grouped

    # ------------------------------------------------------------------ #
    # Tagging
    # ------------------------------------------------------------------ #
    def _pattern_flag(self, text: str, patterns: List[str]) -> bool:
        return any(re.search(p, text) for p in patterns)

    def _add_tags(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config
        df = df.copy()
        kw = df["normalized_keyword"].astype(str)
        n = len(df)

        def any_match(patterns: List[str]) -> np.ndarray:
            if not patterns:
                return np.zeros(n, dtype=bool)
            combined = "|".join(f"(?:{p})" for p in patterns)
            return kw.str.contains(combined, regex=True, na=False).to_numpy()

        # Intent: later assignments take precedence (matches the old
        # transactional > commercial > navigational > informational chain).
        intent = np.full(n, "informational", dtype=object)
        intent[any_match(cfg.intent_patterns.navigational)] = "navigational"
        intent[any_match(cfg.intent_patterns.commercial)] = "commercial"
        intent[any_match(cfg.intent_patterns.transactional)] = "transactional"

        modifier_specs = [
            (r"\bbest\b|\btop\b", "best"),
            (r"\bvs\b|\bversus\b|\bcompare\b|\bcomparison\b", "compare"),
            (r"\breview\b|\breviews\b", "review"),
            (r"\balternatives?\b", "alternatives"),
            (r"\bpricing\b|\bprice\b|\bcost\b", "pricing"),
            (r"\bhow\b|\bguide\b|\btutorial\b", "learn"),
        ]
        modifier_masks = [(group, any_match([token])) for token, group in modifier_specs]
        modifier_groups = [
            "+".join(group for group, mask in modifier_masks if mask[i]) or "core"
            for i in range(n)
        ]
        learn_mask = dict(modifier_masks)["learn"]

        page_type = np.full(n, "article", dtype=object)
        page_type[learn_mask] = "guide"
        page_type[intent == "commercial"] = "comparison"
        page_type[intent == "transactional"] = "money-page"

        branded = [normalize_text(term) for term in cfg.branded_terms if term.strip()]
        if branded:
            brand_pattern = "|".join(re.escape(term) for term in branded if term)
            is_brand = kw.str.contains(brand_pattern, regex=True, na=False).to_numpy()
        else:
            is_brand = np.zeros(n, dtype=bool)

        df["intent"] = intent
        df["modifier_group"] = modifier_groups
        df["page_type"] = page_type
        df["is_brand"] = is_brand
        df["topic_root"] = [self._topic_root(k) for k in kw.tolist()]
        return df

    def _topic_root(self, kw: str) -> str:
        stop = {
            "best","top","free","review","reviews","vs","versus","compare","comparison",
            "pricing","price","cost","how","guide","tutorial","what","is","to","for",
            "the","a","an","and","of","in","on","use","choose"
        }
        tokens = [t for t in kw.split() if t not in stop]
        return " ".join(tokens[:4]) if tokens else kw

    # ------------------------------------------------------------------ #
    # Vectors & similarity graph
    # ------------------------------------------------------------------ #
    def _lexical_vectors(self, texts: List[str]) -> csr_matrix:
        cfg = self.config
        specs = [
            ("word", cfg.lexical_ngram_range, cfg.max_features_word),
            ("char_wb", cfg.char_ngram_range, cfg.max_features_char),
        ]
        blocks = []
        for analyzer, ngram_range, max_features in specs:
            try:
                blocks.append(
                    TfidfVectorizer(
                        analyzer=analyzer,
                        ngram_range=ngram_range,
                        max_features=max_features,
                        min_df=1,
                    ).fit_transform(texts)
                )
            except ValueError:
                # e.g. the word analyzer drops single-character tokens
                # ("a b") and raises "empty vocabulary"; carry on with the
                # analyzer(s) that did produce features.
                continue
        if not blocks:
            raise UserError(
                "NO_KEYWORDS",
                "Keywords were too short or empty to analyse after normalization.",
            )
        if len(blocks) == 1:
            return blocks[0].tocsr()
        return hstack(blocks).tocsr()

    def _semantic_vectors(self, texts: List[str], lexical_vectors: csr_matrix) -> np.ndarray:
        cfg = self.config
        if cfg.semantic_backend == "sentence-transformers":
            try:
                from sentence_transformers import SentenceTransformer

                model = SentenceTransformer(cfg.semantic_model)
                emb = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
                return np.asarray(emb, dtype=float)
            except Exception:
                pass
        n_components = int(
            min(cfg.semantic_components, lexical_vectors.shape[0] - 1, lexical_vectors.shape[1] - 1)
        )
        if n_components < 2:
            return lexical_vectors.toarray().astype(float)
        svd = TruncatedSVD(n_components=n_components, random_state=cfg.random_state)
        emb = svd.fit_transform(lexical_vectors)
        norms = np.linalg.norm(emb, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return emb / norms

    def _hybrid_similarity_graph(
        self, lexical_vectors: csr_matrix, semantic_vectors: np.ndarray
    ) -> csr_matrix:
        """Build a SPARSE symmetric similarity graph over k-NN pairs.

        Fully vectorized: lexical rows are L2-normalized once, so cosine
        similarity is a plain sparse dot product — no per-pair sklearn calls
        and no dense n x n matrix (the previous version needed ~20 GB at
        50k keywords and called cosine_similarity once per edge).
        """
        cfg = self.config
        n = semantic_vectors.shape[0]
        if n == 0:
            return csr_matrix((0, 0), dtype=float)
        if n == 1:
            return csr_matrix(np.ones((1, 1), dtype=float))

        neighbor_k = int(min(cfg.neighbor_k, max(1, n - 1)))
        nn = NearestNeighbors(metric="cosine", n_neighbors=neighbor_k + 1)
        nn.fit(semantic_vectors)
        distances, indices = nn.kneighbors(semantic_vectors)

        rows = np.repeat(np.arange(n), neighbor_k)
        cols = indices[:, 1:].ravel()
        sem_sims = np.clip(1.0 - distances[:, 1:].ravel(), 0.0, 1.0)

        lex_norm = l2_normalize(lexical_vectors, norm="l2", copy=True)
        lex_sims = np.asarray(lex_norm[rows].multiply(lex_norm[cols]).sum(axis=1)).ravel()
        lex_sims = np.clip(lex_sims, 0.0, 1.0)

        sims = cfg.hybrid_semantic_weight * sem_sims + cfg.hybrid_lexical_weight * lex_sims
        keep = sims >= cfg.similarity_threshold

        graph = coo_matrix(
            (sims[keep], (rows[keep], cols[keep])), shape=(n, n), dtype=float
        ).tocsr()
        graph = graph.maximum(graph.T)  # symmetrize
        graph = graph.maximum(sparse_eye(n, format="csr", dtype=float))  # self-sim = 1
        return graph.tocsr()

    # ------------------------------------------------------------------ #
    # Clustering
    # ------------------------------------------------------------------ #
    def _cluster_from_graph(self, graph: spmatrix) -> np.ndarray:
        _, labels = connected_components(graph > 0, directed=False, return_labels=True)
        return labels

    def _min_size_mask(self, labels: np.ndarray) -> np.ndarray:
        """True where the keyword's cluster meets min_cluster_size.

        Cluster ids are left untouched (stable, one per component); the flag
        lets the UI/exports separate real clusters from unclustered strays.
        The old implementation only renumbered small clusters, which had no
        observable effect.
        """
        series = pd.Series(labels)
        sizes = series.map(series.value_counts())
        return (sizes >= self.config.min_cluster_size).to_numpy()

    def _post_split(self, df: pd.DataFrame, labels: np.ndarray) -> np.ndarray:
        cfg = self.config
        current_labels = labels.copy()
        next_label = int(current_labels.max()) + 1
        group_cols: List[str] = []
        if cfg.post_split_on_intent:
            group_cols.append("intent")
        group_cols.append("topic_root")
        if cfg.post_split_on_modifiers:
            group_cols.append("modifier_group")

        snapshot = pd.Series(current_labels)
        for cluster_label, members in snapshot.groupby(snapshot, sort=True).groups.items():
            idx = np.asarray(members)
            if len(idx) <= 1:
                continue
            sub = df.iloc[idx]
            grouped = sub.groupby(group_cols, dropna=False, sort=False)
            if grouped.ngroups <= 1:
                continue
            position_of = dict(zip(sub.index, idx))
            first = True
            for _, subset in grouped:
                if first:
                    first = False
                    continue
                for original_idx in subset.index:
                    current_labels[position_of[original_idx]] = next_label
                next_label += 1
        return current_labels

    # ------------------------------------------------------------------ #
    # Cluster descriptors
    # ------------------------------------------------------------------ #
    def _canonical_keyword(self, sub: pd.DataFrame) -> str:
        # Hot path: called once per cluster; keep it numpy, not pandas.
        if len(sub) == 1:
            return str(sub["keyword"].iloc[0])
        volume = np.nan_to_num(sub["volume"].to_numpy(dtype=float), nan=0.0)
        position = sub["position"].to_numpy(dtype=float)
        position = np.clip(np.where(np.isnan(position), 100.0, position), 1.0, 100.0)
        difficulty = sub["difficulty"].to_numpy(dtype=float)
        difficulty = np.where(np.isnan(difficulty), 50.0, difficulty)
        score = (np.log1p(volume) + (21 - position)) / (1 + difficulty)
        best = np.lexsort((-volume, -score))[0]  # score desc, volume tie-break
        return str(sub["keyword"].iloc[best])

    def _topic_label(self, sub: pd.DataFrame) -> str:
        if len(sub) == 1:
            # Fast path: fitting a TfidfVectorizer per singleton cluster
            # dominated runtime on large files. Use the keyword's own
            # non-stopword tokens instead.
            from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

            tokens = [t for t in str(sub["normalized_keyword"].iloc[0]).split()
                      if t not in ENGLISH_STOP_WORDS]
            if tokens:
                return " ".join(tokens[:4]).title()
            return self._canonical_keyword(sub)
        vec = TfidfVectorizer(ngram_range=(1, 2), stop_words="english")
        try:
            tfidf = vec.fit_transform(sub["normalized_keyword"].tolist())
        except ValueError:
            # e.g. "empty vocabulary" when every token is an English stopword
            # ("how to", "what is"). Fall back instead of failing the request.
            return self._canonical_keyword(sub)
        weights = np.asarray(tfidf.sum(axis=0)).ravel()
        terms = np.asarray(vec.get_feature_names_out())
        if len(terms) == 0:
            return self._canonical_keyword(sub)
        top = terms[weights.argsort()[::-1][:3]]
        label = " / ".join([t.title() for t in top])
        return label or self._canonical_keyword(sub)

    def _quality_score(self, graph: spmatrix, idx: np.ndarray, sub: pd.DataFrame) -> float:
        if len(idx) == 1:
            return 0.55
        block = graph[idx][:, idx].toarray()
        upper = block[np.triu_indices_from(block, k=1)]
        cohesion = float(upper.mean()) if upper.size else 0.0
        intent_consistency = float(sub["intent"].value_counts(normalize=True).iloc[0])
        mod_consistency = float(sub["modifier_group"].value_counts(normalize=True).iloc[0])
        score = 0.5 * cohesion + 0.3 * intent_consistency + 0.2 * mod_consistency
        return round(float(min(max(score, 0.0), 1.0)), 4)

    def _opportunity_score(self, sub: pd.DataFrame) -> float:
        """NaN-proof opportunity score.

        Missing difficulty/position fall back to neutral constants instead of
        propagating NaN (Python's max() with a NaN operand returns NaN, which
        previously leaked into the JSON response as an invalid literal).
        """
        vol = sub["volume"].to_numpy(dtype=float)
        volume = float(np.nansum(vol)) if vol.size else 0.0
        diff = sub["difficulty"].to_numpy(dtype=float)
        diff = diff[~np.isnan(diff)]
        difficulty = float(diff.mean()) if diff.size else 50.0
        pos = sub["position"].to_numpy(dtype=float)
        pos = pos[~np.isnan(pos)]
        rank = float(pos.mean()) if pos.size else 50.0
        gap = max(0.0, float(self.config.opportunity_rank_ceiling) - rank)
        raw = (math.log1p(volume) * (1 + gap)) / (1 + max(difficulty, 0.0))
        return round(raw, 4)

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #
    def run(self, df: pd.DataFrame) -> Dict[str, Any]:
        prepared = self.prepare_dataframe(df)
        texts = prepared["normalized_keyword"].tolist()
        lexical = self._lexical_vectors(texts)
        semantic = self._semantic_vectors(texts, lexical)
        graph = self._hybrid_similarity_graph(lexical, semantic)
        labels = self._cluster_from_graph(graph)
        labels = self._post_split(prepared, labels)
        clustered_mask = self._min_size_mask(labels)

        prepared = prepared.copy()
        prepared["cluster_id"] = labels
        prepared["is_clustered"] = clustered_mask

        clusters: List[Dict[str, Any]] = []
        canon_map: Dict[int, str] = {}
        label_map: Dict[int, str] = {}
        intent_map: Dict[int, str] = {}
        page_map: Dict[int, str] = {}
        quality_map: Dict[int, float] = {}
        opp_map: Dict[int, float] = {}
        for cluster_id, sub in prepared.groupby("cluster_id", sort=True):
            cluster_id = int(cluster_id)
            idx = sub.index.to_numpy()
            canonical_keyword = self._canonical_keyword(sub)
            topic_label = self._topic_label(sub)
            quality = self._quality_score(graph, idx, sub)
            opp = self._opportunity_score(sub)
            intent = _fast_mode(sub["intent"])
            page_type = _fast_mode(sub["page_type"])
            clusters.append({
                "cluster_id": cluster_id,
                "cluster_slug": stable_slug(canonical_keyword),
                "canonical_keyword": canonical_keyword,
                "topic_label": topic_label,
                "intent": intent,
                "page_type": page_type,
                "is_clustered": bool(len(sub) >= self.config.min_cluster_size),
                "keyword_count": int(len(sub)),
                "total_volume": float(np.nansum(sub["volume"].to_numpy(dtype=float))),
                "avg_difficulty": _nanmean_or_none(sub["difficulty"]),
                "avg_rank": _nanmean_or_none(sub["position"]),
                "cluster_quality": quality,
                "opportunity_score": opp,
                "keywords": sub["keyword"].tolist(),
                "urls": [u for u in unique_preserve_order(sub["url"].tolist()) if u],
            })
            canon_map[cluster_id] = canonical_keyword
            label_map[cluster_id] = topic_label
            intent_map[cluster_id] = intent
            page_map[cluster_id] = page_type
            quality_map[cluster_id] = quality
            opp_map[cluster_id] = opp

        ids = prepared["cluster_id"]
        prepared["canonical_keyword"] = ids.map(canon_map)
        prepared["topic_label"] = ids.map(label_map)
        prepared["intent"] = ids.map(intent_map)
        prepared["page_type"] = ids.map(page_map)
        prepared["cluster_quality"] = ids.map(quality_map)
        prepared["opportunity_score"] = ids.map(opp_map)

        prepared = prepared.sort_values(
            ["cluster_id", "volume"], ascending=[True, False]
        ).reset_index(drop=True)
        cluster_summary = pd.DataFrame(clusters).sort_values(
            ["opportunity_score", "total_volume", "cluster_quality"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        top_cluster = None
        if not cluster_summary.empty:
            top_cluster = df_records_json_safe(cluster_summary.head(1))[0]

        return {
            "config": asdict(self.config),
            "column_mapping": dict(self.column_mapping_),
            "rows": prepared,
            "clusters": cluster_summary,
            "summary": {
                "keywords": int(len(prepared)),
                "clusters": int(cluster_summary.shape[0]),
                "clustered_keywords": int(prepared["is_clustered"].sum()),
                "unclustered_keywords": int((~prepared["is_clustered"]).sum()),
                "avg_cluster_quality": scalar_or_none(cluster_summary["cluster_quality"].mean()) or 0.0,
                "top_cluster": top_cluster,
            },
        }

    # ------------------------------------------------------------------ #
    # Exports
    # ------------------------------------------------------------------ #
    def export_rows(self, result: Dict[str, Any]) -> pd.DataFrame:
        rows = result["rows"].copy()
        for col in DEFAULT_EXPORT_COLUMNS:
            if col not in rows.columns:
                rows[col] = np.nan
        return rows[DEFAULT_EXPORT_COLUMNS]

    def export_clusters(self, result: Dict[str, Any]) -> pd.DataFrame:
        return result["clusters"].copy()


def run_pipeline(df: pd.DataFrame, config: Optional[PipelineConfig] = None) -> Dict[str, Any]:
    return ClusterPipeline(config).run(df)


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "keyword": [
                "best ai writing tools",
                "free ai writing tools",
                "ai writing tools comparison",
                "ai writing tools review",
                "how to use ai writing tools",
                "crm software pricing",
                "crm software cost",
                "best crm software",
                "crm software review",
                "how to choose crm software",
            ],
            "volume": [3200, 900, 700, 500, 300, 1400, 1200, 2400, 650, 420],
            "difficulty": [25, 18, 22, 20, 16, 35, 30, 42, 34, 28],
            "position": [14, 19, 12, 16, 21, 11, 10, 18, 15, 23],
        }
    )
    result = run_pipeline(sample)
    print(json.dumps(result["summary"], indent=2))
    print(result["clusters"].head().to_string(index=False))
