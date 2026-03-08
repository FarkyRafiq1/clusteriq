from __future__ import annotations

import io
import json
import math
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, hstack
from scipy.sparse.csgraph import connected_components
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors

from .schemas import DEFAULT_EXPORT_COLUMNS, PipelineConfig
from .utils import normalize_text, stable_slug, unique_preserve_order


class ClusterPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()

    def read_table(self, file_bytes: bytes, filename: str) -> pd.DataFrame:
        name = filename.lower()
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(file_bytes))
        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
            except Exception:
                continue
        raise ValueError("Could not parse uploaded file as CSV/XLSX")

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config
        if cfg.keyword_column not in df.columns:
            raise ValueError(f"Missing required keyword column: {cfg.keyword_column}")

        out = df.copy()
        out[cfg.keyword_column] = out[cfg.keyword_column].astype(str).fillna("")
        out = out[out[cfg.keyword_column].str.strip() != ""].copy()
        out["keyword"] = out[cfg.keyword_column].astype(str)
        out["normalized_keyword"] = out["keyword"].map(normalize_text)
        out = out[out["normalized_keyword"] != ""].copy()
        out["volume"] = self._safe_numeric(out, cfg.volume_column)
        out["difficulty"] = self._safe_numeric(out, cfg.difficulty_column)
        out["position"] = self._safe_numeric(out, cfg.rank_column)
        out["url"] = out[cfg.url_column].astype(str) if cfg.url_column and cfg.url_column in out.columns else ""
        out = self._dedupe_keywords(out)
        out = self._add_tags(out)
        return out.reset_index(drop=True)

    def _safe_numeric(self, df: pd.DataFrame, col: Optional[str]) -> pd.Series:
        if not col or col not in df.columns:
            return pd.Series(np.nan, index=df.index, dtype=float)
        return pd.to_numeric(df[col], errors="coerce")

    def _dedupe_keywords(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped_rows = []
        for _, sub in df.groupby("normalized_keyword", sort=False):
            row = sub.iloc[0].copy()
            row["volume"] = pd.to_numeric(sub["volume"], errors="coerce").fillna(0).max()
            row["difficulty"] = pd.to_numeric(sub["difficulty"], errors="coerce").mean()
            row["position"] = pd.to_numeric(sub["position"], errors="coerce").mean()
            urls = [u for u in sub["url"].astype(str).tolist() if u and u != "nan"]
            row["url"] = " | ".join(unique_preserve_order(urls))
            grouped_rows.append(row)
        return pd.DataFrame(grouped_rows)

    def _pattern_flag(self, text: str, patterns: List[str]) -> bool:
        return any(re.search(p, text) for p in patterns)

    def _add_tags(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config
        df = df.copy()
        branded = [normalize_text(term) for term in cfg.branded_terms if term.strip()]

        intents = []
        modifier_groups = []
        page_types = []
        is_brand = []
        topic_roots = []
        for kw in df["normalized_keyword"].tolist():
            if self._pattern_flag(kw, cfg.intent_patterns.transactional):
                intent = "transactional"
            elif self._pattern_flag(kw, cfg.intent_patterns.commercial):
                intent = "commercial"
            elif self._pattern_flag(kw, cfg.intent_patterns.navigational):
                intent = "navigational"
            else:
                intent = "informational"
            intents.append(intent)

            mods = []
            for token, group in [
                (r"\bbest\b|\btop\b", "best"),
                (r"\bvs\b|\bversus\b|\bcompare\b|\bcomparison\b", "compare"),
                (r"\breview\b|\breviews\b", "review"),
                (r"\balternatives?\b", "alternatives"),
                (r"\bpricing\b|\bprice\b|\bcost\b", "pricing"),
                (r"\bhow\b|\bguide\b|\btutorial\b", "learn"),
            ]:
                if re.search(token, kw):
                    mods.append(group)
            modifier_groups.append("+".join(mods) if mods else "core")

            if intent == "transactional":
                page_type = "money-page"
            elif intent == "commercial":
                page_type = "comparison"
            elif "learn" in modifier_groups[-1]:
                page_type = "guide"
            else:
                page_type = "article"
            page_types.append(page_type)
            is_brand.append(any(term and term in kw for term in branded))
            root = self._topic_root(kw)
            topic_roots.append(root)

        df["intent"] = intents
        df["modifier_group"] = modifier_groups
        df["page_type"] = page_types
        df["is_brand"] = is_brand
        df["topic_root"] = topic_roots
        return df


    def _topic_root(self, kw: str) -> str:
        stop = {
            "best","top","free","review","reviews","vs","versus","compare","comparison",
            "pricing","price","cost","how","guide","tutorial","what","is","to","for",
            "the","a","an","and","of","in","on","use","choose"
        }
        tokens = [t for t in kw.split() if t not in stop]
        return " ".join(tokens[:4]) if tokens else kw

    def _lexical_vectors(self, texts: List[str]) -> csr_matrix:
        cfg = self.config
        word = TfidfVectorizer(
            analyzer="word",
            ngram_range=cfg.lexical_ngram_range,
            max_features=cfg.max_features_word,
            min_df=1,
        ).fit_transform(texts)
        char = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=cfg.char_ngram_range,
            max_features=cfg.max_features_char,
            min_df=1,
        ).fit_transform(texts)
        return hstack([word, char]).tocsr()

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
        n_components = int(min(cfg.semantic_components, max(2, lexical_vectors.shape[0] - 1), lexical_vectors.shape[1] - 1))
        if n_components < 2:
            return lexical_vectors.toarray().astype(float)
        svd = TruncatedSVD(n_components=n_components, random_state=cfg.random_state)
        emb = svd.fit_transform(lexical_vectors)
        norms = np.linalg.norm(emb, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return emb / norms

    def _hybrid_similarity_graph(self, lexical_vectors: csr_matrix, semantic_vectors: np.ndarray) -> np.ndarray:
        cfg = self.config
        n = semantic_vectors.shape[0]
        if n == 0:
            return np.zeros((0, 0), dtype=float)
        if n == 1:
            return np.ones((1, 1), dtype=float)

        neighbor_k = int(min(cfg.neighbor_k, max(1, n - 1)))
        nn = NearestNeighbors(metric="cosine", n_neighbors=neighbor_k + 1)
        nn.fit(semantic_vectors)
        distances, indices = nn.kneighbors(semantic_vectors)
        graph = np.zeros((n, n), dtype=float)

        for row in range(n):
            for dist, col in zip(distances[row][1:], indices[row][1:]):
                sem_sim = max(0.0, 1.0 - float(dist))
                lex_sim = float(cosine_similarity(lexical_vectors[row], lexical_vectors[col])[0, 0])
                sim = cfg.hybrid_semantic_weight * sem_sim + cfg.hybrid_lexical_weight * lex_sim
                if sim >= cfg.similarity_threshold:
                    graph[row, col] = sim
                    graph[col, row] = sim
        np.fill_diagonal(graph, 1.0)
        return graph

    def _cluster_from_graph(self, graph: np.ndarray) -> np.ndarray:
        sparse = csr_matrix(graph > 0)
        _, labels = connected_components(sparse, directed=False, return_labels=True)
        return labels

    def _enforce_min_cluster_size(self, labels: np.ndarray) -> np.ndarray:
        cfg = self.config
        labels = labels.copy()
        counts = pd.Series(labels).value_counts().to_dict()
        small = {label for label, count in counts.items() if count < cfg.min_cluster_size}
        next_noise = labels.max() + 1
        for i, label in enumerate(labels):
            if label in small:
                labels[i] = next_noise
                next_noise += 1
        return labels

    def _post_split(self, df: pd.DataFrame, labels: np.ndarray) -> np.ndarray:
        cfg = self.config
        current_labels = labels.copy()
        next_label = int(current_labels.max()) + 1
        for cluster_label in sorted(pd.unique(current_labels)):
            idx = np.where(current_labels == cluster_label)[0]
            if len(idx) <= 1:
                continue
            sub = df.iloc[idx]
            group_cols = []
            if cfg.post_split_on_intent:
                group_cols.append("intent")
            group_cols.append("topic_root")
            if cfg.post_split_on_modifiers:
                group_cols.append("modifier_group")
            if not group_cols:
                continue
            grouped = sub.groupby(group_cols, dropna=False)
            if len(grouped) <= 1:
                continue
            first = True
            for _, subset in grouped:
                if first:
                    first = False
                    continue
                for original_idx in subset.index:
                    arr_idx = idx[list(sub.index).index(original_idx)]
                    current_labels[arr_idx] = next_label
                next_label += 1
        return current_labels

    def _canonical_keyword(self, sub: pd.DataFrame) -> str:
        ranked = sub.copy()
        volume = ranked["volume"].fillna(0)
        position = ranked["position"].fillna(100)
        difficulty = ranked["difficulty"].fillna(50)
        score = (np.log1p(volume) + (21 - position.clip(lower=1, upper=100))) / (1 + difficulty)
        ranked["__score"] = score
        ranked = ranked.sort_values(["__score", "volume"], ascending=[False, False])
        return str(ranked.iloc[0]["keyword"])

    def _topic_label(self, sub: pd.DataFrame) -> str:
        vec = TfidfVectorizer(ngram_range=(1, 2), stop_words="english")
        tfidf = vec.fit_transform(sub["normalized_keyword"].tolist())
        weights = np.asarray(tfidf.sum(axis=0)).ravel()
        terms = np.asarray(vec.get_feature_names_out())
        if len(terms) == 0:
            return self._canonical_keyword(sub)
        top = terms[weights.argsort()[::-1][:3]]
        label = " / ".join([t.title() for t in top])
        return label or self._canonical_keyword(sub)

    def _quality_score(self, cluster_graph: np.ndarray, idx: np.ndarray, sub: pd.DataFrame) -> float:
        if len(idx) == 1:
            return 0.55
        sims = cluster_graph[np.ix_(idx, idx)]
        upper = sims[np.triu_indices_from(sims, k=1)]
        cohesion = float(np.nanmean(upper)) if upper.size else 0.0
        intent_consistency = float(sub["intent"].value_counts(normalize=True).iloc[0])
        mod_consistency = float(sub["modifier_group"].value_counts(normalize=True).iloc[0])
        score = 0.5 * cohesion + 0.3 * intent_consistency + 0.2 * mod_consistency
        return round(float(min(max(score, 0.0), 1.0)), 4)

    def _opportunity_score(self, sub: pd.DataFrame) -> float:
        volume = float(sub["volume"].fillna(0).sum())
        difficulty = float(sub["difficulty"].fillna(sub["difficulty"].median()).mean()) if len(sub) else 0.0
        rank = float(sub["position"].fillna(50).mean()) if len(sub) else 50.0
        gap = max(0.0, self.config.opportunity_rank_ceiling - rank)
        raw = (math.log1p(volume) * (1 + gap)) / (1 + max(difficulty, 0.0))
        return round(raw, 4)

    def run(self, df: pd.DataFrame) -> Dict[str, Any]:
        prepared = self.prepare_dataframe(df)
        texts = prepared["normalized_keyword"].tolist()
        lexical = self._lexical_vectors(texts)
        semantic = self._semantic_vectors(texts, lexical)
        graph = self._hybrid_similarity_graph(lexical, semantic)
        labels = self._cluster_from_graph(graph)
        labels = self._enforce_min_cluster_size(labels)
        labels = self._post_split(prepared, labels)

        prepared = prepared.copy()
        prepared["cluster_id"] = labels

        clusters: List[Dict[str, Any]] = []
        for cluster_id in sorted(prepared["cluster_id"].unique()):
            sub = prepared[prepared["cluster_id"] == cluster_id].copy()
            idx = sub.index.to_numpy()
            canonical_keyword = self._canonical_keyword(sub)
            topic_label = self._topic_label(sub)
            quality = self._quality_score(graph, idx, sub)
            opp = self._opportunity_score(sub)
            intent = str(sub["intent"].mode().iloc[0])
            page_type = str(sub["page_type"].mode().iloc[0])
            cluster_slug = stable_slug(canonical_keyword)
            clusters.append({
                "cluster_id": int(cluster_id),
                "cluster_slug": cluster_slug,
                "canonical_keyword": canonical_keyword,
                "topic_label": topic_label,
                "intent": intent,
                "page_type": page_type,
                "keyword_count": int(len(sub)),
                "total_volume": float(sub["volume"].fillna(0).sum()),
                "avg_difficulty": float(sub["difficulty"].mean()) if sub["difficulty"].notna().any() else None,
                "avg_rank": float(sub["position"].mean()) if sub["position"].notna().any() else None,
                "cluster_quality": quality,
                "opportunity_score": opp,
                "keywords": sub["keyword"].tolist(),
                "urls": [u for u in unique_preserve_order(sub["url"].astype(str).tolist()) if u],
            })
            prepared.loc[sub.index, "canonical_keyword"] = canonical_keyword
            prepared.loc[sub.index, "topic_label"] = topic_label
            prepared.loc[sub.index, "intent"] = intent
            prepared.loc[sub.index, "page_type"] = page_type
            prepared.loc[sub.index, "cluster_quality"] = quality
            prepared.loc[sub.index, "opportunity_score"] = opp

        prepared = prepared.sort_values(["cluster_id", "volume"], ascending=[True, False]).reset_index(drop=True)
        cluster_summary = pd.DataFrame(clusters).sort_values(
            ["opportunity_score", "total_volume", "cluster_quality"], ascending=[False, False, False]
        ).reset_index(drop=True)

        return {
            "config": asdict(self.config),
            "rows": prepared,
            "clusters": cluster_summary,
            "summary": {
                "keywords": int(len(prepared)),
                "clusters": int(cluster_summary.shape[0]),
                "avg_cluster_quality": float(cluster_summary["cluster_quality"].mean()) if not cluster_summary.empty else 0.0,
                "top_cluster": cluster_summary.iloc[0].to_dict() if not cluster_summary.empty else None,
            },
        }

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
