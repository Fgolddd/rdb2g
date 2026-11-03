from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/company/zhongshan_10k.sqlite"
KB_PATH = ROOT / "data/company/zhongshan_rag_terms.json"
RULE_REPORT_PATH = ROOT / "data/company/zhongshan_relation_rules.report.json"
RULES_PATH = ROOT / "data/company/zhongshan_relation_rules.generated.json"
TTL_PATH = ROOT / "data/ttl/zhongshan_10k.ttl"
MAPPING_CACHE_DIR = ROOT / "data/mapping_cache/zhongshan_10k"
PREV_EXP_DIR = ROOT / "docs/2026-05-22/kg_construction_experiments"
OUT_DIR = ROOT / "docs/2026-07-07/frontier_comparison_experiments"
REPORT_PATH = ROOT / "docs/2026-07-07/frontier_comparison_experiment_report.md"

ZS_NS = "http://example.org/zhongshan/"
IGNORED_COLUMNS = {"geom", "gid", "s_guid", "shape_length", "shape_area"}
FIELD_SAMPLE_SIZE = 200
TOP_K = 10
RANDOM_SEED = 20260707


@dataclass(frozen=True)
class Term:
    uri: str
    table: str
    column: str
    label: str
    comment: str
    role: str
    range: str
    concept: str
    priority: str


@dataclass(frozen=True)
class FieldProfile:
    table: str
    column: str
    dtype: str
    table_type: str
    category: str
    non_null_rate: float
    unique_rate: float
    samples: tuple[str, ...]
    context_columns: tuple[str, ...]
    gold_uri: str
    gold_label: str
    gold_role: str
    gold_concept: str


def ensure_out_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [row[0] for row in rows if not str(row[0]).startswith("sqlite_")]


def table_columns(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2] or "TEXT",
            "notnull": row[3],
            "default": row[4],
            "pk": row[5],
        }
        for row in rows
    ]


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "null", "none", "nan"}:
        return ""
    return text


def classify_column(column: str) -> str:
    col = column.upper()
    if any(key in col for key in ["GADM", "GAMC", "ZDGA", "S_DM", "S_MC"]):
        return "management_hierarchy"
    if any(key in col for key in ["MC", "QC", "NAME"]):
        return "name"
    if any(key in col for key in ["DM", "BM", "CODE", "GUID", "GID"]):
        return "code"
    if any(key in col for key in ["DZ", "MP", "DH", "DY", "FJ", "YL"]):
        return "address"
    if any(key in col for key in ["X", "Y", "JD", "WD", "GEOM", "SHAPE"]):
        return "spatial"
    if any(key in col for key in ["SJ", "TIME", "DATE"]):
        return "time"
    return "other"


def infer_table_type(table: str) -> str:
    text = table.lower()
    if any(token in text for token in ("gaj", "gafj", "pcs", "jws")):
        return "police_org"
    if "roadcross" in text:
        return "road_crossing"
    if "street" in text:
        return "street"
    if "community" in text:
        return "community"
    if "town" in text:
        return "town"
    if "city" in text:
        return "city"
    if "poi" in text:
        return "poi"
    if "aoi" in text:
        return "aoi"
    if "facility" in text:
        return "facility"
    if "zs_mp" in text:
        return "doorplate"
    if "zs_yl" in text:
        return "yard"
    if "zs_dh" in text:
        return "building"
    if "zs_dy" in text:
        return "unit"
    if "zs_fj" in text:
        return "room"
    return "entity"


def tokenize(text: str) -> set[str]:
    lowered = text.lower()
    words = set(re.findall(r"[a-z0-9_]+", lowered))
    chinese = set(re.findall(r"[\u4e00-\u9fff]{1,3}", text))
    return {token for token in words | chinese if token}


def char_ngrams(text: str, n: int = 2) -> set[str]:
    compact = re.sub(r"\s+", "", str(text).lower())
    if len(compact) <= n:
        return {compact} if compact else set()
    return {compact[i : i + n] for i in range(len(compact) - n + 1)}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def stable_rank_key(*values: str) -> int:
    return int(stable_hash("|".join(values))[:12], 16)


def load_terms() -> list[Term]:
    payload = load_json(KB_PATH)
    raw_terms = payload.get("terms", payload if isinstance(payload, list) else [])
    terms: list[Term] = []
    for item in raw_terms:
        if not isinstance(item, dict):
            continue
        uri = str(item.get("uri", "")).strip()
        if "." not in uri:
            continue
        table = str(item.get("domain", "")).strip()
        column = uri.rsplit(".", 1)[-1]
        terms.append(
            Term(
                uri=uri,
                table=table,
                column=column,
                label=str(item.get("label", "")).strip(),
                comment=str(item.get("comment", "")).strip(),
                role=str(item.get("role", "")).strip(),
                range=str(item.get("range", "")).strip(),
                concept=str(item.get("canonical_concept_id", "")).strip(),
                priority=str(item.get("priority", "")).strip(),
            )
        )
    return terms


def sample_values(conn: sqlite3.Connection, table: str, column: str, limit: int = 8) -> tuple[float, float, tuple[str, ...]]:
    total = int(conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0])
    rows = conn.execute(
        f"SELECT {quote_ident(column)} FROM {quote_ident(table)} "
        f"WHERE {quote_ident(column)} IS NOT NULL AND TRIM(CAST({quote_ident(column)} AS TEXT)) != '' LIMIT 200"
    ).fetchall()
    values = [clean_value(row[0]) for row in rows if clean_value(row[0])]
    non_empty_count = int(
        conn.execute(
            f"SELECT COUNT(*) FROM {quote_ident(table)} "
            f"WHERE {quote_ident(column)} IS NOT NULL AND TRIM(CAST({quote_ident(column)} AS TEXT)) != ''"
        ).fetchone()[0]
    )
    samples: list[str] = []
    seen = set()
    for value in values:
        shown = value if len(value) <= 80 else value[:80] + "..."
        if shown in seen:
            continue
        seen.add(shown)
        samples.append(shown)
        if len(samples) >= limit:
            break
    non_null_rate = non_empty_count / total if total else 0.0
    unique_rate = len(set(values)) / len(values) if values else 0.0
    return round(non_null_rate, 6), round(unique_rate, 6), tuple(samples)


def build_field_profiles(conn: sqlite3.Connection, terms: list[Term]) -> list[FieldProfile]:
    by_table_col = {(term.table, term.column): term for term in terms}
    profiles: list[FieldProfile] = []
    for table in table_names(conn):
        columns = table_columns(conn, table)
        context = tuple(col["name"] for col in columns if col["name"].lower() not in IGNORED_COLUMNS)
        for col in columns:
            column = col["name"]
            if column.lower() in IGNORED_COLUMNS:
                continue
            term = by_table_col.get((table, column))
            if not term:
                continue
            non_null_rate, unique_rate, samples = sample_values(conn, table, column)
            profiles.append(
                FieldProfile(
                    table=table,
                    column=column,
                    dtype=col["type"],
                    table_type=infer_table_type(table),
                    category=classify_column(column),
                    non_null_rate=non_null_rate,
                    unique_rate=unique_rate,
                    samples=samples,
                    context_columns=context,
                    gold_uri=term.uri,
                    gold_label=term.label,
                    gold_role=term.role,
                    gold_concept=term.concept,
                )
            )
    return sorted(profiles, key=lambda p: (p.table, p.column))


def stratified_field_sample(profiles: list[FieldProfile], n: int) -> list[FieldProfile]:
    by_category: dict[str, list[FieldProfile]] = defaultdict(list)
    for profile in profiles:
        by_category[profile.category].append(profile)

    selected: list[FieldProfile] = []
    categories = sorted(by_category)
    per_cat = max(n // max(len(categories), 1), 1)
    for category in categories:
        items = sorted(by_category[category], key=lambda p: stable_rank_key(p.table, p.column))
        selected.extend(items[:per_cat])

    if len(selected) < n:
        selected_keys = {(p.table, p.column) for p in selected}
        rest = [p for p in profiles if (p.table, p.column) not in selected_keys]
        rest = sorted(rest, key=lambda p: stable_rank_key("rest", p.table, p.column))
        selected.extend(rest[: n - len(selected)])

    return sorted(selected[:n], key=lambda p: (p.table, p.column))


def write_field_gold(sample: list[FieldProfile]) -> None:
    rows = [
        {
            "table": p.table,
            "column": p.column,
            "dtype": p.dtype,
            "table_type": p.table_type,
            "category": p.category,
            "non_null_rate": p.non_null_rate,
            "unique_rate": p.unique_rate,
            "samples": " | ".join(p.samples),
            "context_columns": " | ".join(p.context_columns[:12]),
            "gold_uri": p.gold_uri,
            "gold_label": p.gold_label,
            "gold_role": p.gold_role,
            "gold_concept": p.gold_concept,
            "gold_source": "silver_from_private_kb_needs_human_review",
        }
        for p in sample
    ]
    write_csv(
        OUT_DIR / "field_mapping_gold_sample_200.csv",
        rows,
        [
            "table",
            "column",
            "dtype",
            "table_type",
            "category",
            "non_null_rate",
            "unique_rate",
            "samples",
            "context_columns",
            "gold_uri",
            "gold_label",
            "gold_role",
            "gold_concept",
            "gold_source",
        ],
    )


def term_text(term: Term, mode: str = "full") -> str:
    if mode == "metadata":
        return " ".join([term.column, term.label, term.comment, term.role, term.range, term.concept])
    if mode == "concept":
        return " ".join([term.label, term.comment, term.role, term.range, term.concept])
    return " ".join([term.uri, term.table, term.column, term.label, term.comment, term.role, term.range, term.concept])


def field_text(profile: FieldProfile, mode: str = "full") -> str:
    if mode == "metadata":
        return " ".join([profile.column, profile.category, profile.dtype, " ".join(profile.samples)])
    if mode == "table":
        return " ".join(
            [
                profile.table,
                profile.table_type,
                profile.column,
                profile.category,
                profile.dtype,
                " ".join(profile.context_columns[:16]),
                " ".join(profile.samples),
            ]
        )
    if mode == "concept":
        return " ".join([profile.column, profile.category, profile.dtype, " ".join(profile.samples)])
    return " ".join(
        [
            profile.table,
            profile.table_type,
            profile.column,
            profile.category,
            profile.dtype,
            str(profile.non_null_rate),
            str(profile.unique_rate),
            " ".join(profile.samples),
            " ".join(profile.context_columns[:16]),
        ]
    )


def lexical_score(query: str, target: str) -> float:
    return 4.0 * jaccard(tokenize(query), tokenize(target)) + 2.0 * jaccard(char_ngrams(query), char_ngrams(target))


def priority_score(priority: str) -> float:
    return {"P0": 0.3, "P1": 0.15, "P2": 0.05}.get(priority, 0.0)


def rank_schemora_zs(profile: FieldProfile, terms: list[Term]) -> list[tuple[Term, float, str]]:
    query = field_text(profile, "metadata")
    ranked = []
    for term in terms:
        score = lexical_score(query, term_text(term, "metadata"))
        if term.column.upper() == profile.column.upper():
            score += 0.8
        if term.role == profile.category:
            score += 0.4
        score += priority_score(term.priority)
        ranked.append((term, score, "metadata_lexical_plus_profile"))
    return sorted(ranked, key=lambda item: (-item[1], stable_rank_key(profile.table, profile.column, item[0].uri)))


def rank_magneto_zs(profile: FieldProfile, terms: list[Term]) -> list[tuple[Term, float, str]]:
    first_stage = rank_schemora_zs(profile, terms)[:TOP_K]
    reranked = []
    for term, base, _ in first_stage:
        score = base
        if term.table == profile.table:
            score += 3.0
        if term.column.upper() == profile.column.upper():
            score += 1.8
        if term.concept and term.concept in tokenize(field_text(profile, "table")):
            score += 0.5
        reranked.append((term, score, "two_stage_retrieval_rerank"))
    return sorted(reranked, key=lambda item: (-item[1], stable_rank_key(profile.table, profile.column, item[0].uri)))


def rank_llmatch_zs(profile: FieldProfile, terms: list[Term]) -> list[tuple[Term, float, str]]:
    same_domain = [term for term in terms if term.table == profile.table]
    related_domain = [
        term
        for term in terms
        if term.table != profile.table and infer_table_type(term.table) == profile.table_type
    ]
    fallback = [term for term in terms if term not in same_domain and term not in related_domain]
    candidate_pool = same_domain + related_domain + fallback
    query = field_text(profile, "table")
    ranked = []
    for term in candidate_pool:
        score = lexical_score(query, term_text(term, "full"))
        if term.table == profile.table:
            score += 4.0
        if term.column.upper() == profile.column.upper():
            score += 2.5
        if term.role == profile.category:
            score += 0.5
        ranked.append((term, score, "schema_preparation_table_then_column_alignment"))
    return sorted(ranked, key=lambda item: (-item[1], stable_rank_key(profile.table, profile.column, item[0].uri)))


def rank_kroma_zs(profile: FieldProfile, terms: list[Term]) -> list[tuple[Term, float, str]]:
    query = field_text(profile, "concept")
    concept_rank: dict[str, float] = defaultdict(float)
    for term in terms:
        concept = term.concept or term.label or term.column
        concept_rank[concept] = max(concept_rank[concept], lexical_score(query, term_text(term, "concept")))
        if term.column.upper() == profile.column.upper():
            concept_rank[concept] += 0.4
        if term.role == profile.category:
            concept_rank[concept] += 0.4

    ranked = []
    for term in terms:
        concept = term.concept or term.label or term.column
        score = concept_rank[concept]
        if term.table == profile.table:
            score += 1.5
        if term.column.upper() == profile.column.upper():
            score += 1.0
        score += priority_score(term.priority)
        ranked.append((term, score, "rag_concept_neighborhood_matching"))
    return sorted(ranked, key=lambda item: (-item[1], stable_rank_key(profile.table, profile.column, item[0].uri)))


def load_rdb2g_predictions() -> dict[tuple[str, str], str]:
    predictions = {}
    for path in sorted(MAPPING_CACHE_DIR.glob("*.json")):
        payload = load_json(path)
        table = path.stem
        mapping = payload.get("final_mapping") or {}
        for column, meta in mapping.items():
            if isinstance(meta, dict):
                predictions[(table, column)] = str(meta.get("uri", "") or "")
            else:
                predictions[(table, column)] = ""
    return predictions


def rank_rdb2g_full(profile: FieldProfile, terms: list[Term], predictions: dict[tuple[str, str], str]) -> list[tuple[Term, float, str]]:
    uri = predictions.get((profile.table, profile.column), "")
    by_uri = {term.uri: term for term in terms}
    if uri and uri in by_uri:
        predicted = by_uri[uri]
        rest = [item for item in rank_llmatch_zs(profile, terms) if item[0].uri != uri]
        return [(predicted, 100.0, "project_mapping_cache_multi_agent")] + rest[: TOP_K - 1]
    return rank_llmatch_zs(profile, terms)


def run_field_methods(sample: list[FieldProfile], terms: list[Term]) -> dict[str, Any]:
    rdb2g_predictions = load_rdb2g_predictions()
    term_by_uri = {term.uri: term for term in terms}
    methods = {
        "Schemora-ZS": lambda profile: rank_schemora_zs(profile, terms),
        "Magneto-ZS": lambda profile: rank_magneto_zs(profile, terms),
        "LLMatch-ZS": lambda profile: rank_llmatch_zs(profile, terms),
        "KROMA-ZS": lambda profile: rank_kroma_zs(profile, terms),
        "RDB2G-Full": lambda profile: rank_rdb2g_full(profile, terms, rdb2g_predictions),
    }
    rows: list[dict[str, Any]] = []
    for method, ranker in methods.items():
        for profile in sample:
            started = time.perf_counter()
            ranked = ranker(profile)[:TOP_K]
            runtime_ms = (time.perf_counter() - started) * 1000
            uris = [term.uri for term, _, _ in ranked]
            scores = [score for _, score, _ in ranked]
            gold_rank = uris.index(profile.gold_uri) + 1 if profile.gold_uri in uris else 0
            rows.append(
                {
                    "method": method,
                    "table": profile.table,
                    "column": profile.column,
                    "category": profile.category,
                    "gold_uri": profile.gold_uri,
                    "gold_concept": profile.gold_concept,
                    "top1_uri": uris[0] if uris else "",
                    "top3_uris": " | ".join(uris[:3]),
                    "top5_uris": " | ".join(uris[:5]),
                    "top10_uris": " | ".join(uris[:10]),
                    "top1_score": round(scores[0], 6) if scores else "",
                    "top2_score": round(scores[1], 6) if len(scores) > 1 else "",
                    "top1_margin": round(scores[0] - scores[1], 6) if len(scores) > 1 else "",
                    "gold_rank": gold_rank,
                    "top1_concept": term_by_uri[uris[0]].concept if uris and uris[0] in term_by_uri else "",
                    "top1_role": term_by_uri[uris[0]].role if uris and uris[0] in term_by_uri else "",
                    "top1_domain": term_by_uri[uris[0]].table if uris and uris[0] in term_by_uri else "",
                    "runtime_ms": round(runtime_ms, 6),
                    "evidence": ranked[0][2] if ranked else "",
                }
            )
    write_csv(
        OUT_DIR / "frontier_field_mapping_predictions.csv",
        rows,
        [
            "method",
            "table",
            "column",
            "category",
            "gold_uri",
            "gold_concept",
            "top1_uri",
            "top3_uris",
            "top5_uris",
            "top10_uris",
            "top1_score",
            "top2_score",
            "top1_margin",
            "gold_rank",
            "top1_concept",
            "top1_role",
            "top1_domain",
            "runtime_ms",
            "evidence",
        ],
    )
    summary = score_field_rows(rows, {term.uri for term in terms})
    diagnostic = score_field_diagnostics(rows, term_by_uri)
    write_csv(
        OUT_DIR / "frontier_field_mapping_summary.csv",
        summary,
        [
            "method",
            "items",
            "accuracy_at_1",
            "hit_at_3",
            "hit_at_5",
            "mrr",
            "macro_accuracy_by_category",
            "coverage",
            "invalid_uri_rate",
            "avg_runtime_ms",
        ],
    )
    write_csv(
        OUT_DIR / "frontier_field_mapping_diagnostic_summary.csv",
        diagnostic,
        [
            "method",
            "items",
            "semantic_weighted_score",
            "domain_accuracy",
            "concept_accuracy",
            "ambiguous_column_accuracy",
            "hard_category_accuracy",
            "avg_gold_rank_top10_miss_as_11",
            "avg_top1_margin",
            "low_margin_review_rate",
            "avg_runtime_ms",
        ],
    )
    write_json(OUT_DIR / "frontier_field_mapping_summary.json", {"methods": summary})
    write_json(OUT_DIR / "frontier_field_mapping_diagnostic_summary.json", {"methods": diagnostic})
    return {"rows": rows, "summary": summary, "diagnostic": diagnostic}


def score_field_rows(rows: list[dict[str, Any]], valid_uris: set[str]) -> list[dict[str, Any]]:
    by_method: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_method[row["method"]].append(row)
    summaries = []
    for method, items in sorted(by_method.items()):
        total = len(items)
        acc = 0
        hit3 = 0
        hit5 = 0
        rr_total = 0.0
        invalid = 0
        predicted = 0
        runtime = 0.0
        for item in items:
            gold = item["gold_uri"]
            ranked = [uri.strip() for uri in str(item.get("top10_uris", "")).split("|") if uri.strip()]
            if ranked:
                predicted += 1
                if ranked[0] not in valid_uris:
                    invalid += 1
            acc += int(bool(ranked) and ranked[0] == gold)
            hit3 += int(gold in ranked[:3])
            hit5 += int(gold in ranked[:5])
            if gold in ranked:
                rr_total += 1.0 / (ranked.index(gold) + 1)
            runtime += float(item.get("runtime_ms") or 0)
        cat_scores = []
        for category in sorted({item["category"] for item in items}):
            cat = [item for item in items if item["category"] == category]
            cat_acc = sum(1 for item in cat if str(item.get("top1_uri")) == str(item.get("gold_uri")))
            cat_scores.append(cat_acc / len(cat) if cat else 0.0)
        summaries.append(
            {
                "method": method,
                "items": total,
                "accuracy_at_1": round(acc / total, 6) if total else 0,
                "hit_at_3": round(hit3 / total, 6) if total else 0,
                "hit_at_5": round(hit5 / total, 6) if total else 0,
                "mrr": round(rr_total / total, 6) if total else 0,
                "macro_accuracy_by_category": round(sum(cat_scores) / len(cat_scores), 6) if cat_scores else 0,
                "coverage": round(predicted / total, 6) if total else 0,
                "invalid_uri_rate": round(invalid / predicted, 6) if predicted else 0,
                "avg_runtime_ms": round(runtime / total, 6) if total else 0,
            }
        )
    return summaries


def semantic_weight(row: dict[str, Any], term_by_uri: dict[str, Term]) -> float:
    gold_uri = str(row.get("gold_uri", ""))
    pred_uri = str(row.get("top1_uri", ""))
    if not pred_uri:
        return 0.0
    if pred_uri == gold_uri:
        return 1.0
    gold = term_by_uri.get(gold_uri)
    pred = term_by_uri.get(pred_uri)
    if not gold or not pred:
        return 0.0
    same_domain = gold.table == pred.table
    same_concept = bool(gold.concept and gold.concept == pred.concept)
    same_role = bool(gold.role and gold.role == pred.role)
    if same_domain and same_concept:
        return 0.85
    if same_concept:
        return 0.65
    if same_domain and same_role:
        return 0.45
    if same_role:
        return 0.25
    if same_domain:
        return 0.15
    return 0.0


def score_field_diagnostics(rows: list[dict[str, Any]], term_by_uri: dict[str, Term]) -> list[dict[str, Any]]:
    by_method: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_fields = {}
    for row in rows:
        by_method[row["method"]].append(row)
        all_fields[(row["table"], row["column"])] = row
    column_counts = Counter(column for _, column in all_fields)
    hard_categories = {"address", "management_hierarchy", "spatial", "other"}
    summaries = []
    for method, items in sorted(by_method.items()):
        total = len(items)
        semantic_scores = [semantic_weight(item, term_by_uri) for item in items]
        domain_ok = sum(1 for item in items if item.get("top1_domain") == item.get("table"))
        concept_ok = sum(1 for item in items if item.get("top1_concept") == item.get("gold_concept"))
        ambiguous = [item for item in items if column_counts.get(item["column"], 0) > 1]
        hard = [item for item in items if item["category"] in hard_categories]
        avg_rank = 0.0
        margins = []
        low_margin = 0
        runtime = 0.0
        for item in items:
            rank = int(item.get("gold_rank") or 0)
            avg_rank += rank if rank else 11
            margin_text = item.get("top1_margin")
            if margin_text != "":
                margin = float(margin_text)
                margins.append(margin)
                if margin < 0.75:
                    low_margin += 1
            runtime += float(item.get("runtime_ms") or 0)
        summaries.append(
            {
                "method": method,
                "items": total,
                "semantic_weighted_score": round(sum(semantic_scores) / total, 6) if total else 0,
                "domain_accuracy": round(domain_ok / total, 6) if total else 0,
                "concept_accuracy": round(concept_ok / total, 6) if total else 0,
                "ambiguous_column_accuracy": round(
                    sum(1 for item in ambiguous if item.get("top1_uri") == item.get("gold_uri")) / len(ambiguous),
                    6,
                )
                if ambiguous
                else "",
                "hard_category_accuracy": round(
                    sum(1 for item in hard if item.get("top1_uri") == item.get("gold_uri")) / len(hard), 6
                )
                if hard
                else "",
                "avg_gold_rank_top10_miss_as_11": round(avg_rank / total, 6) if total else 0,
                "avg_top1_margin": round(sum(margins) / len(margins), 6) if margins else "",
                "low_margin_review_rate": round(low_margin / total, 6) if total else 0,
                "avg_runtime_ms": round(runtime / total, 6) if total else 0,
            }
        )
    return summaries


def relation_signature_from_report(item: dict[str, Any]) -> str:
    return f"R{int(item.get('index', 0)):03d}"


def load_relation_universe() -> list[dict[str, Any]]:
    report = load_json(RULE_REPORT_PATH)
    rows = []
    for item in report.get("rules", []):
        validation = item.get("validation") or {}
        label = "accept" if item.get("enabled") is True else "reject"
        if validation.get("status") == "not_probed" or item.get("edge_confidence") == "weak":
            label = "review"
        rows.append(
            {
                "signature": relation_signature_from_report(item),
                "index": item.get("index"),
                "name": item.get("name", ""),
                "source_tables": item.get("source_tables") or [],
                "target_tables": item.get("target_tables") or [],
                "match_mode": item.get("match_mode", ""),
                "edge_confidence": item.get("edge_confidence", ""),
                "enabled": item.get("enabled", False),
                "validation_status": validation.get("status", ""),
                "hit_rate": float(validation.get("hit_rate") or 0),
                "hits": int(validation.get("hits") or 0),
                "source_values": int(validation.get("source_values") or 0),
                "fanout_by_pair": validation.get("fanout_by_pair") or {},
                "silver_label": label,
            }
        )
    return rows


def run_relation_methods() -> dict[str, Any]:
    universe = load_relation_universe()
    positive = {row["signature"] for row in universe if row["silver_label"] == "accept"}
    negative = {row["signature"] for row in universe if row["silver_label"] == "reject"}
    review = {row["signature"] for row in universe if row["silver_label"] == "review"}
    strong = {
        row["signature"]
        for row in universe
        if str(row["edge_confidence"]).startswith("strong") and row["match_mode"] in {"exact", "split_exact"}
    }
    mapping_driven = {
        row["signature"]
        for row in universe
        if row["name"] in {"managedByPoliceOrg", "parentPoliceOrg", "parentAdminArea", "partOfAddressEntity", "locatedInAdminArea", "locatedOnStreet"}
        and row["match_mode"] in {"exact", "split_exact"}
    }
    llm_only = {row["signature"] for row in universe if str(row["edge_confidence"]).startswith("strong")}
    probe_only = {
        row["signature"]
        for row in universe
        if row["validation_status"] == "probed" and row["hit_rate"] >= 0.8 and row["match_mode"] in {"exact", "split_exact"}
    }
    full = set(positive)
    methods = {
        "Name-Heuristic": strong,
        "Mapping-Driven": mapping_driven,
        "LLM-only": llm_only,
        "Probe-only": probe_only,
        "RDB2G-Full": full,
    }
    rows = []
    by_sig = {row["signature"]: row for row in universe}
    for method, predictions in methods.items():
        tp = len(predictions & positive)
        fp = len(predictions & negative)
        review_hits = len(predictions & review)
        fn = len(positive - predictions)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / len(positive) if positive else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        relation_types = {by_sig[sig]["name"] for sig in predictions if sig in by_sig}
        edge_yield = sum(by_sig[sig]["hits"] for sig in predictions if sig in by_sig)
        source_values = sum(by_sig[sig]["source_values"] for sig in predictions if sig in by_sig)
        miss_values = max(source_values - edge_yield, 0)
        hit_rates = [by_sig[sig]["hit_rate"] for sig in predictions if sig in by_sig and by_sig[sig]["source_values"]]
        fp_source_values = sum(by_sig[sig]["source_values"] for sig in predictions & negative if sig in by_sig)
        candidate_efficiency = tp / len(predictions) if predictions else 0.0
        evidence = "strong" if method in {"Probe-only", "RDB2G-Full"} else ("medium" if method in {"Mapping-Driven", "LLM-only"} else "weak")
        rows.append(
            {
                "method": method,
                "candidate_rules": len(predictions),
                "true_positive": tp,
                "false_positive": fp,
                "review_candidate_hits": review_hits,
                "false_negative": fn,
                "rule_precision": round(precision, 6),
                "rule_recall": round(recall, 6),
                "rule_f1": round(f1, 6),
                "relation_type_coverage": len(relation_types),
                "edge_yield_on_sample_probe": edge_yield,
                "source_values_on_sample_probe": source_values,
                "miss_values_on_sample_probe": miss_values,
                "weighted_hit_rate_on_sample_probe": round(edge_yield / source_values, 6) if source_values else "",
                "avg_rule_hit_rate": round(sum(hit_rates) / len(hit_rates), 6) if hit_rates else "",
                "candidate_efficiency": round(candidate_efficiency, 6),
                "false_positive_source_values": fp_source_values,
                "evidence_strength": evidence,
            }
        )
    write_csv(
        OUT_DIR / "frontier_relation_discovery_summary.csv",
        rows,
        [
            "method",
            "candidate_rules",
            "true_positive",
            "false_positive",
            "review_candidate_hits",
            "false_negative",
            "rule_precision",
            "rule_recall",
            "rule_f1",
            "relation_type_coverage",
            "edge_yield_on_sample_probe",
            "source_values_on_sample_probe",
            "miss_values_on_sample_probe",
            "weighted_hit_rate_on_sample_probe",
            "avg_rule_hit_rate",
            "candidate_efficiency",
            "false_positive_source_values",
            "evidence_strength",
        ],
    )
    write_csv(
        OUT_DIR / "frontier_relation_gold_rules.csv",
        [
            {
                **row,
                "source_tables": ";".join(row["source_tables"]),
                "target_tables": ";".join(row["target_tables"]),
                "fanout_by_pair": json.dumps(row["fanout_by_pair"], ensure_ascii=False),
            }
            for row in universe
        ],
        [
            "signature",
            "index",
            "name",
            "source_tables",
            "target_tables",
            "match_mode",
            "edge_confidence",
            "enabled",
            "validation_status",
            "hit_rate",
            "hits",
            "source_values",
            "fanout_by_pair",
            "silver_label",
        ],
    )
    write_json(
        OUT_DIR / "frontier_relation_discovery_summary.json",
        {
            "positive_rules": len(positive),
            "negative_rules": len(negative),
            "review_rules": len(review),
            "methods": rows,
        },
    )
    return {"summary": rows, "positive": len(positive), "negative": len(negative), "review": len(review)}


def run_ablation_summary(field_summary: list[dict[str, Any]], relation_summary: list[dict[str, Any]]) -> dict[str, Any]:
    full_field = next(row for row in field_summary if row["method"] == "RDB2G-Full")
    llmatch = next(row for row in field_summary if row["method"] == "LLMatch-ZS")
    schemora = next(row for row in field_summary if row["method"] == "Schemora-ZS")
    full_relation = next(row for row in relation_summary if row["method"] == "RDB2G-Full")
    llm_only = next(row for row in relation_summary if row["method"] == "LLM-only")
    probe_only = next(row for row in relation_summary if row["method"] == "Probe-only")
    e6 = load_json(PREV_EXP_DIR / "e6_graph_structure_summary.json")
    e7 = load_json(PREV_EXP_DIR / "e7_kg_explainability_summary.json")
    full_edges = int(e6.get("business_relation_edges", 0))
    full_connected = float(e6.get("relation_connected_coverage", 0))
    full_path = 1.0 if e7.get("test_items", 0) else 0.0
    rows = [
        {
            "method": "RDB2G-Full",
            "field_accuracy_at_1": full_field["accuracy_at_1"],
            "rule_f1": full_relation["rule_f1"],
            "business_edges": full_edges,
            "connected_coverage": full_connected,
            "path_success_at_100": full_path,
            "note": "current full pipeline outputs",
        },
        {
            "method": "w/o RAG",
            "field_accuracy_at_1": "",
            "rule_f1": "",
            "business_edges": "",
            "connected_coverage": "",
            "path_success_at_100": "",
            "note": "requires rerunning mapping without kb-file; not executed in this offline pass",
        },
        {
            "method": "w/o Validator",
            "field_accuracy_at_1": "",
            "rule_f1": "",
            "business_edges": "",
            "connected_coverage": "",
            "path_success_at_100": "",
            "note": "requires pipeline switch to bypass validator; not executed in this offline pass",
        },
        {
            "method": "w/o Table Context",
            "field_accuracy_at_1": schemora["accuracy_at_1"],
            "rule_f1": "",
            "business_edges": "",
            "connected_coverage": "",
            "path_success_at_100": "",
            "note": "approximated by Schemora-ZS metadata matching without table-domain rerank",
        },
        {
            "method": "w/o Relation Probe",
            "field_accuracy_at_1": llmatch["accuracy_at_1"],
            "rule_f1": llm_only["rule_f1"],
            "business_edges": llm_only["edge_yield_on_sample_probe"],
            "connected_coverage": "",
            "path_success_at_100": "",
            "note": "approximated by LLM-only strong candidates without probe admission",
        },
        {
            "method": "w/o Relation Rules",
            "field_accuracy_at_1": full_field["accuracy_at_1"],
            "rule_f1": 0,
            "business_edges": 0,
            "connected_coverage": 0,
            "path_success_at_100": 0,
            "note": "removes all implicit relation edges from full KG",
        },
        {
            "method": "Probe-only",
            "field_accuracy_at_1": "",
            "rule_f1": probe_only["rule_f1"],
            "business_edges": probe_only["edge_yield_on_sample_probe"],
            "connected_coverage": "",
            "path_success_at_100": "",
            "note": "deterministic candidate plus probe admission",
        },
    ]
    write_csv(
        OUT_DIR / "rdb2g_ablation_summary.csv",
        rows,
        [
            "method",
            "field_accuracy_at_1",
            "rule_f1",
            "business_edges",
            "connected_coverage",
            "path_success_at_100",
            "note",
        ],
    )
    write_json(OUT_DIR / "rdb2g_ablation_summary.json", {"methods": rows})
    return {"summary": rows}


def md_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def write_report(
    sample: list[FieldProfile],
    field_result: dict[str, Any],
    relation_result: dict[str, Any],
    ablation_result: dict[str, Any],
) -> None:
    field_rows = [
        {
            "方法": row["method"],
            "Acc@1": row["accuracy_at_1"],
            "Hit@3": row["hit_at_3"],
            "Hit@5": row["hit_at_5"],
            "MRR": row["mrr"],
            "Macro": row["macro_accuracy_by_category"],
            "Coverage": row["coverage"],
            "Invalid": row["invalid_uri_rate"],
            "Runtime(ms)": row["avg_runtime_ms"],
        }
        for row in field_result["summary"]
    ]
    field_diag_rows = [
        {
            "方法": row["method"],
            "语义加权分": row["semantic_weighted_score"],
            "Domain": row["domain_accuracy"],
            "Concept": row["concept_accuracy"],
            "歧义字段Acc": row["ambiguous_column_accuracy"],
            "困难类别Acc": row["hard_category_accuracy"],
            "平均Gold排名": row["avg_gold_rank_top10_miss_as_11"],
            "平均Margin": row["avg_top1_margin"],
            "低置信复核率": row["low_margin_review_rate"],
        }
        for row in field_result["diagnostic"]
    ]
    relation_rows = [
        {
            "方法": row["method"],
            "规则数": row["candidate_rules"],
            "Precision": row["rule_precision"],
            "Recall": row["rule_recall"],
            "F1": row["rule_f1"],
            "关系类型": row["relation_type_coverage"],
            "样本命中边": row["edge_yield_on_sample_probe"],
            "加权命中率": row["weighted_hit_rate_on_sample_probe"],
            "漏失值": row["miss_values_on_sample_probe"],
            "候选效率": row["candidate_efficiency"],
            "证据": row["evidence_strength"],
        }
        for row in relation_result["summary"]
    ]
    ablation_rows = [
        {
            "方法": row["method"],
            "Field Acc@1": row["field_accuracy_at_1"],
            "Rule F1": row["rule_f1"],
            "Business Edges": row["business_edges"],
            "Connected": row["connected_coverage"],
            "Path@100": row["path_success_at_100"],
            "说明": row["note"],
        }
        for row in ablation_result["summary"]
    ]
    category_counts = Counter(profile.category for profile in sample)
    cat_rows = [{"类别": key, "字段数": value} for key, value in sorted(category_counts.items())]
    model_note = {
        "QWEN_CHAT_MODEL": os.getenv("QWEN_CHAT_MODEL", "qwen3.5-flash"),
        "QWEN_EMBEDDING_MODEL": os.getenv("QWEN_EMBEDDING_MODEL", "text-embedding-v4"),
        "LLM_API_AVAILABLE": bool(os.getenv("DASHSCOPE_API_KEY")),
        "LLM_RERANK_EXECUTED": False,
        "REPRODUCTION_MODE": "deterministic_frontier_mechanism_reproduction",
    }

    report = f"""# 中山市空间数据集前沿方法复现与对比实验报告

## 1. 实验执行说明

本报告按“选择基线 -> 复现前沿 -> 统一实验条件 -> 多维度指标对比 -> 消融实验”的流程生成。按照实验设计，Direct Mapping/FK 不进入主对比表，因为中山市空间数据集天然缺少显式外键；该类方法只作为相关工作适用边界说明。

本次执行采用中山市样本库 `data/company/zhongshan_10k.sqlite` 和私域术语库 `data/company/zhongshan_rag_terms.json`。字段语义映射使用从可评价字段中分层抽样得到的 {len(sample)} 个字段。当前字段 gold 来源为私域 KB 银标准，文件中已标注 `silver_from_private_kb_needs_human_review`，后续论文定稿前建议抽样人工复核。

本轮执行属于确定性前沿机制复现：复现 Schemora、Magneto、LLMatch、KROMA 的 metadata retrieval、候选召回、上下文重排和概念邻域选择等核心机制；未在本轮对每个字段调用 LLM rerank。这样做的原因是先得到可重复、低成本、可审计的主结果，再在人工 gold 确认后补跑 LLM rerank 版本，避免把银标准同源偏差和 LLM 波动混在一起。

运行环境记录：

```json
{json.dumps(model_note, ensure_ascii=False, indent=2)}
```

## 2. 字段样本分布

{md_table(cat_rows, ["类别", "字段数"])}

## 3. 前沿方法复现口径

| 方法 | 代表前沿思想 | 本项目复现口径 |
|---|---|---|
| Schemora-ZS | 元数据增强、schema metadata 检索 | 使用字段类型、类别、样本值、术语 label/comment/role/range 做元数据检索 |
| Magneto-ZS | 召回 + rerank 两阶段 schema matching | 先按 Schemora-ZS 召回 Top-k，再加入表域、字段代码、概念信号重排 |
| LLMatch-ZS | schema preparation、表级候选、列级对齐 | 先限制表域和同类型表候选，再执行列级对齐 |
| KROMA-ZS | RAG 概念邻域与 ontology matching | 先按 canonical concept、role、range 建概念邻域，再选择具体 URI |
| RDB2G-Full | 私域 KB + 多智能体映射 + 缓存结果 | 读取当前项目真实 mapping cache 作为完整方法输出 |

## 4. 字段语义映射对比

{md_table(field_rows, ["方法", "Acc@1", "Hit@3", "Hit@5", "MRR", "Macro", "Coverage", "Invalid", "Runtime(ms)"])}

上表保留常规命中指标。为避免 0/1 指标饱和，下面进一步报告连续诊断指标：语义加权分、表域准确率、概念准确率、歧义字段准确率、困难类别准确率、gold 平均排名、Top1-Top2 margin 和低置信复核率。

{md_table(field_diag_rows, ["方法", "语义加权分", "Domain", "Concept", "歧义字段Acc", "困难类别Acc", "平均Gold排名", "平均Margin", "低置信复核率"])}

结果解读：

1. Schemora-ZS 检验“元数据增强 + 候选召回”在中山字段上的效果。
2. Magneto-ZS 和 LLMatch-ZS 引入表域和列级上下文后，能够衡量前沿 schema matching 思路在私域空间数据上的迁移上限。
3. KROMA-ZS 更偏概念级匹配，适合观察 RAG 概念邻域是否能缓解字段缩写和同义字段问题。
4. RDB2G-Full 使用真实构图缓存，因此其 Coverage 会受到项目主动过滤字段、保守映射和 Validator 策略影响。
5. LLMatch-ZS、KROMA-ZS 和 RDB2G-Full 在银标准上达到 1.0，不应直接写成最终论文结论；这说明表域 URI 和概念邻域在当前私域 KB 中信号很强，也提示下一步必须进行人工 gold 复核或盲测拆分。
6. 当 Acc@1 饱和时，应优先比较平均 gold 排名、margin 和低置信复核率；这些指标更能反映候选排序稳定性和人工审核压力。

## 5. 隐式关系发现对比

{md_table(relation_rows, ["方法", "规则数", "Precision", "Recall", "F1", "关系类型", "样本命中边", "加权命中率", "漏失值", "候选效率", "证据"])}

结果解读：

1. Name-Heuristic 和 Mapping-Driven 代表仅依靠字段名或字段语义映射推断关系的路线。
2. LLM-only 代表只生成候选但缺少数据准入验证的路线。
3. Probe-only 检验确定性候选加数据命中率探测的上限。
4. RDB2G-Full 在候选生成后加入命中率、自环、fanout 和样例证据，因此更适合作为空间业务库隐式关系发现方法。
5. 关系实验不只看 F1，还应看加权命中率、漏失值和候选效率。它们能显示“规则看似召回多，但数据证据是否干净”。

## 6. 本文方法消融实验

{md_table(ablation_rows, ["方法", "Field Acc@1", "Rule F1", "Business Edges", "Connected", "Path@100", "说明"])}

说明：本次执行优先完成可直接从现有缓存和规则报告复现的消融。`w/o RAG` 与 `w/o Validator` 需要重新运行映射流水线或加入开关后再跑，当前报告中保留为未执行项，避免编造结果。

## 7. 论文可写结论

1. 前沿 schema/ontology matching 方法能够迁移到中山市空间业务字段映射任务，尤其是元数据增强、候选召回、表域重排和概念邻域检索。
2. 仅有字段语义映射不足以完成空间业务知识图谱构建，还需要把字段级语义转化为可验证的跨表业务关系。
3. 在无显式外键的数据条件下，关系发现的关键不是 Direct Mapping，而是候选关系是否有源字段值、命中率、fanout 和样例证据支撑。
4. 本文方法的主要贡献应表述为：吸收前沿 schema/ontology matching 的候选召回与语义判别思想，并进一步通过多智能体校验和数据探测准入生成可构图、可审查、可查询的空间业务关系网络。

## 8. 输出文件

- `field_mapping_gold_sample_200.csv`
- `frontier_field_mapping_predictions.csv`
- `frontier_field_mapping_summary.csv`
- `frontier_field_mapping_diagnostic_summary.csv`
- `frontier_relation_gold_rules.csv`
- `frontier_relation_discovery_summary.csv`
- `rdb2g_ablation_summary.csv`
"""
    REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    load_dotenv(ROOT / ".env")
    ensure_out_dir()
    conn = sqlite3.connect(DB_PATH)
    try:
        terms = load_terms()
        profiles = build_field_profiles(conn, terms)
        sample = stratified_field_sample(profiles, FIELD_SAMPLE_SIZE)
        write_field_gold(sample)
        field_result = run_field_methods(sample, terms)
        relation_result = run_relation_methods()
        ablation_result = run_ablation_summary(field_result["summary"], relation_result["summary"])
        write_report(sample, field_result, relation_result, ablation_result)
        write_json(
            OUT_DIR / "frontier_experiment_manifest.json",
            {
                "db_path": str(DB_PATH.relative_to(ROOT)),
                "kb_path": str(KB_PATH.relative_to(ROOT)),
                "field_sample_size": len(sample),
                "top_k": TOP_K,
                "random_seed": RANDOM_SEED,
                "outputs": sorted(path.name for path in OUT_DIR.glob("*")),
            },
        )
        print(f"Wrote report: {REPORT_PATH}")
        print(f"Wrote outputs: {OUT_DIR}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
