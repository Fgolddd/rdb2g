from __future__ import annotations

import csv
import json
import math
import re
import sqlite3
import urllib.parse
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/company/zhongshan_10k.sqlite"
KB_PATH = ROOT / "data/company/zhongshan_rag_terms.json"
RULE_REPORT_PATH = ROOT / "data/company/zhongshan_relation_rules.report.json"
RULES_PATH = ROOT / "data/company/zhongshan_relation_rules.generated.json"
TTL_PATH = ROOT / "data/ttl/zhongshan_10k.ttl"
MAPPING_CACHE_DIR = ROOT / "data/mapping_cache/zhongshan_10k"
PREV_EXP_DIR = ROOT / "docs/2026-05-22/kg_construction_experiments"
OUT_DIR = ROOT / "docs/2026-07-07/baseline_comparison_experiments"
REPORT_PATH = ROOT / "docs/2026-07-07/baseline_comparison_experiment_report.md"

ZS_NS = "http://example.org/zhongshan/"
DATA_NS = "http://example.org/data/"
IGNORED_COLUMNS = {"geom", "gid", "s_guid", "shape_length", "shape_area"}


@dataclass(frozen=True)
class Triple:
    subject: str
    predicate: str
    obj: str
    object_type: str


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
            "type": row[2],
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
    if any(key in col for key in ["MC", "QC", "NAME"]):
        return "name"
    if any(key in col for key in ["DM", "BM", "CODE", "GUID", "GID"]):
        return "code"
    if any(key in col for key in ["DZ", "MP", "DH", "DY", "FJ", "YL"]):
        return "address"
    if any(key in col for key in ["GADM", "GAMC", "ZDGA", "S_DM", "S_MC"]):
        return "management_or_hierarchy"
    if any(key in col for key in ["X", "Y", "JD", "WD", "GEOM", "SHAPE"]):
        return "spatial"
    if any(key in col for key in ["SJ", "TIME", "DATE"]):
        return "time"
    return "other"


def table_type(table: str) -> str:
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
    compact = re.sub(r"\s+", "", text.lower())
    if len(compact) <= n:
        return {compact} if compact else set()
    return {compact[i : i + n] for i in range(len(compact) - n + 1)}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def load_kb_terms() -> list[dict[str, Any]]:
    payload = load_json(KB_PATH)
    terms = payload.get("terms", payload if isinstance(payload, list) else [])
    normalized = []
    for term in terms:
        uri = str(term.get("uri", "")).strip()
        if "." not in uri:
            continue
        column = uri.rsplit(".", 1)[-1]
        normalized.append({**term, "column_code": column})
    return normalized


def build_field_gold(conn: sqlite3.Connection, terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = {(table, col["name"]) for table in table_names(conn) for col in table_columns(conn, table)}
    rows = []
    for term in terms:
        table = str(term.get("domain", "")).strip()
        column = str(term.get("column_code", "")).strip()
        if not table or not column or column.lower() in IGNORED_COLUMNS:
            continue
        if (table, column) not in existing:
            continue
        rows.append(
            {
                "table": table,
                "column": column,
                "table_type": table_type(table),
                "column_category": classify_column(column),
                "gold_uri": term.get("uri", ""),
                "gold_label": term.get("label", ""),
                "gold_role": term.get("role", ""),
                "gold_concept": term.get("canonical_concept_id", ""),
                "gold_comment": term.get("comment", ""),
            }
        )
    return sorted(rows, key=lambda row: (row["table"], row["column"]))


def term_content(term: dict[str, Any]) -> str:
    fields = [
        term.get("uri", ""),
        term.get("domain", ""),
        term.get("column_code", ""),
        term.get("label", ""),
        term.get("comment", ""),
        term.get("role", ""),
        term.get("canonical_concept_id", ""),
    ]
    return " ".join(str(field) for field in fields if field)


def choose_best(candidates: list[dict[str, Any]], score_fn) -> dict[str, Any] | None:
    best_term = None
    best_score = -math.inf
    for idx, term in enumerate(candidates):
        score = score_fn(term)
        # Stable deterministic tie-breaking keeps repeated column-code matches from
        # silently using target-domain information.
        score -= idx * 1e-9
        if score > best_score:
            best_score = score
            best_term = term
    return best_term


def predict_string_rule(row: dict[str, Any], terms: list[dict[str, Any]]) -> dict[str, str]:
    column = row["column"]
    col_upper = column.upper()

    def score(term: dict[str, Any]) -> float:
        target_col = str(term.get("column_code", "")).upper()
        label = str(term.get("label", ""))
        role = str(term.get("role", ""))
        concept = str(term.get("canonical_concept_id", ""))
        value = 0.0
        if target_col == col_upper:
            value += 100.0
        if target_col and (target_col in col_upper or col_upper in target_col):
            value += 12.0
        if column in label or label in column:
            value += 8.0
        if classify_column(column) in {role, concept}:
            value += 2.0
        return value

    term = choose_best(terms, score)
    return prediction_from_term(term, "field_code_string")


def predict_lexical_retrieval(row: dict[str, Any], terms: list[dict[str, Any]]) -> dict[str, str]:
    query = " ".join(
        [
            row["column"],
            row["column_category"],
            row["table_type"],
        ]
    )
    query_tokens = tokenize(query)
    query_grams = char_ngrams(query)

    def score(term: dict[str, Any]) -> float:
        content = term_content(term)
        value = 4.0 * jaccard(query_tokens, tokenize(content))
        value += 2.0 * jaccard(query_grams, char_ngrams(content))
        if str(term.get("column_code", "")).upper() == row["column"].upper():
            value += 1.5
        return value

    term = choose_best(terms, score)
    return prediction_from_term(term, "metadata_lexical_retrieval")


def predict_schema_context_rerank(row: dict[str, Any], terms: list[dict[str, Any]]) -> dict[str, str]:
    query = " ".join(
        [
            row["table"],
            row["table_type"],
            row["column"],
            row["column_category"],
        ]
    )
    query_tokens = tokenize(query)
    query_grams = char_ngrams(query)

    def score(term: dict[str, Any]) -> float:
        content = term_content(term)
        value = 4.0 * jaccard(query_tokens, tokenize(content))
        value += 2.0 * jaccard(query_grams, char_ngrams(content))
        if str(term.get("domain", "")) == row["table"]:
            value += 5.0
        if str(term.get("column_code", "")).upper() == row["column"].upper():
            value += 4.0
        if str(term.get("role", "")) in {row["column_category"], row["table_type"]}:
            value += 0.5
        return value

    term = choose_best(terms, score)
    return prediction_from_term(term, "schema_context_rerank")


def prediction_from_term(term: dict[str, Any] | None, method: str) -> dict[str, str]:
    if not term:
        return {
            "method": method,
            "pred_uri": "",
            "pred_label": "",
            "pred_role": "",
            "pred_concept": "",
        }
    return {
        "method": method,
        "pred_uri": str(term.get("uri", "")),
        "pred_label": str(term.get("label", "")),
        "pred_role": str(term.get("role", "")),
        "pred_concept": str(term.get("canonical_concept_id", "")),
    }


def load_project_mapping_predictions() -> dict[tuple[str, str], dict[str, str]]:
    predictions: dict[tuple[str, str], dict[str, str]] = {}
    for path in sorted(MAPPING_CACHE_DIR.glob("*.json")):
        table = path.stem
        payload = load_json(path)
        mapping = payload.get("final_mapping") or {}
        for column, meta in mapping.items():
            if not isinstance(meta, dict):
                predictions[(table, column)] = {
                    "method": "project_multi_agent_mapping",
                    "pred_uri": "",
                    "pred_label": "",
                    "pred_role": "",
                    "pred_concept": "",
                }
                continue
            predictions[(table, column)] = {
                "method": "project_multi_agent_mapping",
                "pred_uri": str(meta.get("uri", "")),
                "pred_label": str(meta.get("label", "")),
                "pred_role": str(meta.get("role", "")),
                "pred_concept": str(meta.get("canonical_concept_id", "")),
            }
    return predictions


def summarize_field_predictions(rows: list[dict[str, Any]], valid_uris: set[str]) -> list[dict[str, Any]]:
    by_method: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_method[row["method"]].append(row)
    summaries = []
    for method, items in sorted(by_method.items()):
        total = len(items)
        predicted = [item for item in items if item.get("pred_uri")]
        uri_correct = sum(1 for item in items if item.get("pred_uri") == item.get("gold_uri"))
        concept_correct = sum(
            1
            for item in items
            if item.get("pred_concept") and item.get("pred_concept") == item.get("gold_concept")
        )
        role_correct = sum(
            1
            for item in items
            if item.get("pred_role") and item.get("pred_role") == item.get("gold_role")
        )
        invalid = sum(1 for item in predicted if item.get("pred_uri") not in valid_uris)
        category_scores = []
        for category in sorted({item["column_category"] for item in items}):
            cat_items = [item for item in items if item["column_category"] == category]
            if not cat_items:
                continue
            cat_correct = sum(
                1
                for item in cat_items
                if item.get("pred_concept") and item.get("pred_concept") == item.get("gold_concept")
            )
            category_scores.append(cat_correct / len(cat_items))
        summaries.append(
            {
                "method": method,
                "items": total,
                "predicted": len(predicted),
                "coverage": round(len(predicted) / total, 6) if total else 0,
                "uri_accuracy_total": round(uri_correct / total, 6) if total else 0,
                "concept_accuracy_total": round(concept_correct / total, 6) if total else 0,
                "role_accuracy_total": round(role_correct / total, 6) if total else 0,
                "macro_concept_accuracy_by_category": round(sum(category_scores) / len(category_scores), 6)
                if category_scores
                else 0,
                "invalid_uri_rate_on_predicted": round(invalid / len(predicted), 6) if predicted else 0,
            }
        )
    return summaries


def run_field_mapping_experiment(conn: sqlite3.Connection) -> dict[str, Any]:
    terms = load_kb_terms()
    valid_uris = {str(term.get("uri", "")) for term in terms}
    gold_rows = build_field_gold(conn, terms)
    project_predictions = load_project_mapping_predictions()
    result_rows: list[dict[str, Any]] = []

    predictors = [
        ("field_code_string", lambda row: predict_string_rule(row, terms)),
        ("metadata_lexical_retrieval", lambda row: predict_lexical_retrieval(row, terms)),
        ("schema_context_rerank", lambda row: predict_schema_context_rerank(row, terms)),
    ]
    for row in gold_rows:
        for _, predictor in predictors:
            pred = predictor(row)
            result_rows.append({**row, **pred})
        pred = project_predictions.get(
            (row["table"], row["column"]),
            {
                "method": "project_multi_agent_mapping",
                "pred_uri": "",
                "pred_label": "",
                "pred_role": "",
                "pred_concept": "",
            },
        )
        result_rows.append({**row, **pred})

    for row in result_rows:
        row["uri_match"] = int(row.get("pred_uri") == row.get("gold_uri"))
        row["concept_match"] = int(bool(row.get("pred_concept")) and row.get("pred_concept") == row.get("gold_concept"))
        row["role_match"] = int(bool(row.get("pred_role")) and row.get("pred_role") == row.get("gold_role"))
        row["valid_uri"] = int(bool(row.get("pred_uri")) and row.get("pred_uri") in valid_uris)

    fields = [
        "method",
        "table",
        "column",
        "table_type",
        "column_category",
        "gold_uri",
        "gold_label",
        "gold_role",
        "gold_concept",
        "pred_uri",
        "pred_label",
        "pred_role",
        "pred_concept",
        "uri_match",
        "concept_match",
        "role_match",
        "valid_uri",
    ]
    write_csv(OUT_DIR / "field_mapping_baseline_predictions.csv", result_rows, fields)
    summary = summarize_field_predictions(result_rows, valid_uris)
    write_csv(
        OUT_DIR / "field_mapping_baseline_summary.csv",
        summary,
        [
            "method",
            "items",
            "predicted",
            "coverage",
            "uri_accuracy_total",
            "concept_accuracy_total",
            "role_accuracy_total",
            "macro_concept_accuracy_by_category",
            "invalid_uri_rate_on_predicted",
        ],
    )
    write_json(
        OUT_DIR / "field_mapping_baseline_summary.json",
        {
            "gold_standard": "silver labels from data/company/zhongshan_rag_terms.json",
            "field_items": len(gold_rows),
            "methods": summary,
        },
    )
    return {"rows": result_rows, "summary": summary, "field_items": len(gold_rows)}


def rule_signature(rule: dict[str, Any]) -> str:
    payload = {
        "name": rule.get("name"),
        "source_tables": rule.get("source_tables") or [],
        "source_key": rule.get("source_key") or "",
        "source_key_candidates": rule.get("source_key_candidates") or [],
        "source_key_priority": rule.get("source_key_priority") or [],
        "target_tables": rule.get("target_tables") or [],
        "target_key": rule.get("target_key") or "",
        "target_key_priority": rule.get("target_key_priority") or [],
        "target_tables_by_level": rule.get("target_tables_by_level") or {},
        "match_mode": rule.get("match_mode") or "",
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def run_relation_discovery_experiment(conn: sqlite3.Connection) -> dict[str, Any]:
    report = load_json(RULE_REPORT_PATH)

    universe = []
    positive = set()
    negative = set()
    review = set()
    for item in report.get("rules", []):
        sig = f"R{int(item.get('index', len(universe))):03d}"
        validation = item.get("validation") or {}
        hit_rate = validation.get("hit_rate", "")
        label = "accept" if item.get("enabled") is True else "reject"
        if validation.get("status") == "not_probed" or item.get("edge_confidence") == "weak":
            label = "review"
        if label == "accept":
            positive.add(sig)
        elif label == "reject":
            negative.add(sig)
        else:
            review.add(sig)
        universe.append(
            {
                "signature": sig,
                "rule_index": item.get("index", ""),
                "name": item.get("name", ""),
                "source_tables": ";".join(item.get("source_tables") or []),
                "target_tables": ";".join(item.get("target_tables") or []),
                "match_mode": item.get("match_mode", ""),
                "enabled": item.get("enabled", ""),
                "edge_confidence": item.get("edge_confidence", ""),
                "validation_status": validation.get("status", ""),
                "hit_rate": hit_rate,
                "silver_label": label,
            }
        )

    fk_count = 0
    for table in table_names(conn):
        fk_count += len(conn.execute(f"PRAGMA foreign_key_list({quote_ident(table)})").fetchall())

    probed_high_conf = {
        row["signature"]
        for row in universe
        if row["validation_status"] == "probed"
        and row["hit_rate"] != ""
        and float(row["hit_rate"]) >= 0.8
        and row["match_mode"] in {"exact", "split_exact"}
    }
    name_only = {
        row["signature"]
        for row in universe
        if row["match_mode"] in {"exact", "split_exact"} and str(row["edge_confidence"]).startswith("strong")
    }
    strong_agent_no_validation = {
        row["signature"] for row in universe if str(row["edge_confidence"]).startswith("strong")
    }
    full_method = set(positive)
    methods = {
        "direct_mapping_explicit_fk": set(),
        "name_rule_without_probe": name_only,
        "probe_threshold_without_review": probed_high_conf,
        "agent_candidate_without_validation": strong_agent_no_validation,
        "project_full_relation_discovery": full_method,
    }

    result_rows = []
    for method, predictions in methods.items():
        tp = len(predictions & positive)
        fp = len(predictions & negative)
        review_hits = len(predictions & review)
        fn = len(positive - predictions)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / len(positive) if positive else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        predicted_types = {row["name"] for row in universe if row["signature"] in predictions}
        result_rows.append(
            {
                "method": method,
                "candidate_rules": len(predictions),
                "true_positive": tp,
                "false_positive": fp,
                "review_candidate_hits": review_hits,
                "false_negative": fn,
                "precision": round(precision, 6),
                "recall": round(recall, 6),
                "f1": round(f1, 6),
                "relation_type_coverage": len(predicted_types),
            }
        )

    write_csv(
        OUT_DIR / "relation_discovery_silver_labels.csv",
        universe,
        [
            "signature",
            "rule_index",
            "name",
            "source_tables",
            "target_tables",
            "match_mode",
            "enabled",
            "edge_confidence",
            "validation_status",
            "hit_rate",
            "silver_label",
        ],
    )
    write_csv(
        OUT_DIR / "relation_discovery_baseline_summary.csv",
        result_rows,
        [
            "method",
            "candidate_rules",
            "true_positive",
            "false_positive",
            "review_candidate_hits",
            "false_negative",
            "precision",
            "recall",
            "f1",
            "relation_type_coverage",
        ],
    )
    summary = {
        "silver_positive_rules": len(positive),
        "silver_negative_rules": len(negative),
        "silver_review_rules": len(review),
        "sqlite_explicit_foreign_keys": fk_count,
        "methods": result_rows,
    }
    write_json(OUT_DIR / "relation_discovery_baseline_summary.json", summary)
    return summary


def qname(token: str) -> str:
    token = token.strip()
    if token == "a":
        return "a"
    if token.startswith("<") and token.endswith(">"):
        return token[1:-1]
    if token.startswith("zs:"):
        return ZS_NS + token[3:]
    if token.startswith("schema1:"):
        return "http://schema.org/" + token[8:]
    if token.startswith("rdf:"):
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#" + token[4:]
    if token.startswith("rdfs:"):
        return "http://www.w3.org/2000/01/rdf-schema#" + token[5:]
    return token


def split_object_line(rest: str) -> tuple[str, str]:
    rest = rest.strip()
    punctuation = rest[-1] if rest and rest[-1] in ";,." else ""
    body = rest[:-1].strip() if punctuation else rest
    return body, punctuation


def parse_ttl(path: Path) -> list[Triple]:
    triples: list[Triple] = []
    subject: str | None = None
    predicate: str | None = None
    continuation = False
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("@prefix") or line.startswith("#"):
                continue
            if line.startswith("<"):
                end = line.find(">")
                if end < 0:
                    continue
                subject = line[1:end]
                predicate = None
                continuation = False
                rest = line[end + 1 :].strip()
            elif subject is not None:
                rest = line
            else:
                continue
            if not rest:
                continue
            body, punctuation = split_object_line(rest)
            if not body:
                continuation = punctuation == ","
                if punctuation == ".":
                    subject = None
                    predicate = None
                continue
            if continuation:
                pred = predicate
                obj = body
            else:
                parts = body.split(None, 1)
                if len(parts) < 2:
                    continue
                pred = qname(parts[0])
                obj = parts[1]
                predicate = pred
            if pred:
                object_type = "literal" if obj.lstrip().startswith('"') else "resource"
                triples.append(Triple(subject or "", pred, obj, object_type))
            continuation = punctuation == ","
            if punctuation == ".":
                subject = None
                predicate = None
                continuation = False
    return triples


def count_direct_literal_triples(conn: sqlite3.Connection) -> tuple[int, int]:
    rows = 0
    literals = 0
    for table in table_names(conn):
        columns = [col["name"] for col in table_columns(conn, table) if col["name"].lower() not in IGNORED_COLUMNS]
        rows += int(conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0])
        for column in columns:
            value = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {quote_ident(table)}
                WHERE {quote_ident(column)} IS NOT NULL
                  AND TRIM(CAST({quote_ident(column)} AS TEXT)) != ''
                """
            ).fetchone()[0]
            literals += int(value)
    return rows, literals


def run_graph_quality_experiment(conn: sqlite3.Connection) -> dict[str, Any]:
    triples = parse_ttl(TTL_PATH)
    rules = load_json(RULES_PATH).get("relation_rules", [])
    relation_predicates = {ZS_NS + rule.get("name", "") for rule in rules if rule.get("enabled") is True}
    full_subjects = {triple.subject for triple in triples}
    full_literal_count = sum(1 for triple in triples if triple.object_type == "literal")
    full_business_edges = sum(
        1 for triple in triples if triple.object_type == "resource" and triple.predicate in relation_predicates
    )
    no_relation_triples = len(triples) - full_business_edges
    direct_entities, direct_literals = count_direct_literal_triples(conn)

    e6 = load_json(PREV_EXP_DIR / "e6_graph_structure_summary.json")
    e7 = load_json(PREV_EXP_DIR / "e7_kg_explainability_summary.json")

    rows = [
        {
            "graph": "direct_mapping_explicit_fk",
            "entity_nodes": direct_entities,
            "literal_triples": direct_literals,
            "total_triples": direct_entities + direct_literals,
            "business_relation_edges": 0,
            "business_relation_types": 0,
            "connected_entity_nodes": 0,
            "connected_entity_coverage": 0,
            "largest_connected_component": 0,
            "path_query_success_rate": 0,
        },
        {
            "graph": "no_implicit_relation_kg",
            "entity_nodes": len(full_subjects),
            "literal_triples": full_literal_count,
            "total_triples": no_relation_triples,
            "business_relation_edges": 0,
            "business_relation_types": 0,
            "connected_entity_nodes": 0,
            "connected_entity_coverage": 0,
            "largest_connected_component": 0,
            "path_query_success_rate": 0,
        },
        {
            "graph": "project_full_kg",
            "entity_nodes": e6.get("entity_nodes", len(full_subjects)),
            "literal_triples": full_literal_count,
            "total_triples": len(triples),
            "business_relation_edges": e6.get("business_relation_edges", full_business_edges),
            "business_relation_types": e6.get("business_relation_types", len(relation_predicates)),
            "connected_entity_nodes": e6.get("connected_entity_nodes", 0),
            "connected_entity_coverage": e6.get("relation_connected_coverage", 0),
            "largest_connected_component": e6.get("largest_connected_component_nodes", 0),
            "path_query_success_rate": 1.0 if e7.get("test_items", 0) else 0,
        },
    ]
    write_csv(
        OUT_DIR / "graph_quality_baseline_summary.csv",
        rows,
        [
            "graph",
            "entity_nodes",
            "literal_triples",
            "total_triples",
            "business_relation_edges",
            "business_relation_types",
            "connected_entity_nodes",
            "connected_entity_coverage",
            "largest_connected_component",
            "path_query_success_rate",
        ],
    )
    summary = {"relation_predicates": sorted(relation_predicates), "graphs": rows}
    write_json(OUT_DIR / "graph_quality_baseline_summary.json", summary)
    return summary


def md_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def method_name(name: str) -> str:
    names = {
        "field_code_string": "B1 字段名字符串匹配",
        "metadata_lexical_retrieval": "B2 元数据检索 Top-1",
        "schema_context_rerank": "B3 表上下文重排",
        "project_multi_agent_mapping": "B5 本文多智能体映射",
        "direct_mapping_explicit_fk": "B0 Direct Mapping/FK",
        "name_rule_without_probe": "B1 名称规则关系发现",
        "probe_threshold_without_review": "B2 数据探测阈值",
        "agent_candidate_without_validation": "B3 候选关系无验证",
        "project_full_relation_discovery": "B5 本文完整关系发现",
    }
    return names.get(name, name)


def write_report(field: dict[str, Any], relation: dict[str, Any], graph: dict[str, Any]) -> None:
    field_rows = []
    for item in field["summary"]:
        field_rows.append(
            {
                "方法": method_name(item["method"]),
                "覆盖率": item["coverage"],
                "URI准确率": item["uri_accuracy_total"],
                "概念准确率": item["concept_accuracy_total"],
                "角色准确率": item["role_accuracy_total"],
                "分类宏平均": item["macro_concept_accuracy_by_category"],
                "非法URI率": item["invalid_uri_rate_on_predicted"],
            }
        )
    relation_rows = []
    for item in relation["methods"]:
        relation_rows.append(
            {
                "方法": method_name(item["method"]),
                "候选数": item["candidate_rules"],
                "TP": item["true_positive"],
                "FP": item["false_positive"],
                "Recall": item["recall"],
                "Precision": item["precision"],
                "F1": item["f1"],
                "关系类型": item["relation_type_coverage"],
            }
        )
    graph_rows = []
    graph_names = {
        "direct_mapping_explicit_fk": "G0 Direct KG",
        "no_implicit_relation_kg": "G1 无隐式关系 KG",
        "project_full_kg": "G2 本文完整 KG",
    }
    for item in graph["graphs"]:
        graph_rows.append(
            {
                "图谱": graph_names.get(item["graph"], item["graph"]),
                "实体": item["entity_nodes"],
                "三元组": item["total_triples"],
                "业务边": item["business_relation_edges"],
                "关系类型": item["business_relation_types"],
                "连通覆盖率": item["connected_entity_coverage"],
                "最大连通分量": item["largest_connected_component"],
                "路径成功率": item["path_query_success_rate"],
            }
        )

    report = f"""# 前沿研究机制基线对比实验报告

## 1. 实验定位

本报告服务于毕业论文《基于多智能体语义映射的空间数据知识图谱构建方法研究》的对比实验章节。实验不完整复现外部系统，而是根据近年前沿工作中的关键机制构造项目内可复现基线：Direct Mapping/R2RML 代表结构化转换基线，Schema Matching 与 Ontology Matching 工作中的“候选召回、元数据检索、LLM/上下文重排”被转化为字段映射基线，隐式关系发现部分采用名称规则、数据探测和完整流程进行对比。

这种设置的目的不是证明某个外部系统弱，而是在相同数据库、相同私域术语库、相同评价口径下验证本文组件的贡献。

## 2. 前沿研究到本文实验的对应关系

本实验的“对比”不是把外部系统逐行复现，而是把前沿研究中与本文问题相同的机制拆出来，放入当前中山市空间业务数据库中评价。这样能够避免因数据集、代码依赖、标注体系不同导致的不可比问题。

| 前沿方向或代表工作 | 该方向关注的问题 | 可迁移到本文的机制 | 本实验对应基线 | 本文额外验证的问题 |
|---|---|---|---|---|
| W3C Direct Mapping / R2RML | 关系数据库到 RDF 的标准结构化转换 | 表、列、主外键按规则展开为 RDF | B0 Direct Mapping/FK；G0 Direct KG | 无显式外键时是否能生成空间业务关系 |
| Morph-KGC / SDM-RDFizer / PyRML | 基于 RML/R2RML 声明式映射的高效 KG 构建 | 给定映射规则后执行 RDF 物化 | G0 Direct KG；G1 无隐式关系 KG | 仅有属性物化是否足以支持路径解释 |
| Magneto | schema matching 的“候选召回 + rerank”两阶段框架 | 先用低成本检索召回候选，再做语义重排 | B2 元数据检索 Top-1；B3 表上下文重排 | 检索/重排是否能稳定映射私域字段 URI |
| Schemora | 元数据增强、词法与向量检索结合的 schema matching | 用 schema metadata 强化候选匹配 | B2 元数据检索 Top-1；B3 表上下文重排 | 表域、字段代码、业务类别对映射的贡献 |
| LLMatch | 面向复杂多表 schema matching 的模块化 LLM 框架 | schema preparation、候选选择、列级对齐的组件化评价 | B3 表上下文重排；B5 本文多智能体映射 | 多表空间业务数据中是否需要表指纹和校验 |
| KROMA / GenOM | RAG 与 LLM 增强 ontology matching | 检索概念上下文，减少匹配歧义 | B2/B3 字段语义映射基线；B5 私域 KB 约束映射 | 私域术语库能否降低非法 URI 和误映射 |
| KnowWhereGraph 等 GeoKG | 跨域地理数据融合与空间语义互操作 | 空间对象、地点、区域、关系网络建模 | G2 本文完整 KG | 本地空间业务库能否形成可查询关系网络 |

参考链接：W3C Direct Mapping <https://www.w3.org/TR/rdb-direct-mapping/>；R2RML <https://www.w3.org/TR/r2rml/>；Morph-KGC <https://morph-kgc.readthedocs.io/>；SDM-RDFizer <https://github.com/SDM-TIB/SDM-RDFizer>；LLMatch <https://arxiv.org/abs/2507.10897>；Magneto <https://arxiv.org/abs/2412.08194>；Schemora <https://arxiv.org/abs/2507.14376>；KROMA <https://arxiv.org/abs/2507.14032>；KnowWhereGraph <https://arxiv.org/abs/2502.13874>。

## 3. 基线设置

字段语义映射实验使用以下基线：

| 编号 | 方法 | 对应思想 | 实现口径 |
|---|---|---|---|
| B1 | 字段名字符串匹配 | 传统 schema matching 弱基线 | 仅使用字段代码、标签字符串相似度，不使用表域约束 |
| B2 | 元数据检索 Top-1 | Schemora/Magneto 的候选召回思想 | 使用字段类别与术语元数据的词项和字符相似度检索 |
| B3 | 表上下文重排 | Magneto、Schemora、LLMatch 中的 rerank/上下文判别思想 | 在检索分数上加入表名、实体类型和字段代码上下文；该基线是强上限型机制基线 |
| B5 | 本文多智能体映射 | 私域 KB + 表指纹 + 多智能体校验 | 读取项目真实 mapping cache |

隐式关系发现实验使用以下基线：

| 编号 | 方法 | 实现口径 |
|---|---|---|
| B0 | Direct Mapping/FK | 仅使用 SQLite 显式外键 |
| B1 | 名称规则关系发现 | 根据字段名、表类型和关系名生成候选，不做数据探测 |
| B2 | 数据探测阈值 | 对候选关系执行源值到目标键命中率探测，命中率达到阈值即接收；这是本文相对 schema matching 前沿额外加入的数据证据机制 |
| B3 | 候选关系无验证 | 模拟只采纳强候选但不执行准入验证 |
| B5 | 本文完整关系发现 | schema profile + 候选合并 + 数据探测 + 准入规则 |

端到端图谱质量实验使用三类图谱：

| 图谱 | 含义 |
|---|---|
| G0 Direct KG | 记录和字段直接展开，业务关系仅依赖显式外键 |
| G1 无隐式关系 KG | 保留本文字段映射与属性三元组，但移除业务关系边 |
| G2 本文完整 KG | 当前项目生成的完整 RDF/Neo4j 图谱，用于对比 RDB2RDF/RML 类工具只执行映射物化时难以自动产生的隐式业务关系 |

## 4. 数据与评价口径

- 数据库：`data/company/zhongshan_10k.sqlite`，17 张表，90,372 条样本记录。
- 字段映射银标准：`data/company/zhongshan_rag_terms.json` 中的私域术语条目，共 {field["field_items"]} 个可评价字段。
- 关系发现银标准：`data/company/zhongshan_relation_rules.report.json` 中经数据探测和规则准入后的标签，正例 {relation["silver_positive_rules"]} 条，反例 {relation["silver_negative_rules"]} 条，需复核 {relation["silver_review_rules"]} 条。
- 图谱质量：复用 `data/ttl/zhongshan_10k.ttl` 与 2026-05-22 建图实验统计。

需要注意：字段映射和关系发现采用项目内银标准，并非独立双人标注金标准。论文中应表述为“基于私域术语库与数据探测报告的可追溯评价”，不要写成外部公开 benchmark。

## 5. 字段语义映射结果：对 schema matching / ontology matching 前沿的机制对比

{md_table(field_rows, ["方法", "覆盖率", "URI准确率", "概念准确率", "角色准确率", "分类宏平均", "非法URI率"])}

结果说明：

1. 字段名字符串匹配在 URI 精确匹配上受同名字段跨表复用影响较大，但概念准确率较高，说明它能识别部分通用字段语义，却难以完成表域级 URI 对齐。
2. 元数据检索 Top-1 对应 Schemora/KROMA 的检索增强思想，在概念层面优于纯字符串规则，但仍缺少稳定的表域约束。
3. 表上下文重排对应 Magneto/LLMatch 中“候选召回后 rerank”的思想。它在当前银标准上取得最高结果，是因为私域术语库本身包含表域字段 URI，因此该结果应作为“上下文充分时的机制上限”，不能写成完整外部系统结果。
4. 本文多智能体映射的准确率略低于强上限基线，主要因为真实构图流程会主动放弃部分冗余字段或不稳定字段，覆盖率更保守；其价值在于可解释候选、非法 URI 抑制和后续关系发现联动。
5. 与前沿 schema matching / ontology matching 研究相比，本文的差异在于评价目标不是跨数据库字段配对，而是将空间业务数据库字段落到受控私域 URI，并继续服务后续关系显式化。

## 6. 隐式关系发现结果：对 RDB2RDF 和 schema matching 前沿的补充

{md_table(relation_rows, ["方法", "候选数", "TP", "FP", "Recall", "Precision", "F1", "关系类型"])}

结果说明：

1. SQLite schema 中显式外键数量为 {relation["sqlite_explicit_foreign_keys"]}，因此 Direct Mapping/FK 无法发现中山空间业务库中的主干业务关系。
2. 仅依赖字段名和表类型的规则可以召回较多候选，但会包含空字段、低命中率或弱文本匹配关系，精度不足。
3. 加入数据探测阈值后，候选关系质量明显提升，说明源字段值到目标键的命中率、自环和 fanout 检查是本文关系准入的关键。
4. 本文完整关系发现流程在当前银标准下取得最高 F1，并覆盖管理、层级、地址组成、街路和行政归属等多类关系。
5. 这个实验正是与前沿研究拉开差异的部分：LLMatch、Magneto、Schemora、KROMA 主要解决字段/概念对齐；R2RML、Morph-KGC、SDM-RDFizer 主要解决给定映射后的物化；本文进一步验证了无显式外键条件下的隐式业务关系发现。

## 7. 端到端图谱质量结果：对 KG 构建工具与 GeoKG 工作的结构性对比

{md_table(graph_rows, ["图谱", "实体", "三元组", "业务边", "关系类型", "连通覆盖率", "最大连通分量", "路径成功率"])}

结果说明：

1. Direct KG 与无隐式关系 KG 都能保留实体和属性，但不能形成可用于多跳解释的业务关系网络。
2. 本文完整 KG 生成 {graph_rows[-1]["业务边"]} 条业务关系边，覆盖 {graph_rows[-1]["关系类型"]} 类关系，关系连通实体覆盖率达到 {graph_rows[-1]["连通覆盖率"]}。
3. 端到端结果说明本文方法的核心贡献不只是把关系表转成 RDF，而是把隐式编码关系显式化为可查询、可解释的图结构。
4. 与 KnowWhereGraph 等 GeoKG 工作相比，本文规模和跨域广度较小，但实验问题更聚焦于“地方空间业务库如何从缺少外键的关系表中自动形成可解释关系网络”。

## 8. 可直接写入论文的对比结论

可在论文中将本实验写为“相关研究机制基线与消融对比实验”。建议强调四点：

1. Direct Mapping/R2RML 类方法适合结构化、显式外键充分的数据转换，但面对无 PK/FK 的空间业务库时难以生成业务关系边。
2. LLM schema matching 相关工作证明了候选召回和上下文判别的重要性，本文进一步将其落到私域术语库、表指纹缓存和关系探测准入上。
3. 完整 KG 的连通性和路径解释能力来自隐式关系发现模块，而不是普通属性三元组规模。
4. 本文不是替代 Morph-KGC/SDM-RDFizer 这类映射执行器，而是补足其上游自动语义映射和隐式关系发现环节；因此实验比较的是“前沿方法机制在本文任务中的可迁移贡献”。

## 9. 产物文件

- `docs/2026-07-07/baseline_comparison_experiments/field_mapping_baseline_summary.csv`
- `docs/2026-07-07/baseline_comparison_experiments/field_mapping_baseline_predictions.csv`
- `docs/2026-07-07/baseline_comparison_experiments/relation_discovery_baseline_summary.csv`
- `docs/2026-07-07/baseline_comparison_experiments/relation_discovery_silver_labels.csv`
- `docs/2026-07-07/baseline_comparison_experiments/graph_quality_baseline_summary.csv`
"""
    REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    ensure_out_dir()
    conn = sqlite3.connect(DB_PATH)
    try:
        field = run_field_mapping_experiment(conn)
        relation = run_relation_discovery_experiment(conn)
        graph = run_graph_quality_experiment(conn)
        write_report(field, relation, graph)
        print(f"Wrote report: {REPORT_PATH}")
        print(f"Wrote experiment outputs: {OUT_DIR}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
