from __future__ import annotations

import csv
import json
import re
import sqlite3
import urllib.parse
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/company/zhongshan_10k.sqlite"
TTL_PATH = ROOT / "data/ttl/zhongshan_10k.ttl"
RULES_PATH = ROOT / "data/company/zhongshan_relation_rules.json"
RULE_REPORT_PATH = ROOT / "data/company/zhongshan_relation_rules.report.json"
MAPPING_CACHE_DIR = ROOT / "data/mapping_cache/zhongshan_10k"
OUT_DIR = ROOT / "docs/2026-05-22/kg_construction_experiments"
REPORT_PATH = ROOT / "docs/2026-05-22/kg_construction_experiments_report.md"

ZS_NS = "http://example.org/zhongshan/"
DATA_NS = "http://example.org/data/"
SCHEMA_NS = "http://schema.org/"


@dataclass(frozen=True)
class Triple:
    subject: str
    predicate: str
    obj: str
    object_type: str


def ensure_out_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def qname(token: str) -> str:
    token = token.strip()
    if token == "a":
        return "a"
    if token.startswith("<") and token.endswith(">"):
        return token[1:-1]
    if token.startswith("zs:"):
        return ZS_NS + token[3:]
    if token.startswith("schema1:"):
        return SCHEMA_NS + token[8:]
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


def object_type(obj: str) -> str:
    if obj.lstrip().startswith('"'):
        return "literal"
    return "resource"


def literal_value(obj: str) -> str:
    text = obj.strip()
    if not text.startswith('"'):
        return text
    escaped = False
    chars: list[str] = []
    for ch in text[1:]:
        if escaped:
            chars.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"':
            break
        chars.append(ch)
    return "".join(chars)


def parse_ttl(path: Path) -> list[Triple]:
    triples: list[Triple] = []
    subject: str | None = None
    predicate: str | None = None
    expect_continuation_object = False

    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("@prefix") or line.startswith("#"):
                continue
            if line.startswith("<"):
                end = line.find(">")
                if end == -1:
                    continue
                subject = line[1:end]
                predicate = None
                expect_continuation_object = False
                rest = line[end + 1 :].strip()
            elif subject is not None:
                rest = line
            else:
                continue

            if not rest:
                continue
            body, punctuation = split_object_line(rest)
            if not body:
                expect_continuation_object = punctuation == ","
                if punctuation == ".":
                    subject = None
                    predicate = None
                continue
            if expect_continuation_object:
                pred = predicate
                obj = body
            else:
                parts = body.split(None, 1)
                if len(parts) < 2:
                    expect_continuation_object = punctuation == ","
                    if punctuation == ".":
                        subject = None
                        predicate = None
                    continue
                pred = qname(parts[0])
                obj = parts[1]
                predicate = pred
            if pred is None:
                continue
            triples.append(Triple(subject, pred, obj, object_type(obj)))
            expect_continuation_object = punctuation == ","
            if punctuation == ".":
                subject = None
                predicate = None
                expect_continuation_object = False
    return triples


def table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [r[0] for r in rows if not str(r[0]).startswith("sqlite_")]


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"null", "none", "nan"}:
        return ""
    return text


def load_rules() -> dict[str, Any]:
    return json.loads(RULES_PATH.read_text(encoding="utf-8"))


def load_rule_report() -> dict[str, Any]:
    return json.loads(RULE_REPORT_PATH.read_text(encoding="utf-8"))


def pick_first(row: sqlite3.Row, fields: list[str]) -> str:
    keys = set(row.keys())
    for field in fields:
        if field not in keys:
            continue
        value = clean_value(row[field])
        if value:
            return value
    return ""


def entity_id_for_row(rules: dict[str, Any], table: str, row: sqlite3.Row, fallback: str) -> str:
    fields = rules.get("entity_key_priority", {}).get(table) or rules.get("entity_key_priority", {}).get("default") or ["GUID", "DM", "gid"]
    return pick_first(row, [str(f) for f in fields]) or fallback


def uri_for_entity(table: str, entity_id: str) -> str:
    return DATA_NS + urllib.parse.quote(str(table), safe="") + "/" + urllib.parse.quote(str(entity_id), safe="")


def parse_entity_uri(uri: str) -> tuple[str, str] | None:
    if not uri.startswith(DATA_NS):
        return None
    rest = uri[len(DATA_NS) :]
    if "/" not in rest:
        return None
    table, entity_id = rest.split("/", 1)
    return urllib.parse.unquote(table), urllib.parse.unquote(entity_id)


def build_entity_uri_index(conn: sqlite3.Connection, rules: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for table in table_names(conn):
        cols = table_columns(conn, table)
        select_cols = ", ".join(quote_ident(c) for c in cols)
        for i, row in enumerate(conn.execute(f"SELECT {select_cols} FROM {quote_ident(table)}")):
            entity_id = entity_id_for_row(rules, table, row, f"row_{i}")
            uri = uri_for_entity(table, entity_id)
            index[uri] = {"table": table, "entity_id": entity_id, "row": dict(row), "row_index": i}
    return index


def ttl_indexes(triples: list[Triple]) -> tuple[dict[str, dict[str, list[str]]], dict[tuple[str, str], set[str]]]:
    by_subject: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    literal_values: dict[tuple[str, str], set[str]] = defaultdict(set)
    for t in triples:
        by_subject[t.subject][t.predicate].append(t.obj)
        if t.object_type == "literal":
            literal_values[(t.subject, t.predicate)].add(literal_value(t.obj))
    return by_subject, literal_values


def experiment_e1(conn: sqlite3.Connection, triples: list[Triple], entity_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    table_counts = {table: conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0] for table in table_names(conn)}
    subjects = {t.subject for t in triples}
    predicates = {t.predicate for t in triples}
    literal_count = sum(1 for t in triples if t.object_type == "literal")
    resource_count = len(triples) - literal_count
    non_type_resource_count = sum(1 for t in triples if t.object_type == "resource" and t.predicate != "a")
    rows = []
    for table, count in table_counts.items():
        subject_count = sum(1 for uri, meta in entity_index.items() if meta["table"] == table and uri in subjects)
        rows.append({
            "table": table,
            "sqlite_rows": count,
            "kg_subjects": subject_count,
            "entity_coverage": round(subject_count / count, 6) if count else 1,
        })
    write_csv(OUT_DIR / "e1_entity_coverage_by_table.csv", rows, ["table", "sqlite_rows", "kg_subjects", "entity_coverage"])
    summary = {
        "sqlite_total_rows": sum(table_counts.values()),
        "ttl_unique_subjects": len(subjects),
        "ttl_triples": len(triples),
        "ttl_unique_predicates": len(predicates),
        "literal_triples": literal_count,
        "resource_triples": resource_count,
        "resource_object_non_type_triples": non_type_resource_count,
        "entity_coverage": round(len(subjects & set(entity_index.keys())) / sum(table_counts.values()), 6),
    }
    write_json(OUT_DIR / "e1_graph_scale_summary.json", summary)
    return summary


def classify_column(col: str) -> str:
    c = col.upper()
    if any(k in c for k in ["MC", "QC", "NAME"]):
        return "名称字段"
    if any(k in c for k in ["DM", "BM", "CODE", "GUID", "GID"]):
        return "编码字段"
    if any(k in c for k in ["DZ", "MP", "DH", "DY", "FJ", "YL"]):
        return "地址字段"
    if any(k in c for k in ["GADM", "GAMC", "ZDGA", "S_DM", "S_MC"]):
        return "管理或层级字段"
    if any(k in c for k in ["X", "Y", "JD", "WD", "GEOM", "SHAPE"]):
        return "空间位置字段"
    return "其他字段"


def build_property_testset(conn: sqlite3.Connection, rules: dict[str, Any], max_items: int = 100) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidates: list[tuple[str, str]] = []
    for table in table_names(conn):
        cols = table_columns(conn, table)
        selected_cols = sorted(cols, key=lambda c: (classify_column(c) == "其他字段", c))[:10]
        candidates.extend((table, col) for col in selected_cols)

    # First pass: one value per selected field to maximize table and field coverage.
    for table, col in candidates:
        if len(rows) >= max_items:
            return rows
        query = f"SELECT * FROM {quote_ident(table)} WHERE {quote_ident(col)} IS NOT NULL AND TRIM(CAST({quote_ident(col)} AS TEXT)) != '' LIMIT 1"
        for row in conn.execute(query):
            value = clean_value(row[col])
            if not value:
                continue
            entity_id = entity_id_for_row(rules, table, row, "row")
            rows.append({
                "test_id": f"PV{len(rows)+1:03d}",
                "table": table,
                "column": col,
                "column_category": classify_column(col),
                "entity_id": entity_id,
                "entity_uri": uri_for_entity(table, entity_id),
                "source_value": value,
            })
            break

    # Second pass: add more non-empty examples from covered fields until reaching 100.
    for table, col in candidates:
        if len(rows) >= max_items:
            return rows
        existing = {(r["table"], r["column"], r["entity_uri"], r["source_value"]) for r in rows}
        query = f"SELECT * FROM {quote_ident(table)} WHERE {quote_ident(col)} IS NOT NULL AND TRIM(CAST({quote_ident(col)} AS TEXT)) != '' LIMIT 10"
        for row in conn.execute(query):
            if len(rows) >= max_items:
                return rows
            value = clean_value(row[col])
            if not value:
                continue
            entity_id = entity_id_for_row(rules, table, row, "row")
            entity_uri = uri_for_entity(table, entity_id)
            key = (table, col, entity_uri, value)
            if key in existing:
                continue
            existing.add(key)
            rows.append({
                "test_id": f"PV{len(rows)+1:03d}",
                "table": table,
                "column": col,
                "column_category": classify_column(col),
                "entity_id": entity_id,
                "entity_uri": entity_uri,
                "source_value": value,
            })
    return rows


def experiment_e2(conn: sqlite3.Connection, rules: dict[str, Any], literal_values: dict[tuple[str, str], set[str]]) -> dict[str, Any]:
    testset = build_property_testset(conn, rules, 100)
    result_rows = []
    matched = 0
    for item in testset:
        predicate = ZS_NS + f"{item['table']}.{item['column']}"
        values = literal_values.get((item["entity_uri"], predicate), set())
        ok = item["source_value"] in values
        matched += int(ok)
        result_rows.append({**item, "predicate": predicate, "kg_values": " | ".join(sorted(values)[:3]), "status": "matched" if ok else "missing"})
    fields = ["test_id", "table", "column", "column_category", "entity_id", "entity_uri", "source_value", "predicate", "kg_values", "status"]
    write_csv(OUT_DIR / "e2_property_fidelity_testset_100.csv", result_rows, fields)
    summary = {"test_items": len(testset), "matched": matched, "missing": len(testset) - matched, "coverage_rate": round(matched / len(testset), 6) if testset else 0}
    write_json(OUT_DIR / "e2_property_fidelity_summary.json", summary)
    return summary


def load_mapping_rows(max_items: int = 100) -> list[dict[str, Any]]:
    candidates_by_table: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(MAPPING_CACHE_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        table = path.stem
        mapping = data.get("final_mapping") or {}
        for col, meta in mapping.items():
            if not isinstance(meta, dict):
                meta = {"uri": str(meta)}
            label = str(meta.get("label", "") or "")
            comment = str(meta.get("comment", "") or "")
            role = str(meta.get("role", "") or "")
            concept = str(meta.get("canonical_concept_id", "") or "")
            evidence_score = 0
            col_upper = str(col).upper()
            label_text = label + comment + role + concept
            if col in meta.get("uri", ""):
                evidence_score += 1
            if label:
                evidence_score += 1
            if comment:
                evidence_score += 1
            if role:
                evidence_score += 1
            if concept:
                evidence_score += 1
            if any(k in col_upper for k in ["MC", "QC"]) and any(k in label_text for k in ["名称", "地址", "name"]):
                evidence_score += 1
            if any(k in col_upper for k in ["DM", "BM", "GUID", "GID"]) and any(k in label_text for k in ["编码", "标识", "代码", "id", "guid"]):
                evidence_score += 1
            status = "auto_consistent_pending_human_review" if evidence_score >= 4 else "needs_human_review"
            candidates_by_table.setdefault(table, []).append({
                "test_id": "",
                "table": table,
                "column": col,
                "column_category": classify_column(col),
                "mapped_uri": meta.get("uri", ""),
                "label": label,
                "comment": comment,
                "role": role,
                "canonical_concept_id": concept,
                "auto_evidence_score": evidence_score,
                "auto_review_status": status,
                "human_label": "pending",
            })
    rows: list[dict[str, Any]] = []
    tables = sorted(candidates_by_table)
    index = 0
    while len(rows) < max_items:
        progressed = False
        for table in tables:
            candidates = candidates_by_table.get(table, [])
            if index >= len(candidates):
                continue
            row = dict(candidates[index])
            row["test_id"] = f"SM{len(rows)+1:03d}"
            rows.append(row)
            progressed = True
            if len(rows) >= max_items:
                break
        if not progressed:
            break
        index += 1
    return rows


def experiment_e3() -> dict[str, Any]:
    rows = load_mapping_rows(100)
    fields = ["test_id", "table", "column", "column_category", "mapped_uri", "label", "comment", "role", "canonical_concept_id", "auto_evidence_score", "auto_review_status", "human_label"]
    write_csv(OUT_DIR / "e3_semantic_mapping_review_testset_100.csv", rows, fields)
    counts = Counter(row["auto_review_status"] for row in rows)
    summary = {"test_items": len(rows), **dict(counts), "note": "auto_review_status is a pre-review signal, not final semantic accuracy"}
    write_json(OUT_DIR / "e3_semantic_mapping_prereview_summary.json", summary)
    return summary


def experiment_e4() -> dict[str, Any]:
    report = load_rule_report()
    rows = []
    for rule in report.get("rules", []):
        validation = rule.get("validation") or {}
        rows.append({
            "rule_index": rule.get("index"),
            "name": rule.get("name"),
            "enabled": rule.get("enabled"),
            "edge_confidence": rule.get("edge_confidence"),
            "match_mode": rule.get("match_mode"),
            "source_tables": ";".join(rule.get("source_tables") or []),
            "target_tables": ";".join(rule.get("target_tables") or []),
            "validation_status": validation.get("status"),
            "sampled_rows": validation.get("sampled_rows", ""),
            "non_empty_rows": validation.get("non_empty_rows", ""),
            "source_values": validation.get("source_values", ""),
            "hits": validation.get("hits", ""),
            "hit_rate": validation.get("hit_rate", ""),
            "fanout_by_pair": json.dumps(validation.get("fanout_by_pair", {}), ensure_ascii=False),
            "agent_reason": rule.get("agent_reason", ""),
        })
    fields = ["rule_index", "name", "enabled", "edge_confidence", "match_mode", "source_tables", "target_tables", "validation_status", "sampled_rows", "non_empty_rows", "source_values", "hits", "hit_rate", "fanout_by_pair", "agent_reason"]
    write_csv(OUT_DIR / "e4_relation_rule_effectiveness.csv", rows, fields)
    summary = report.get("summary", {})
    write_json(OUT_DIR / "e4_relation_rule_effectiveness_summary.json", summary)
    return summary


def resource_obj_uri(obj: str) -> str:
    text = obj.strip()
    if text.startswith("<") and text.endswith(">"):
        return text[1:-1]
    return qname(text)


def relation_source_values(rule: dict[str, Any], row: dict[str, Any]) -> list[str]:
    keys = []
    if rule.get("source_key"):
        keys.append(rule.get("source_key"))
    keys.extend(rule.get("source_key_candidates") or [])
    keys.extend(rule.get("source_key_priority") or [])
    split_mode = rule.get("match_mode") == "split_exact"
    delimiter = str(rule.get("split_delimiter", ",") or ",")
    values = []
    for key in keys:
        value = clean_value(row.get(key))
        if not value:
            continue
        if split_mode:
            values.extend(part.strip() for part in value.split(delimiter) if part.strip())
        else:
            values.append(value)
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def target_specs(rule: dict[str, Any], source_row: dict[str, Any]) -> list[tuple[str, str]]:
    if rule.get("target_tables_by_level"):
        level_value = clean_value(source_row.get(rule.get("source_level_field")))
        tables = (rule.get("target_tables_by_level") or {}).get(level_value, [])
        keys = rule.get("target_key_priority") or []
        return [(str(t), str(k)) for t in tables for k in keys]
    tables = rule.get("target_tables") or []
    keys = []
    if rule.get("target_key"):
        keys.append(rule.get("target_key"))
    keys.extend(rule.get("target_key_priority") or [])
    return [(str(t), str(k)) for t in tables for k in keys]


def experiment_e5(triples: list[Triple], rules: dict[str, Any], entity_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rule_by_pred = {ZS_NS + rule.get("name", ""): rule for rule in rules.get("relation_rules", []) if rule.get("enabled") is not False}
    relation_triples = [t for t in triples if t.predicate in rule_by_pred and t.object_type == "resource"]
    by_pred: dict[str, list[Triple]] = defaultdict(list)
    for t in relation_triples:
        by_pred[t.predicate].append(t)
    relation_types = sorted(by_pred)
    rows = []
    # Allocate at least one sample per relation type, then fill round-robin to 100.
    cursors = {pred: 0 for pred in relation_types}
    while len(rows) < 100 and relation_types:
        progressed = False
        for pred in relation_types:
            items = by_pred[pred]
            cursor = cursors[pred]
            if cursor >= len(items):
                continue
            t = items[cursor]
            cursors[pred] += max(1, len(items) // 25)
            progressed = True
            source_meta = entity_index.get(t.subject)
            target_uri = resource_obj_uri(t.obj)
            target_meta = entity_index.get(target_uri)
            rule = rule_by_pred[pred]
            status = "missing_source_or_target"
            support = ""
            if source_meta and target_meta:
                values = relation_source_values(rule, source_meta["row"])
                specs = target_specs(rule, source_meta["row"])
                target_values = []
                for target_table, target_key in specs:
                    if target_meta["table"] != target_table:
                        continue
                    target_values.append(clean_value(target_meta["row"].get(target_key)))
                if set(values) & set(v for v in target_values if v):
                    status = "supported_by_source_value"
                    support = ";".join(sorted(set(values) & set(target_values)))
                else:
                    status = "not_supported_by_sampled_rule_fields"
                    support = f"source_values={values}; target_values={target_values}"
            rows.append({
                "test_id": f"EV{len(rows)+1:03d}",
                "relation": pred.replace(ZS_NS, "zs:"),
                "source_uri": t.subject,
                "target_uri": target_uri,
                "source_table": source_meta.get("table", "") if source_meta else "",
                "source_entity_id": source_meta.get("entity_id", "") if source_meta else "",
                "target_table": target_meta.get("table", "") if target_meta else "",
                "target_entity_id": target_meta.get("entity_id", "") if target_meta else "",
                "validation_status": status,
                "supporting_value_or_note": support,
            })
            if len(rows) >= 100:
                break
        if not progressed:
            break
    fields = ["test_id", "relation", "source_uri", "target_uri", "source_table", "source_entity_id", "target_table", "target_entity_id", "validation_status", "supporting_value_or_note"]
    write_csv(OUT_DIR / "e5_relation_edge_validation_testset_100.csv", rows, fields)
    counts = Counter(row["validation_status"] for row in rows)
    summary = {"test_items": len(rows), **dict(counts)}
    write_json(OUT_DIR / "e5_relation_edge_validation_summary.json", summary)
    return summary


def experiment_e6(triples: list[Triple]) -> dict[str, Any]:
    entity_subjects = {t.subject for t in triples if t.subject.startswith(DATA_NS)}
    relation_edges = []
    relation_counter = Counter()
    adjacency: dict[str, set[str]] = defaultdict(set)
    for t in triples:
        if t.object_type != "resource" or t.predicate == "a":
            continue
        obj_uri = resource_obj_uri(t.obj)
        if not (t.subject.startswith(DATA_NS) and obj_uri.startswith(DATA_NS)):
            continue
        relation_edges.append((t.subject, t.predicate, obj_uri))
        relation_counter[t.predicate] += 1
        adjacency[t.subject].add(obj_uri)
        adjacency[obj_uri].add(t.subject)
    connected = {node for edge in relation_edges for node in (edge[0], edge[2])}
    isolated = entity_subjects - connected
    visited = set()
    largest = 0
    components = 0
    for node in connected:
        if node in visited:
            continue
        components += 1
        size = 0
        q = deque([node])
        visited.add(node)
        while q:
            current = q.popleft()
            size += 1
            for nxt in adjacency.get(current, set()):
                if nxt in visited:
                    continue
                visited.add(nxt)
                q.append(nxt)
        largest = max(largest, size)
    degree_values = [len(adjacency.get(node, set())) for node in entity_subjects]
    top_relations = [{"relation": rel.replace(ZS_NS, "zs:"), "edge_count": count} for rel, count in relation_counter.most_common(30)]
    write_csv(OUT_DIR / "e6_relation_type_distribution.csv", top_relations, ["relation", "edge_count"])
    summary = {
        "entity_nodes": len(entity_subjects),
        "business_relation_edges": len(relation_edges),
        "business_relation_types": len(relation_counter),
        "connected_entity_nodes": len(connected),
        "isolated_entity_nodes": len(isolated),
        "isolated_entity_ratio": round(len(isolated) / len(entity_subjects), 6) if entity_subjects else 0,
        "connected_components": components,
        "largest_connected_component_nodes": largest,
        "average_degree": round(sum(degree_values) / len(degree_values), 6) if degree_values else 0,
        "max_degree": max(degree_values) if degree_values else 0,
    }
    write_json(OUT_DIR / "e6_graph_structure_summary.json", summary)
    return summary


def experiment_e7(triples: list[Triple], entity_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    relation_edges = [(t.subject, t.predicate, resource_obj_uri(t.obj)) for t in triples if t.object_type == "resource" and t.predicate.startswith(ZS_NS) and t.predicate != "a" and resource_obj_uri(t.obj).startswith(DATA_NS)]
    by_relation = defaultdict(list)
    outgoing = defaultdict(list)
    for source, pred, target in relation_edges:
        by_relation[pred].append((source, pred, target))
        outgoing[source].append((pred, target))
    relation_order = [ZS_NS + name for name in ["managedByPoliceOrg", "partOfAddressEntity", "parentPoliceOrg", "parentAdminArea", "locatedInAdminArea", "locatedOnStreet"]]
    rows = []
    available_relations = [pred for pred in relation_order if by_relation.get(pred)]
    per_relation = 100 // len(available_relations) if available_relations else 0
    remainder = 100 % len(available_relations) if available_relations else 0
    for relation_index, pred in enumerate(available_relations):
        samples = by_relation.get(pred, [])
        quota = per_relation + (1 if relation_index < remainder else 0)
        step = max(1, len(samples) // quota) if quota else 1
        for source, relation, target in samples[::step][:quota]:
            if len(rows) >= 100:
                break
            path = [(source, relation, target)]
            # Extend one more hop when possible to test Graph RAG-style context expansion.
            for rel2, target2 in outgoing.get(target, [])[:1]:
                path.append((target, rel2, target2))
                break
            source_meta = entity_index.get(source, {})
            target_meta = entity_index.get(target, {})
            rows.append({
                "test_id": f"KQ{len(rows)+1:03d}",
                "query_type": relation.replace(ZS_NS, ""),
                "start_entity_uri": source,
                "start_table": source_meta.get("table", ""),
                "start_entity_id": source_meta.get("entity_id", ""),
                "first_relation": relation.replace(ZS_NS, "zs:"),
                "first_target_uri": target,
                "first_target_table": target_meta.get("table", ""),
                "path_length": len(path),
                "path_relations": " -> ".join(edge[1].replace(ZS_NS, "zs:") for edge in path),
                "cypher_template": "MATCH p=(n)-[*1..2]-(m) WHERE n.uri=$start_uri RETURN p LIMIT 5",
                "explainability_status": "path_available_for_evidence_chain",
            })
        if len(rows) >= 100:
            break
    fields = ["test_id", "query_type", "start_entity_uri", "start_table", "start_entity_id", "first_relation", "first_target_uri", "first_target_table", "path_length", "path_relations", "cypher_template", "explainability_status"]
    write_csv(OUT_DIR / "e7_kg_explainability_testset_100.csv", rows, fields)
    counts = Counter(row["query_type"] for row in rows)
    summary = {"test_items": len(rows), "relation_type_coverage": len(counts), "query_type_counts": dict(counts), "note": "This validates path availability only, not Graph RAG answer generation quality."}
    write_json(OUT_DIR / "e7_kg_explainability_summary.json", summary)
    return summary


def render_report(summaries: dict[str, dict[str, Any]]) -> None:
    lines = [
        "# KG 建图有效性实验执行报告",
        "",
        "本报告按 `kg_construction_experiment_design.md` 中的 7 个 KG 建图有效性实验顺序执行。测试集条数按用户要求扩充到 100 条，尽量覆盖 17 张表、主要实体类型和当前关系规则中的主要关系类型。",
        "",
        "## 结果文件",
        "",
        "| 实验 | 结果文件 |",
        "|---|---|",
        "| E1 图谱规模与实体覆盖率 | `kg_construction_experiments/e1_graph_scale_summary.json`, `e1_entity_coverage_by_table.csv` |",
        "| E2 属性保真与字段值覆盖率 | `kg_construction_experiments/e2_property_fidelity_testset_100.csv`, `e2_property_fidelity_summary.json` |",
        "| E3 字段语义映射正确性预审 | `kg_construction_experiments/e3_semantic_mapping_review_testset_100.csv`, `e3_semantic_mapping_prereview_summary.json` |",
        "| E4 隐式关系规则生成有效性 | `kg_construction_experiments/e4_relation_rule_effectiveness.csv`, `e4_relation_rule_effectiveness_summary.json` |",
        "| E5 关系边有效性抽样核验 | `kg_construction_experiments/e5_relation_edge_validation_testset_100.csv`, `e5_relation_edge_validation_summary.json` |",
        "| E6 图谱结构一致性与连通性 | `kg_construction_experiments/e6_graph_structure_summary.json`, `e6_relation_type_distribution.csv` |",
        "| E7 KG 查询可解释性与 Graph RAG 基础 | `kg_construction_experiments/e7_kg_explainability_testset_100.csv`, `e7_kg_explainability_summary.json` |",
        "",
        "## 汇总结果",
        "",
    ]
    for name, summary in summaries.items():
        lines.append(f"### {name}")
        lines.append("")
        for key, value in summary.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            lines.append(f"- `{key}`: {value}")
        lines.append("")
    lines.extend([
        "## 写作边界",
        "",
        "- E3 为自动预审测试集，`auto_review_status` 不是最终人工语义准确率。论文中只能写为待人工复核的预审结果。",
        "- E5 通过源字段值和目标键值核验关系边，能够证明抽样边具有数据支撑，但仍不等同于全图语义完全正确。",
        "- E7 仅验证 KG 路径和证据链可用性，不评价 Graph RAG 问答生成效果。",
        "- SQL 与 KG 查询对比仍应沿用已有结论：KG 不全局优于 SQL，仅在部分多跳聚合和小结果集路径查询中表现出局部优势。",
    ])
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ensure_out_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rules = load_rules()
    triples = parse_ttl(TTL_PATH)
    by_subject, literal_values = ttl_indexes(triples)
    entity_index = build_entity_uri_index(conn, rules)
    summaries = {
        "E1 图谱规模与实体覆盖率": experiment_e1(conn, triples, entity_index),
        "E2 属性保真与字段值覆盖率": experiment_e2(conn, rules, literal_values),
        "E3 字段语义映射正确性预审": experiment_e3(),
        "E4 隐式关系规则生成有效性": experiment_e4(),
        "E5 关系边有效性抽样核验": experiment_e5(triples, rules, entity_index),
        "E6 图谱结构一致性与连通性": experiment_e6(triples),
        "E7 KG 查询可解释性与 Graph RAG 基础": experiment_e7(triples, entity_index),
    }
    write_json(OUT_DIR / "kg_construction_experiment_summaries.json", summaries)
    render_report(summaries)
    print(REPORT_PATH)


if __name__ == "__main__":
    main()
