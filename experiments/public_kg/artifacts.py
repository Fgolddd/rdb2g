"""Shared materialization and review gates for public experiment predictions."""

import csv
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import XSD

from experiments.public_kg.methods import (
    _validate_rule, evaluate_expression, schema_profile,
)


AUDIT = Namespace("https://w3id.org/rdb2g/audit/")


def file_hash(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def materialize(db_path, prediction_path, ontology_path, out_dir, max_edges=200000):
    """Emit only model-selected semantics and explicit audit provenance.

    Row IRIs preserve source records. Identity merging is not assumed; identity
    relationships must be predicted. No labels or edges from gold are read.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    predictions = json.loads(Path(prediction_path).read_text())
    if predictions.get("run_status") != "success":
        return {"status": "not_materialized", "reason": "inference_not_successful"}
    vocabulary = json.loads(Path(ontology_path).read_text())
    terms = vocabulary if isinstance(vocabulary, list) else vocabulary["terms"]
    terms = {term["uri"]: term for term in terms}
    profile = schema_profile(db_path)
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    graph = Graph()
    graph.bind("audit", AUDIT)
    graph.bind("spatial", Namespace("https://w3id.org/rdb2g/public-spatial/"))
    dataset = Path(db_path).parent.name
    classes = {item["table"]: item["target_uri"] for item in predictions["table_mappings"]}
    mappings = defaultdict(list)
    for item in predictions["mappings"]:
        if item.get("target_uri"):
            mappings[item["table"]].append(item)
    rows_by_table = {}
    issues, triples = [], 0
    started = time.perf_counter()
    try:
        for table in profile:
            rows = [dict(row) for row in conn.execute(f"SELECT * FROM {quote_identifier(table)} ORDER BY rowid")]
            entries = []
            key_candidates = [c["name"] for c in profile[table]["columns"] if c.get("primary_key")]
            for offset, row in enumerate(rows, 1):
                if len(key_candidates) == 1 and row[key_candidates[0]] is not None:
                    key = str(row[key_candidates[0]])
                    key_kind = "pk"
                else:
                    key = str(offset)
                    key_kind = "row"
                node = URIRef("https://w3id.org/rdb2g/record/" + "/".join(
                    quote(value, safe="") for value in (dataset, table, key_kind, key)))
                entries.append((node, row, key))
                graph.add((node, AUDIT.sourceTable, Literal(table)))
                graph.add((node, AUDIT.sourceKey, Literal(key)))
                if classes.get(table):
                    graph.add((node, RDF.type, URIRef(classes[table])))
                for item in mappings[table]:
                    value = row[item["column"]]
                    if value is None:
                        continue
                    term = terms[item["target_uri"]]
                    if term.get("range") != "Literal":
                        issues.append({"type": "object_property_assigned_to_literal_column",
                                       "table": table, "column": item["column"], "uri": item["target_uri"]})
                        continue
                    # rdflib treats Python bytes as their repr (for example
                    # ``b'\\x01'``), which silently changes binary geometry and
                    # other BLOB values. Emit the original bytes in the standard
                    # XML Schema hexBinary lexical form instead.
                    literal = (Literal(value.hex(), datatype=XSD.hexBinary)
                               if isinstance(value, bytes) else Literal(value))
                    graph.add((node, URIRef(item["target_uri"]), literal))
                    triples += 1
            rows_by_table[table] = entries
        edges = []
        for rule_id, rule in enumerate(predictions["relation_rules"]):
            _validate_rule(rule, profile, terms)
            targets = defaultdict(list)
            for node, row, key in rows_by_table[rule["target_table"]]:
                value = evaluate_expression(rule["target_expr"], row)
                if value is not None:
                    targets[value].append((node, key))
            for source_node, row, source_key in rows_by_table[rule["source_table"]]:
                value = evaluate_expression(rule["source_expr"], row)
                if value is None:
                    continue
                for target_node, target_key in targets.get(value, []):
                    if len(edges) >= max_edges:
                        raise ValueError("materialization edge budget exceeded")
                    head, tail = source_node, target_node
                    if rule.get("direction", "forward") == "reverse":
                        head, tail = tail, head
                    graph.add((head, URIRef(rule["predicate"]), tail))
                    edges.append({"rule_id": rule_id, "head": str(head), "predicate": rule["predicate"],
                                  "tail": str(tail), "source_table": rule["source_table"],
                                  "source_key": source_key, "target_table": rule["target_table"],
                                  "target_key": target_key})
        graph.serialize(out_dir / "graph.ttl", format="turtle")
        with (out_dir / "edge_evidence.jsonl").open("w", encoding="utf-8") as stream:
            for edge in edges:
                stream.write(json.dumps(edge, ensure_ascii=False) + "\n")
        metrics = {"status": "success", "source_rows": sum(len(v) for v in rows_by_table.values()),
                   "mapped_columns": sum(bool(item.get("target_uri")) for item in predictions["mappings"]),
                   "total_columns": len(predictions["mappings"]), "rules": len(predictions["relation_rules"]),
                   "edge_evidence_rows": len(edges), "unique_relation_edges": len({(e["head"], e["predicate"], e["tail"]) for e in edges}),
                   "rdf_triples": len(graph), "literal_value_emissions": triples,
                   "representation": "source-record IRIs; no implicit same-entity merge",
                   "issues": list({json.dumps(issue, sort_keys=True): issue for issue in issues}.values()),
                   "elapsed_seconds": time.perf_counter() - started,
                   "semantic_quality": "not_scored_pending_independent_human_gold"}
        write_json(out_dir / "structural_metrics.json", metrics)
        return metrics
    finally:
        conn.close()


def export_review(db_path, prediction_path, ontology_path, out_dir):
    """Export editable suggestions, with every approval field explicitly pending."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    data = json.loads(Path(prediction_path).read_text())
    profile = schema_profile(db_path)
    by_field = {(item["table"], item["column"]): item.get("target_uri") for item in data["mappings"]}
    with (out_dir / "mapping_suggestions.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        fields = ["table", "column", "type", "samples", "suggested_uri", "accepted_uris_json",
                  "no_match", "review_status", "reviewer", "evidence", "prediction_origin"]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for table, table_profile in profile.items():
            for col in table_profile["columns"]:
                writer.writerow({"table": table, "column": col["name"], "type": col["type"],
                                 "samples": json.dumps(col["samples"], ensure_ascii=False),
                                 "suggested_uri": by_field.get((table, col["name"])),
                                 "accepted_uris_json": "", "no_match": "", "review_status": "pending",
                                 "reviewer": "", "evidence": "", "prediction_origin": data["method"]})
    rules = [{"suggested_rule": rule, "review_status": "pending", "approved_rule": None,
              "reviewer": "", "evidence": "", "prediction_origin": data["method"]}
             for rule in data["relation_rules"]]
    write_json(out_dir / "relation_suggestions.json", rules)
    write_json(out_dir / "review_manifest.json", {
        "status": "model_suggestions_not_gold", "independence_warning":
        "Review against source documentation independently; suggestions must not define the gold universe.",
        "source_database_sha256": file_hash(db_path), "prediction_sha256": file_hash(prediction_path),
        "ontology_sha256": file_hash(ontology_path), "mapping_tasks": len(by_field), "suggested_rules": len(rules)})


def score_mapping(predictions, gold_path):
    """Fail closed unless every evaluation label is independently reviewed."""
    path = Path(gold_path)
    if not path.exists():
        return {"status": "pending_human_gold", "accuracy": None, "f1": None}
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or any(row.get("review_status") != "approved" or not row.get("reviewer", "").strip()
                       or not row.get("evidence", "").strip() for row in rows):
        return {"status": "pending_human_gold", "accuracy": None, "f1": None}
    predicted = {(item["table"], item["column"]): item.get("target_uri") for item in predictions["mappings"]}
    seen, correct, tp, fp, fn = set(), 0, 0, 0, 0
    for row in rows:
        key = row["table"], row["column"]
        if key in seen or key not in predicted:
            raise ValueError("duplicate or unknown gold item")
        seen.add(key)
        no_match_value = row.get("no_match", "").strip().casefold()
        if no_match_value not in {"", "true", "1", "yes", "false", "0", "no"}:
            raise ValueError("no_match must be a boolean value")
        no_match = no_match_value in {"true", "1", "yes"}
        accepted = json.loads(row["accepted_uris_json"] or "[]")
        if not isinstance(accepted, list) or not all(
                isinstance(value, str) and value and value.strip() == value for value in accepted):
            raise ValueError("accepted_uris_json must be a list of nonempty URIs")
        if no_match == bool(accepted):
            raise ValueError("gold must have accepted URIs OR no_match")
        uri = predicted[key]
        match = (uri is None) if no_match else uri in accepted
        correct += int(match)
        tp += int(not no_match and match)
        fp += int(uri is not None and not match)
        fn += int(not no_match and not match)
    return {"status": "scored_reviewed_subset", "items": len(rows),
            "accuracy": correct / len(rows), "precision": tp / (tp + fp) if tp + fp else 0,
            "recall": tp / (tp + fn) if tp + fn else 0,
            "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0}
