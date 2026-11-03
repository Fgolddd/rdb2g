"""Diagnostics against evidence-linked machine references, never human-gold scores."""

import argparse
import csv
import json
import sqlite3
from pathlib import Path

from experiments.public_kg.artifacts import file_hash, materialize, write_json


def _validate_reference_provenance(manifest_path, version, dataset, db, ontology,
                                   mapping_reference, relation_reference):
    """Require second-pass provenance and verify every frozen input hash."""
    manifest_path = Path(manifest_path)
    if not manifest_path.is_file():
        if version == "second":
            raise FileNotFoundError(f"second machine-reference manifest is missing: {manifest_path}")
        return None
    provenance = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(provenance, dict):
        raise ValueError("machine-reference manifest must be a JSON object")
    if version == "second":
        if provenance.get("status") != "second_machine_reviewed_reference":
            raise ValueError("second machine-reference manifest has an unexpected status")
        if provenance.get("human_reviewed") is not False:
            raise ValueError("second machine-reference provenance must declare human_reviewed=false")
        if provenance.get("dataset") != dataset:
            raise ValueError("machine-reference dataset does not match run dataset")
        database_hash_key = "source_database_sha256"
        required_file_hashes = True
    else:
        database_hash_key = ("database_sha256" if "database_sha256" in provenance
                             else "source_database_sha256")
        required_file_hashes = False

    expected_database_hash = provenance.get(database_hash_key)
    expected_ontology_hash = provenance.get("ontology_sha256")
    if not isinstance(expected_database_hash, str) or not expected_database_hash:
        raise ValueError(f"machine-reference manifest requires {database_hash_key}")
    if not isinstance(expected_ontology_hash, str) or not expected_ontology_hash:
        raise ValueError("machine-reference manifest requires ontology_sha256")
    if expected_database_hash != file_hash(db):
        raise ValueError("machine-reference database hash differs from diagnostic database")
    if expected_ontology_hash != file_hash(ontology):
        raise ValueError("machine-reference ontology hash differs from diagnostic ontology")

    for field, path in (("mapping_reference_sha256", mapping_reference),
                        ("relation_reference_sha256", relation_reference)):
        expected = provenance.get(field)
        if not isinstance(expected, str) or not expected:
            if required_file_hashes:
                raise ValueError(f"second machine-reference manifest requires {field}")
            continue
        if expected != file_hash(path):
            raise ValueError(f"machine-reference {field} does not match reference file")
    return provenance


def mapping_diagnostics(prediction, reference_path):
    if prediction.get("run_status") != "success":
        return {"status": "inference_not_successful", "accuracy": None, "f1": None}
    with Path(reference_path).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    predicted = {}
    for item in prediction["mappings"]:
        key = item["table"], item["column"]
        if key in predicted:
            raise ValueError("duplicate prediction field")
        predicted[key] = item.get("target_uri")
    selected, seen = [], set()
    for row in rows:
        key = row["table"], row["column"]
        if key in seen:
            raise ValueError("duplicate reference field")
        seen.add(key)
        if row.get("include_in_diagnostic", "").casefold() != "true":
            continue
        if row.get("review_status") != "machine_reviewed" or not row.get("evidence", "").strip():
            raise ValueError("selected reference requires machine review and evidence")
        if key not in predicted:
            raise ValueError("prediction is not expanded to the complete field universe")
        accepted = json.loads(row["accepted_uris_json"])
        if not isinstance(accepted, list) or not all(isinstance(uri, str) and uri.strip() for uri in accepted):
            raise ValueError("invalid accepted URI list")
        no_match = row.get("no_match", "").casefold()
        if no_match not in {"true", "false"} or (no_match == "true") == bool(accepted):
            raise ValueError("reference must explicitly declare match or no-match")
        uri = predicted[key]
        correct = uri is None if no_match == "true" else uri in accepted
        selected.append({"table": key[0], "column": key[1], "predicted_uri": uri,
                         "accepted_uris": accepted, "correct": correct,
                         "positive": no_match != "true"})
    if not selected:
        return {"status": "no_assessable_machine_reference", "accuracy": None, "f1": None}
    tp = sum(item["positive"] and item["correct"] for item in selected)
    fp = sum(item["predicted_uri"] is not None and not item["correct"] for item in selected)
    fn = sum(item["positive"] and not item["correct"] for item in selected)
    return {"status": "machine_reference_diagnostic_only", "human_gold": False,
            "items": len(selected), "excluded_items": len(rows) - len(selected),
            "accuracy": sum(item["correct"] for item in selected) / len(selected),
            "precision": tp / (tp + fp) if tp + fp else 0.0,
            "recall": tp / (tp + fn) if tp + fn else 0.0,
            "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0,
            "field_decisions": selected}


def build_relation_reference(db, ontology, reference_path, output):
    raw = json.loads(Path(reference_path).read_text())
    if raw.get("status") != "machine_reviewed_reference" or raw.get("human_reviewed") is not False:
        raise ValueError("explicit machine-reference provenance required")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    source = output / "reference_predictions.json"
    rules = raw["rules"]
    write_json(source, {"run_status": "success", "table_mappings": [], "mappings": [], "relation_rules": rules})
    metrics = materialize(db, source, ontology, output / "graph")
    raw_edge_path = output / "graph/edge_evidence.jsonl"
    excluded_edges = []
    retained_edges = []
    exclusion_counts = [0] * len(raw.get("edge_exclusions", []))
    conn = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True)
    primary_keys = {}
    value_cache = {}

    def column_value(table, row_key, column):
        cache_key = table, row_key, column
        if cache_key not in value_cache:
            if table not in primary_keys:
                info = conn.execute(f'PRAGMA table_info("{table.replace(chr(34), chr(34)*2)}")').fetchall()
                keys = [item[1] for item in info if item[5]]
                if len(keys) != 1:
                    raise ValueError(f"exclusion lookup needs one primary key in {table}")
                primary_keys[table] = keys[0]
            pk = primary_keys[table].replace('"', '""')
            col = column.replace('"', '""')
            row = conn.execute(f'SELECT "{col}" FROM "{table.replace(chr(34), chr(34)*2)}" WHERE "{pk}" = ?',
                               (row_key,)).fetchone()
            value_cache[cache_key] = row[0] if row else None
        return value_cache[cache_key]

    for line in raw_edge_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        edge = json.loads(line)
        excluded = False
        for index, spec in enumerate(raw.get("edge_exclusions", [])):
            if spec.get("rule_index") != edge.get("rule_id"):
                continue
            excluded_by_condition = (
                spec.get("condition") == "source_key_equals_target_key"
                and edge.get("source_key") == edge.get("target_key"))
            if spec.get("condition") == "source_column_equals_target_column":
                source_value = column_value(edge["source_table"], edge["source_key"], spec["source_column"])
                target_value = column_value(edge["target_table"], edge["target_key"], spec["target_column"])
                excluded_by_condition = source_value is not None and source_value == target_value
            if excluded_by_condition:
                excluded_edges.append(edge)
                exclusion_counts[index] += 1
                excluded = True
                break
        if not excluded:
            retained_edges.append(edge)
    conn.close()
    for spec, count in zip(raw.get("edge_exclusions", []), exclusion_counts):
        expected = spec.get("count_in_frozen_sample")
        if expected is not None and count != expected:
            raise ValueError(f"reference exclusion count mismatch: expected {expected}, got {count}")
    reference_edge_path = output / "reference_edge_evidence.jsonl"
    excluded_edge_path = output / "excluded_edge_evidence.jsonl"
    reference_edge_path.write_text("".join(json.dumps(edge, ensure_ascii=False) + "\n" for edge in retained_edges),
                                   encoding="utf-8")
    excluded_edge_path.write_text("".join(json.dumps(edge, ensure_ascii=False) + "\n" for edge in excluded_edges),
                                  encoding="utf-8")
    write_json(output / "manifest.json", {"status": "machine_reference_diagnostic_only",
        "human_gold": False, "database_sha256": file_hash(db), "ontology_sha256": file_hash(ontology),
        "reference_sha256": file_hash(reference_path), "graph": metrics,
        "unfiltered_edge_evidence_sha256": file_hash(raw_edge_path),
        "reference_edge_evidence_sha256": file_hash(reference_edge_path),
        "excluded_edge_evidence_sha256": file_hash(excluded_edge_path),
        "excluded_edge_count": len(excluded_edges), "retained_reference_edge_count": len(retained_edges),
        "exclusions": raw.get("edge_exclusions", [])})
    return reference_edge_path


def edge_diagnostics(predicted_path, reference_path, rules, excluded_path=None):
    def load(path):
        with Path(path).open() as stream:
            return [json.loads(line) for line in stream if line.strip()]

    def normalized(edge):
        head, tail = edge["head"], edge["tail"]
        if edge["predicate"].endswith("/sameSourceEntity"):
            head, tail = sorted((head, tail))
        return head, edge["predicate"], tail

    def exclusion_key(edge):
        # An excluded relation is unknown for this endpoint pair, so neither
        # orientation should turn into a counted false positive.
        return tuple(sorted((edge["head"], edge["tail"]))) + (edge["predicate"],)

    def pair(source, target, predicate):
        # An assessed table pair covers both orientations: reversing a directed
        # predicate is an error, not an excuse to move it outside the scope.
        return (*sorted((source, target)), predicate)

    scopes = set()
    for rule in rules:
        source, target = rule["source_table"], rule["target_table"]
        if rule.get("direction", "forward") == "reverse":
            source, target = target, source
        scopes.add(pair(source, target, rule["predicate"]))
    reference = {normalized(edge) for edge in load(reference_path)}
    excluded = {exclusion_key(edge) for edge in load(excluded_path)} if excluded_path else set()
    assessed, unassessed, excluded_predictions = set(), set(), set()
    for edge in load(predicted_path):
        # Resolve orientation from stable record IRIs, not a join's source/target labels.
        source, target = edge["head"].split("/")[-3], edge["tail"].split("/")[-3]
        normalized_edge = normalized(edge)
        if pair(source, target, edge["predicate"]) in scopes and exclusion_key(edge) in excluded:
            excluded_predictions.add(exclusion_key(edge))
            continue
        target_set = assessed if pair(source, target, edge["predicate"]) in scopes else unassessed
        target_set.add(normalized_edge)
    tp, fp, fn = len(assessed & reference), len(assessed - reference), len(reference - assessed)
    return {"status": "machine_reference_diagnostic_only", "human_gold": False,
            "scope": "declared table-pair and predicate only; both directions assessed; not global relation recall",
            "reference_edges": len(reference), "assessed_predictions": len(assessed),
            "unassessed_predictions": len(unassessed), "tp": tp, "fp": fp, "fn": fn,
            "excluded_reference_edges": len(excluded),
            "excluded_predictions": len(excluded_predictions),
            "precision": tp / (tp + fp) if tp + fp else 0.0,
            "recall": tp / (tp + fn) if tp + fn else 0.0,
            "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--benchmark-dir", type=Path, default=Path("data/public/benchmark_v2"))
    parser.add_argument("--reference-version", choices=("original", "second"), default="second",
                        help="Machine-reference version to use for diagnostics.")
    args = parser.parse_args()
    manifest = json.loads((args.run / "manifest.json").read_text())
    dataset = manifest["dataset"]
    db = args.benchmark_dir / dataset / "sample.sqlite"
    ontology = args.benchmark_dir / "ontology/target_terms.json"
    if manifest["dataset_sha256"] != file_hash(db) or manifest["ontology_sha256"] != file_hash(ontology):
        raise ValueError("run and diagnostic data/ontology hashes differ")
    if args.reference_version == "second":
        folder = args.benchmark_dir / dataset / "machine_review_v2"
    else:
        folder = args.benchmark_dir / dataset / "machine_review"
    mapping_reference = folder / "mapping_reference.csv"
    relation_reference = folder / "relation_reference.json"
    if not mapping_reference.exists() and args.reference_version == "original":
        mapping_reference = args.benchmark_dir / dataset / "annotation/machine_mapping_reference.csv"
        relation_reference = args.benchmark_dir / dataset / "annotation/machine_relation_reference.json"
    if not mapping_reference.exists() or not relation_reference.exists():
        raise FileNotFoundError(f"machine reference version not found: {folder}")
    provenance_path = folder / "manifest.json"
    provenance = _validate_reference_provenance(
        provenance_path, args.reference_version, dataset, db, ontology,
        mapping_reference, relation_reference)
    prediction = json.loads((args.run / "predictions.json").read_text())
    result = {"status": "machine_reference_diagnostic_only", "human_gold": False,
              "dataset": dataset, "method": manifest["method"],
              "prediction_sha256": file_hash(args.run / "predictions.json"),
              "mapping_reference_sha256": file_hash(mapping_reference),
              "relation_reference_sha256": file_hash(relation_reference),
              "reference_provenance_sha256": file_hash(provenance_path) if provenance is not None else None,
              "mapping": mapping_diagnostics(prediction, mapping_reference)}
    if prediction["run_status"] == "success" and (args.run / "graph/edge_evidence.jsonl").exists():
        reference_edges = build_relation_reference(db, ontology, relation_reference,
            args.run / "machine_reference")
        excluded_edges = reference_edges.parent / "excluded_edge_evidence.jsonl"
        result["relations"] = edge_diagnostics(args.run / "graph/edge_evidence.jsonl",
            reference_edges, json.loads(relation_reference.read_text())["rules"],
            excluded_edges if excluded_edges.is_file() else None)
    elif prediction["run_status"] == "success":
        result["relations"] = {"status": "not_applicable_field_matching_component"}
    write_json(args.run / "machine_diagnostics.json", result)
    print(json.dumps({key: value for key, value in result.items() if key != "mapping"}))


if __name__ == "__main__":
    main()
