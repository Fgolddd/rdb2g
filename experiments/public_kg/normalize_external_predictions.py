#!/usr/bin/env python3
"""Normalize external field matcher outputs to the v2 complete-field format.

The candidate ranking from each source result is retained. The selected mapping
is the top ranked DatatypeProperty, with no confidence threshold; unmatched or
ambiguous fields remain explicit nulls. This is a schema adapter, not a scorer.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[2]


def _fields(db: Path) -> list[tuple[str, str]]:
    with sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True) as conn:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        result = []
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            result.extend((table, row[1]) for row in conn.execute(f"PRAGMA table_info({quoted})"))
    return result


def _source_field(source: str, fields: list[tuple[str, str]]) -> tuple[str, str] | None:
    matches = [(table, column) for table, column in fields
               if source == f"{table}.{column}"]
    if len(matches) == 1:
        return matches[0]
    # Some prediction APIs return only a bare column name. Use it only when
    # that name is unique in this database; never guess among duplicate names.
    matches = [(table, column) for table, column in fields if source == column]
    return matches[0] if len(matches) == 1 else None


def normalize(raw_path: Path, database: Path, terms_path: Path,
              output_dir: Path, method: str, target_scope: str = "datatype_only") -> dict:
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    terms_obj = json.loads(terms_path.read_text(encoding="utf-8"))
    terms = terms_obj.get("terms", terms_obj.get("target_terms", []))
    if target_scope == "datatype_only":
        allowed = {term["uri"] for term in terms
                   if term.get("type") == "Property" and term.get("property_kind") == "DatatypeProperty"}
    elif target_scope == "all_terms":
        allowed = {term["uri"] for term in terms}
    else:
        raise ValueError(f"Unknown target scope: {target_scope}")

    fields = _fields(database)
    grouped: dict[tuple[str, str], list[dict]] = {}
    unassigned = []
    for rank, item in enumerate(raw.get("predictions", []), start=1):
        key = _source_field(str(item.get("source", "")), fields)
        if key is None:
            unassigned.append({"source": item.get("source"), "reason": "source field is absent or ambiguous"})
            continue
        uri = item.get("target_uri")
        if uri not in allowed:
            unassigned.append({"source": item.get("source"), "target_uri": uri,
                               "reason": "target excluded by field-matching scope"})
            continue
        candidate = {"rank": rank, "target_uri": uri}
        if isinstance(item.get("score"), (int, float)):
            candidate["score"] = float(item["score"])
        if item.get("reasoning"):
            candidate["reasoning"] = item["reasoning"]
        grouped.setdefault(key, []).append(candidate)

    mappings = []
    for table, column in fields:
        candidates = grouped.get((table, column), [])
        # Magneto emits an ordered candidate list. LLMatch emits target matches
        # without confidence; preserve input order as the only available rank.
        chosen = candidates[0] if candidates else None
        mappings.append({"table": table, "column": column,
                         "target_uri": chosen["target_uri"] if chosen else None,
                         "selection_policy": "top1_no_threshold",
                         "candidates": candidates})

    output_dir.mkdir(parents=True, exist_ok=True)
    prediction = {"run_status": "success", "table_mappings": [], "mappings": mappings,
                  "relation_rules": []}
    (output_dir / "predictions.json").write_text(
        json.dumps(prediction, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with Path(path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()
    run_manifest = {
        "method": method,
        "dataset": database.parent.name,
        "dataset_sha256": file_hash(database),
        "ontology_sha256": file_hash(terms_path),
        "prediction_sha256": file_hash(output_dir / "predictions.json"),
        "target_scope": target_scope,
        "target_term_count": len(allowed),
        "field_count": len(fields),
        "mapped_field_count": sum(item["target_uri"] is not None for item in mappings),
        "unmatched_field_count": sum(item["target_uri"] is None for item in mappings),
        "unassigned_prediction_count": len(unassigned),
        "selection_policy": {"rule": "top1_no_threshold", "candidate_order": "upstream order",
                             "score_threshold": None,
                             "reason": "No independent calibration set is available."},
        "full_pipeline_reproduction": False,
        "quality_scored": False,
        "human_gold": False,
        "source_result": str(raw_path),
        "source_unassigned_predictions": unassigned,
        "target_vocabulary": str(terms_path),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(run_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return run_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--terms", type=Path, default=ROOT / "data/public/benchmark_v2/ontology/target_terms.json")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--method", required=True)
    parser.add_argument("--target-scope", choices=["datatype_only", "all_terms"], default="datatype_only")
    args = parser.parse_args()
    result = normalize(args.raw.resolve(), args.db.resolve(), args.terms.resolve(),
                       args.out_dir.resolve(), args.method, args.target_scope)
    print(json.dumps({key: result[key] for key in
                      ("method", "dataset", "field_count", "mapped_field_count",
                       "unmatched_field_count", "unassigned_prediction_count", "target_term_count")},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
