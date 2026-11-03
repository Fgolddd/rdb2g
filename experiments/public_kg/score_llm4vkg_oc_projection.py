#!/usr/bin/env python3
"""Score LLM4VKG ontology-completion field projections against the frozen reference.

LLM4VKG's OC stage emits a source-column -> ontology-entity projection before
R2RML MappingGeneration. For source schemas with no recognized structural
patterns, MappingGeneration can emit an empty graph. This scorer measures only
the OC column projection and does not claim that R2RML or RDF materialization
completed.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from pathlib import Path
from typing import Any

from rdflib import Graph

from experiments.public_kg.artifacts import file_hash, write_json
from experiments.public_kg.machine_diagnostics import (
    _validate_reference_provenance,
    mapping_diagnostics,
)

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK = ROOT / "data/public/benchmark_v2"


def _column_inventory(db_path: Path) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    with sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True) as conn:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            fields.extend((table, row[1]) for row in conn.execute(f"PRAGMA table_info({quoted})"))
    return fields


def score_attempt(attempt_path: Path, dataset: str) -> dict[str, Any]:
    attempt_path = Path(attempt_path).resolve()
    attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
    run_dir = Path(attempt["run_directory"]).resolve()
    generation = attempt.get("generation")
    if not isinstance(generation, dict) or generation.get("status") != "ontology_completion_and_mapping_generation_completed":
        raise ValueError(f"{attempt_path}: completed LLM4VKG OC/MG generation is required")

    db_path = BENCHMARK / dataset / "sample.sqlite"
    ontology_path = BENCHMARK / "ontology/target_terms.json"
    reference_dir = BENCHMARK / dataset / "machine_review_v2"
    reference_path = reference_dir / "mapping_reference.csv"
    relation_reference = reference_dir / "relation_reference.json"
    reference_manifest = reference_dir / "manifest.json"
    if not db_path.is_file() or not ontology_path.is_file():
        raise FileNotFoundError(f"frozen benchmark inputs are missing for {dataset}")
    if attempt.get("dataset") != dataset:
        raise ValueError(f"attempt dataset {attempt.get('dataset')!r} does not match {dataset!r}")
    _validate_reference_provenance(reference_manifest, "second", dataset, db_path, ontology_path,
                                   reference_path, relation_reference)

    terms = json.loads(ontology_path.read_text(encoding="utf-8"))["terms"]
    property_uris = {term["uri"] for term in terms if term.get("type") == "Property"}
    raw_projection = generation.get("predicted_source_concepts", {})
    mapping_name = generation.get("outputs", {}).get("mapping_ttl", "__missing__")
    r2rml_mapping_path = run_dir / mapping_name
    r2rml_triple_count = len(Graph().parse(r2rml_mapping_path, format="turtle")) if r2rml_mapping_path.is_file() else None
    fields = _column_inventory(db_path)
    mappings = []
    missing_source_keys = []
    non_vocabulary_predictions = 0
    for table, column in fields:
        source_key = f"{table}$$$${column}"
        prediction = raw_projection.get(source_key)
        if prediction is None:
            missing_source_keys.append(source_key)
            target_uri = None
        else:
            candidate_uri = prediction.get("target_uri")
            if candidate_uri in property_uris:
                target_uri = candidate_uri
            else:
                target_uri = None
                if candidate_uri is not None:
                    non_vocabulary_predictions += 1
        mappings.append({"table": table, "column": column, "target_uri": target_uri})

    output_dir = run_dir / "llm4vkg_oc_projection_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    prediction = {
        "method": "LLM4VKG OC column projection",
        "run_status": "success",
        "table_mappings": [],
        "mappings": mappings,
        "relation_rules": [],
        "normalization": {
            "policy": "Use only LLM4VKG OC's source-column target_uri when it exactly matches a fixed v2 datatype-property URI; no label matching or reference-guided repair.",
            "projection_source": "generation.predicted_source_concepts[table$$$$column]",
            "r2rml_mapping_generation_completed": generation.get("status") == "ontology_completion_and_mapping_generation_completed",
            "r2rml_mapping_graph_triple_count": r2rml_triple_count,
        },
    }
    predictions_path = output_dir / "predictions.json"
    write_json(predictions_path, prediction)
    manifest = {
        "method": "LLM4VKG OC column projection",
        "dataset": dataset,
        "status": "oc_field_projection_ready_for_machine_reference_diagnostic",
        "human_gold": False,
        "source_database": str(db_path.resolve()),
        "dataset_sha256": file_hash(db_path),
        "target_vocabulary": str(ontology_path.resolve()),
        "ontology_sha256": file_hash(ontology_path),
        "generation_record": str((run_dir / f"llm4vkg-oc-mg_{dataset}.json").resolve()),
        "generation_record_sha256": file_hash(run_dir / f"llm4vkg-oc-mg_{dataset}.json"),
        "machine_reference_manifest_sha256": file_hash(reference_manifest),
        "machine_mapping_reference_sha256": file_hash(reference_path),
        "scoring_script_sha256": file_hash(Path(__file__)),
        "prediction_sha256": file_hash(predictions_path),
        "field_count": len(fields),
        "mapped_field_count": sum(item["target_uri"] is not None for item in mappings),
        "unmapped_field_count": sum(item["target_uri"] is None for item in mappings),
        "missing_source_prediction_count": len(missing_source_keys),
        "out_of_vocabulary_prediction_count": non_vocabulary_predictions,
        "normalization_basis": prediction["normalization"]["policy"],
        "r2rml_mapping_output": "The OC projection is scored independently of R2RML MappingGeneration; this scorer does not claim graph materialization.",
        "reference_independence": "The second machine reference is not read during generation or normalization; it is used only by the separate diagnostic scorer.",
    }
    write_json(output_dir / "manifest.json", manifest)
    diagnostic = {
        "status": "machine_reference_diagnostic_only",
        "human_gold": False,
        "dataset": dataset,
        "method": "LLM4VKG OC column projection",
        "reference_version": "second",
        "reference_manifest_sha256": file_hash(reference_manifest),
        "mapping_reference_sha256": file_hash(reference_path),
        "prediction_sha256": file_hash(predictions_path),
        "scoring_scope": "Exact URI field projection from the LLM4VKG ontology-completion stage; R2RML output is excluded.",
        "mapping": mapping_diagnostics(prediction, reference_path),
    }
    write_json(output_dir / "machine_diagnostics.json", diagnostic)
    result = {
        "dataset": dataset,
        "repeat": attempt.get("repeat"),
        "attempt_path": str(attempt_path),
        "projection_directory": str(output_dir),
        "generation_status": generation.get("status"),
        "r2rml_mapping_patterns": generation.get("mapping_patterns"),
        "r2rml_mapping_file_bytes": Path(run_dir / generation.get("outputs", {}).get("mapping_ttl", "__missing__")).stat().st_size
        if Path(run_dir / generation.get("outputs", {}).get("mapping_ttl", "__missing__")).is_file() else None,
        "r2rml_mapping_graph_triple_count": r2rml_triple_count,
        "request_summary": attempt.get("request_summary"),
        "score": diagnostic["mapping"],
    }
    write_json(output_dir / "score.json", result)
    return result


def _metric_summary(values: list[float | None]) -> dict[str, float | int | None]:
    available = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return {
        "n": len(available),
        "mean": statistics.fmean(available) if available else None,
        "sample_stddev": statistics.stdev(available) if len(available) > 1 else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("foshan_osm", "geonames_cn"), required=True)
    parser.add_argument("--attempt", type=Path, action="append", required=True,
                        help="Attempt JSON; provide one argument for each independent repeat.")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    results = [score_attempt(path, args.dataset) for path in args.attempt]
    metrics = {name: _metric_summary([item["score"].get(name) for item in results])
               for name in ("accuracy", "precision", "recall", "f1")}
    report = {
        "experiment": "LLM4VKG OC-stage field projection diagnostic",
        "dataset": args.dataset,
        "scored_repeat_count": len(results),
        "score_reference": "Second machine-reviewed mapping reference; not human gold.",
        "aggregation": "Mean and sample standard deviation (ddof=1) over scored independent attempts.",
        "metrics_mean_and_sample_stddev": metrics,
        "attempts": results,
        "limitation": "LLM4VKG's R2RML MappingGeneration is not scored here. An empty R2RML mapping graph means the method did not emit graph mappings for this schema, even when OC field projections are available.",
    }
    output = args.out or (Path(results[0]["projection_directory"]).parents[1] / "oc_projection_summary.json")
    write_json(output, report)
    print(json.dumps({"out": str(output.resolve()), "metrics": metrics}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
