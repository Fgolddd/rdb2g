"""Run transparent relation-only component baselines on the public KG datasets.

These are simple baselines, not reproductions of Rel2Graph, UrbanKGent,
Schemora, or any other published system. They hold each dataset's Full r1
table classes fixed and emit no field mappings, so they isolate relation
candidate generation and materialization.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

from rdflib import Graph, RDF, RDFS, URIRef

from experiments.public_kg.artifacts import file_hash, materialize, quote_identifier, write_json
from experiments.public_kg.methods import schema_profile


DEFAULT_BENCHMARK = Path("data/public/benchmark_v2")
DEFAULT_CONTEXT = Path("data/eval/public_kg/short_plan_20261029")
DEFAULT_OUTPUT = Path("data/eval/public_kg/relation_components_20261029")
DATASETS = ("geonames_cn", "foshan_osm")
PREDICTION_SCOPE = "relation-only; class context fixed from Full r1; field mappings are explicit nulls"


def _read_json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _terms(ontology_path: Path):
    raw = _read_json(ontology_path)
    values = raw if isinstance(raw, list) else raw["terms"]
    return {item["uri"]: item for item in values}


def _class_hierarchy(ontology_path: Path):
    """Read subclass axioms from the frozen companion ontology TTL."""
    ttl_path = Path(ontology_path).with_name("spatial_v2.ttl")
    graph = Graph()
    graph.parse(ttl_path, format="turtle")
    parents = defaultdict(set)
    for child, _, parent in graph.triples((None, RDFS.subClassOf, None)):
        parents[str(child)].add(str(parent))

    def is_a(child: str | None, parent: str) -> bool:
        if child is None:
            return False
        pending, visited = [child], set()
        while pending:
            current = pending.pop()
            if current == parent:
                return True
            if current in visited:
                continue
            visited.add(current)
            pending.extend(parents.get(current, ()))
        return False

    return is_a


def _predicate_for_classes(source_class, target_class, terms, is_a):
    """Return only the requested ontology-supported directed relation meaning."""
    classes_by_label = {term.get("label"): uri for uri, term in terms.items()
                        if "class" in str(term.get("type", "")).casefold()}
    props_by_label = {term.get("label"): (uri, term) for uri, term in terms.items()
                      if "class" not in str(term.get("type", "")).casefold()}
    admin = classes_by_label.get("AdministrativeUnit")
    feature_type = classes_by_label.get("FeatureType")
    place = classes_by_label.get("Place")
    if not all((admin, feature_type, place)):
        return None

    if source_class == admin and target_class == admin:
        candidate = props_by_label.get("parentAdministrativeUnit")
        if candidate and candidate[1].get("range") in ("AdministrativeUnit", admin):
            return candidate[0]
    if is_a(source_class, place) and target_class == admin:
        candidate = props_by_label.get("locatedIn")
        if candidate and candidate[1].get("range") in ("AdministrativeUnit", admin):
            return candidate[0]
    if is_a(source_class, place) and target_class == feature_type:
        candidate = props_by_label.get("hasFeatureType")
        if candidate and candidate[1].get("range") in ("FeatureType", feature_type):
            return candidate[0]
    # sameSourceEntity is deliberately not inferred from generic overlap. Equal
    # values alone do not identify globally stable IDs or establish identity.
    return None


def _column_names(profile):
    return {table: {col["name"] for col in metadata["columns"]}
            for table, metadata in profile.items()}


def _fk_candidates(db_path, profile, table_classes, terms, is_a):
    """Generate rules only from declared, single-column SQLite foreign keys.

    Composite foreign keys are counted and skipped because the shared materializer
    has no tuple-valued expression. No overlap threshold is applied to declared
    constraints, so empty references remain visible and materialize zero edges.
    """
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        columns = _column_names(profile)
        rules, skipped_composite = [], 0
        for source_table in sorted(profile):
            fk_rows = conn.execute(
                f"PRAGMA foreign_key_list({quote_identifier(source_table)})").fetchall()
            grouped = defaultdict(list)
            for row in fk_rows:
                grouped[row[0]].append(row)
            for fk_id in sorted(grouped):
                members = sorted(grouped[fk_id], key=lambda row: row[1])
                if len(members) != 1:
                    skipped_composite += 1
                    continue
                row = members[0]
                target_table, source_col, target_col = row[2], row[3], row[4]
                if target_table not in profile or source_col not in columns[source_table]:
                    continue
                if not target_col:
                    target_pk = sorted(
                        (column[5], column[1])
                        for column in conn.execute(
                            f"PRAGMA table_info({quote_identifier(target_table)})")
                        if column[5]
                    )
                    if len(target_pk) != 1:
                        continue
                    target_col = target_pk[0][1]
                if target_col not in columns[target_table]:
                    continue
                predicate = _predicate_for_classes(
                    table_classes.get(source_table), table_classes.get(target_table),
                    terms, is_a)
                if predicate is None:
                    continue
                rules.append({"source_table": source_table, "target_table": target_table,
                              "source_expr": {"op": "column", "name": source_col},
                              "target_expr": {"op": "column", "name": target_col},
                              "predicate": predicate, "direction": "forward"})
        return rules, {"skipped_composite_foreign_keys": skipped_composite}
    finally:
        conn.close()


def _sqlite_affinity(declared_type):
    value = (declared_type or "").upper()
    if "INT" in value:
        return "INTEGER"
    if any(token in value for token in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if any(token in value for token in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    if not value or "BLOB" in value:
        return "BLOB"
    return "NUMERIC"


def _column_rows(conn, table, column):
    """Return exact stored nonempty values with row frequencies; no normalization."""
    query = (f"SELECT {quote_identifier(column)}, COUNT(*) FROM "
             f"{quote_identifier(table)} WHERE {quote_identifier(column)} IS NOT NULL "
             f"GROUP BY {quote_identifier(column)}")
    frequencies = {}
    for value, count in conn.execute(query):
        if isinstance(value, str) and value == "":
            continue
        frequencies[value] = frequencies.get(value, 0) + count
    return frequencies


def _ind_candidates(db_path, profile, table_classes, terms, is_a):
    """Find single-column exact inclusion dependencies using the frozen rows.

    A rule is retained only when both columns have at least two distinct nonempty
    values, target uniqueness is >= .9, and source row hit rate is >= .8. Values
    are compared as SQLite returns them: no case folding, trimming, or name-based
    semantic normalization is performed.
    """
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        table_columns = {table: {col["name"]: col for col in metadata["columns"]}
                         for table, metadata in profile.items()}
        sources, targets = [], []
        for table in sorted(profile):
            for column in profile[table]["columns"]:
                # Cheap profile pruning avoids loading empty and constant columns.
                if column["distinct_count"] < 2:
                    continue
                affinity = _sqlite_affinity(column["type"])
                # Keep each nonconstant source once; target pair checks below enforce
                # class-pair and type compatibility at column granularity.
                sources.append((table, column["name"], affinity))

        # Cache only columns from tables that can serve as a target under an
        # ontology-supported class pair; this avoids probing unrelated columns.
        target_values = {}
        target_meta = {}
        source_classes = {value for value in table_classes.values() if value}
        for target_table in sorted(profile):
            target_class = table_classes.get(target_table)
            if not any(_predicate_for_classes(source_class, target_class, terms, is_a)
                       for source_class in source_classes):
                continue
            for target_column in profile[target_table]["columns"]:
                if target_column["distinct_count"] < 2:
                    continue
                affinity = _sqlite_affinity(target_column["type"])
                key = (target_table, target_column["name"])
                values = _column_rows(conn, target_table, target_column["name"])
                total = sum(values.values())
                if len(values) < 2 or total == 0:
                    continue
                target_values[key] = set(values)
                target_meta[key] = {"distinct": len(values), "rows": total,
                                    "uniqueness": len(values) / total,
                                    "affinity": affinity}

        rules, evidence = [], []
        for source_table, source_column, source_affinity in sources:
            source_class = table_classes.get(source_table)
            source_values = _column_rows(conn, source_table, source_column)
            source_rows = sum(source_values.values())
            if len(source_values) < 2 or source_rows == 0:
                continue
            for (target_table, target_column), values in target_values.items():
                if source_affinity != target_meta[(target_table, target_column)]["affinity"]:
                    continue
                if source_table == target_table and source_column == target_column:
                    continue
                predicate = _predicate_for_classes(
                    source_class, table_classes.get(target_table), terms, is_a)
                if predicate is None:
                    continue
                matched_rows = sum(count for value, count in source_values.items()
                                   if value in values)
                hit_rate = matched_rows / source_rows
                target_info = target_meta[(target_table, target_column)]
                if (hit_rate < 0.8 or target_info["uniqueness"] < 0.9):
                    continue
                rule = {"source_table": source_table, "target_table": target_table,
                        "source_expr": {"op": "column", "name": source_column},
                        "target_expr": {"op": "column", "name": target_column},
                        "predicate": predicate, "direction": "forward"}
                rules.append(rule)
                evidence.append({"rule": rule, "source_distinct_nonempty": len(source_values),
                                 "target_distinct_nonempty": target_info["distinct"],
                                 "source_nonempty_rows": source_rows,
                                 "source_matched_rows": matched_rows,
                                 "source_hit_rate": hit_rate,
                                 "target_nonempty_rows": target_info["rows"],
                                 "target_uniqueness": target_info["uniqueness"],
                                 "sqlite_affinity": source_affinity,
                                 "value_comparison": "exact SQLite-returned values"})
        return rules, evidence
    finally:
        conn.close()


def _complete_predictions(dataset, method, db_path, ontology_path, context_prediction,
                          profile, rules, context_path):
    table_mappings = context_prediction["table_mappings"]
    # Full r1 is the shared semantic class context for every component baseline.
    table_classes = {item["table"]: item.get("target_uri") for item in table_mappings}
    mappings = [{"table": table, "column": column["name"], "target_uri": None}
                for table, data in profile.items() for column in data["columns"]]
    predictions = {"method": method, "method_scope": PREDICTION_SCOPE,
                   "dataset": dataset, "run_status": "success",
                   "table_mappings": table_mappings, "mappings": mappings,
                   "relation_rules": rules,
                   "component_context": {"table_class_method": "public_adapted_full",
                       "table_class_run": f"{dataset}_full_r1",
                       "table_class_prediction_sha256": file_hash(context_path),
                       "field_mapping_component": "not_run; all field targets explicitly null",
                       "reference_data_read_before_prediction_freeze": False},
                   "dataset_sha256": file_hash(db_path),
                   "ontology_sha256": file_hash(ontology_path)}
    return predictions, table_classes


def run_dataset(dataset, benchmark_dir=DEFAULT_BENCHMARK, context_dir=DEFAULT_CONTEXT,
                output_dir=DEFAULT_OUTPUT, materialize_graph=True):
    """Freeze both baseline predictions before any diagnostic reference is read."""
    started = time.perf_counter()
    benchmark_dir, context_dir, output_dir = map(Path, (benchmark_dir, context_dir, output_dir))
    db_path = benchmark_dir / dataset / "sample.sqlite"
    ontology_path = benchmark_dir / "ontology" / "target_terms.json"
    context_path = context_dir / f"{dataset}_full_r1" / "predictions.json"
    context_prediction = _read_json(context_path)
    if context_prediction.get("run_status") != "success":
        raise ValueError(f"Full r1 class context is not successful: {context_path}")
    profile = schema_profile(db_path)
    terms = _terms(ontology_path)
    is_a = _class_hierarchy(ontology_path)
    table_classes = {item["table"]: item.get("target_uri")
                     for item in context_prediction["table_mappings"]}
    methods = {
        "declared_foreign_keys": lambda: _fk_candidates(
            db_path, profile, table_classes, terms, is_a),
        "single_column_inclusion_dependencies": lambda: _ind_candidates(
            db_path, profile, table_classes, terms, is_a),
    }
    dataset_result = {"dataset": dataset, "methods": {}}
    for suffix, discover in methods.items():
        method_started = time.perf_counter()
        rules, discovery = discover()
        method = f"component_baseline_{suffix}"
        run_dir = output_dir / dataset / suffix
        run_dir.mkdir(parents=True, exist_ok=True)
        predictions, _ = _complete_predictions(dataset, method, db_path, ontology_path,
                                                context_prediction, profile, rules, context_path)
        # Freeze predictions and hashes before diagnostics can open machine references.
        prediction_path = run_dir / "predictions.json"
        write_json(prediction_path, predictions)
        manifest = {"status": "success", "dataset": dataset, "method": method,
                    "dataset_sha256": file_hash(db_path),
                    "ontology_sha256": file_hash(ontology_path),
                    "prediction_sha256": file_hash(prediction_path),
                    "method_scope": PREDICTION_SCOPE,
                    "class_context_method": "public_adapted_full",
                    "class_context_run": f"{dataset}_full_r1",
                    "class_context_prediction_sha256": file_hash(context_path),
                    "reference_data_read_before_prediction_freeze": False,
                    "field_mapping_metrics": "not_applicable_component_isolation"}
        write_json(run_dir / "manifest.json", manifest)
        write_json(run_dir / "discovery_evidence.json", discovery if suffix.endswith("dependencies")
                   else {"rules": rules, **discovery})
        graph_metrics = None
        if materialize_graph:
            graph_metrics = materialize(db_path, prediction_path, ontology_path,
                                        run_dir / "graph")
            if graph_metrics.get("status") != "success":
                raise RuntimeError(f"materialization failed for {dataset}/{suffix}: {graph_metrics}")
        elapsed = time.perf_counter() - method_started
        write_json(run_dir / "component_summary.json", {
            "dataset": dataset, "method": method, "status": "success",
            "relation_rules": len(rules),
            "materialized_edges": graph_metrics["unique_relation_edges"] if graph_metrics else None,
            "field_mapping_metrics": "N/A: relation-only component; all field mappings are null",
            "machine_reference_diagnostics": "pending_frozen_prediction_diagnostics",
            "elapsed_seconds_including_materialization": elapsed,
            "discovery": discovery if suffix.endswith("dependencies") else {
                "candidate_rules": len(rules), "criteria": {
                    "type_compatibility": "same SQLite affinity",
                    "target_uniqueness_min": 0.9, "source_hit_rate_min": 0.8,
                    "minimum_distinct_nonempty_values": 2,
                    "value_normalization": "none"}}})
        dataset_result["methods"][suffix] = {
            "status": "success", "rules": len(rules),
            "edges": graph_metrics["unique_relation_edges"] if graph_metrics else None,
            "elapsed_seconds": elapsed, "run_dir": str(run_dir)}
    dataset_result["elapsed_seconds"] = time.perf_counter() - started
    return dataset_result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-dir", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--context-dir", type=Path, default=DEFAULT_CONTEXT)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--dataset", choices=DATASETS, action="append",
                        help="Repeat to run a subset; defaults to both datasets.")
    args = parser.parse_args()
    results = [run_dataset(dataset, args.benchmark_dir, args.context_dir, args.out_dir)
               for dataset in (args.dataset or DATASETS)]
    write_json(args.out_dir / "run_summary.json", {
        "status": "success", "scope": "semantic class context + implicit relation discovery only",
        "methods_are_paper_reproductions": False,
        "validation_scope": "component materialization; separate diagnostics use machine reference v2",
        "datasets": results})
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
