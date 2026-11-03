"""Project LLM4VKG R2RML literal-column maps into the v2 field format.

This is a lossless evidence projection, not ontology matching: a predicate is
accepted only when its URI is already an explicitly declared datatype-property
URI in the supplied input and generated ontologies and the fixed v2 vocabulary. The raw
R2RML mapping and generated ontology remain untouched and are hash-linked in the
output manifest.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from rdflib import Graph, Namespace, RDF, OWL, URIRef
from rdflib.compare import isomorphic

from experiments.public_kg.artifacts import file_hash, write_json


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / "data/public/benchmark_v2/geonames_cn/sample.sqlite"
DEFAULT_ORIGINAL_MAPPING = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_generated_mapping.ttl"
DEFAULT_MAPPING = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_generated_mapping_sqlite.ttl"
DEFAULT_INPUT_ONTOLOGY = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_input_ontology.ttl"
DEFAULT_GENERATED_ONTOLOGY = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_generated_ontology.ttl"
DEFAULT_GENERATION_RECORD = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg-oc-mg_geonames_cn.json"
DEFAULT_TARGET_TERMS = ROOT / "data/public/benchmark_v2/ontology/target_terms.json"
DEFAULT_MATERIALIZED = ROOT / "data/eval/public_kg/short_plan_external/llm4vkg_geonames/materialized.ttl"
DEFAULT_OUT = ROOT / "data/eval/public_kg/short_plan_external/llm4vkg_geonames/field_mapping_v2"
RR = Namespace("http://www.w3.org/ns/r2rml#")


def _database_fields(db_path: Path) -> list[tuple[str, str]]:
    """Return every frozen database field in stable order, using read-only SQLite."""
    uri = f"file:{db_path.resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        fields = []
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            fields.extend((table, row[1]) for row in conn.execute(f"PRAGMA table_info({quoted})"))
    return fields


def _extract_r2rml_field_evidence(mapping_path: Path, db_path: Path) -> dict[tuple[str, str], list[dict]]:
    """Read raw R2RML POMs without rewriting or inferring source mappings."""
    graph = Graph().parse(str(mapping_path), format="turtle")
    known_fields = set(_database_fields(db_path))
    evidence: dict[tuple[str, str], list[dict]] = defaultdict(list)
    triples_maps = sorted(set(graph.subjects(RDF.type, RR.TriplesMap)), key=str)
    if not triples_maps:
        raise ValueError("R2RML input contains no rr:TriplesMap")

    for triples_map in triples_maps:
        logical_tables = list(graph.objects(triples_map, RR.logicalTable))
        if len(logical_tables) != 1:
            raise ValueError(f"{triples_map}: expected exactly one rr:logicalTable")
        sql_queries = list(graph.objects(logical_tables[0], RR.sqlQuery))
        if len(sql_queries) != 1:
            raise ValueError(f"{triples_map}: expected exactly one rr:sqlQuery")
        match = re.fullmatch(r'\s*SELECT\s+\*\s+FROM\s+"([^"]+)"\s*;?\s*',
                             str(sql_queries[0]), flags=re.IGNORECASE)
        if not match:
            raise ValueError(f"{triples_map}: unsupported SQL view; expected SELECT * FROM \"table\"")
        table = match.group(1)
        if not any(field_table == table for field_table, _ in known_fields):
            raise ValueError(f"{triples_map}: source table {table!r} is absent from the database")

        for pom in graph.objects(triples_map, RR.predicateObjectMap):
            predicates = list(graph.objects(pom, RR.predicate))
            object_maps = list(graph.objects(pom, RR.objectMap))
            if len(predicates) != 1 or len(object_maps) != 1:
                raise ValueError(f"{triples_map}: expected one predicate and object map per POM")
            columns = list(graph.objects(object_maps[0], RR.column))
            templates = list(graph.objects(object_maps[0], RR.template))
            if len(columns) != 1 or templates:
                # Keep unsupported object-map forms out of field projection. The
                # source mapping itself remains preserved and hash-linked.
                continue
            column = str(columns[0])
            if (table, column) not in known_fields:
                raise ValueError(f"{triples_map}: rr:column {column!r} is absent from {table}")
            datatypes = list(graph.objects(object_maps[0], RR.datatype))
            term_types = list(graph.objects(object_maps[0], RR.termType))
            evidence[(table, column)].append({
                "triples_map_iri": str(triples_map),
                "predicate_uri": str(predicates[0]),
                "rr_datatype": str(datatypes[0]) if len(datatypes) == 1 else None,
                "rr_term_type": str(term_types[0]) if len(term_types) == 1 else None,
            })
    return evidence


def _validate_sqlite_adaptation(original_mapping_path: Path, adapted_mapping_path: Path) -> int:
    """Check that the existing dialect adaptation changed only SQL view triples."""
    original = Graph().parse(str(original_mapping_path), format="turtle")
    adapted = Graph().parse(str(adapted_mapping_path), format="turtle")
    original_semantics, adapted_semantics = Graph(), Graph()
    for triple in original:
        if triple[1] != RR.sqlQuery:
            original_semantics.add(triple)
    for triple in adapted:
        if triple[1] != RR.sqlQuery:
            adapted_semantics.add(triple)
    if not isomorphic(original_semantics, adapted_semantics):
        raise ValueError("SQLite-adapted R2RML changed triples outside rr:sqlQuery")
    original_queries = list(original.objects(None, RR.sqlQuery))
    adapted_queries = list(adapted.objects(None, RR.sqlQuery))
    if not original_queries or len(original_queries) != len(adapted_queries):
        raise ValueError("original and adapted R2RML SQL query counts differ or are empty")
    return len(original_queries)


def normalize(original_mapping_path: Path, mapping_path: Path, input_ontology_path: Path,
              generated_ontology_path: Path, generation_record_path: Path, db_path: Path,
              target_terms_path: Path, output_dir: Path,
              materialized_graph_path: Path | None = None) -> dict:
    target_obj = json.loads(target_terms_path.read_text(encoding="utf-8"))
    target_terms = target_obj.get("terms", target_obj.get("target_terms", []))
    target_by_uri = {term["uri"]: term for term in target_terms}
    input_ontology = Graph().parse(str(input_ontology_path), format="turtle")
    generated = Graph().parse(str(generated_ontology_path), format="turtle")
    adapted_sql_query_count = _validate_sqlite_adaptation(original_mapping_path, mapping_path)
    field_evidence = _extract_r2rml_field_evidence(mapping_path, db_path)
    fields = _database_fields(db_path)

    mappings = []
    unmapped_reasons = defaultdict(int)
    for table, column in fields:
        raw_evidence = field_evidence.get((table, column), [])
        eligible = []
        rejected = []
        for item in raw_evidence:
            uri = item["predicate_uri"]
            term = target_by_uri.get(uri)
            # Require the exact term itself to have the expected type. Never
            # use a similar label, a hierarchy, or a reference answer to align it.
            declared_as_datatype = (uri in target_by_uri and
                                    (URIRef(uri), RDF.type, OWL.DatatypeProperty) in input_ontology and
                                    (URIRef(uri), RDF.type, OWL.DatatypeProperty) in generated)
            if (term and term.get("type") == "Property" and
                    term.get("property_kind") == "DatatypeProperty" and declared_as_datatype):
                eligible.append({**item, "normalized_target_uri": uri,
                                 "normalization": "exact_uri_in_generated_and_fixed_vocabulary"})
            else:
                rejected.append({**item, "normalization": "unresolved_outside_exact_datatype_vocabulary"})

        distinct = sorted({item["normalized_target_uri"] for item in eligible})
        if len(distinct) == 1:
            target_uri = distinct[0]
            status = "exact_uri_projection"
        elif len(distinct) > 1:
            target_uri = None
            status = "ambiguous_multiple_exact_predicates"
        elif raw_evidence:
            target_uri = None
            status = "unresolved_raw_predicate"
        else:
            target_uri = None
            status = "no_rr_column_predicate"
        if target_uri is None:
            unmapped_reasons[status] += 1
        mappings.append({
            "table": table,
            "column": column,
            "target_uri": target_uri,
            "selection_policy": "exact_uri_only_no_semantic_repair",
            "normalization_status": status,
            "raw_r2rml_evidence": raw_evidence,
            "rejected_r2rml_evidence": rejected,
        })

    output_dir.mkdir(parents=True, exist_ok=True)
    prediction = {
        "method": "LLM4VKG",
        "run_status": "success",
        "table_mappings": [],
        "mappings": mappings,
        "relation_rules": [],
        "normalization": {
            "policy": "Exact predicate URI projection only; no label matching, hierarchy alignment, or use of reference labels.",
            "raw_r2rml_preserved": True,
            "raw_r2rml_path": str(original_mapping_path.resolve()),
            "r2rml_used_for_projection": str(mapping_path.resolve()),
            "raw_generated_ontology_path": str(generated_ontology_path.resolve()),
        },
    }
    predictions_path = output_dir / "predictions.json"
    write_json(predictions_path, prediction)
    manifest = {
        "method": "LLM4VKG",
        "dataset": "geonames_cn",
        "status": "normalized_field_mapping_ready_for_diagnostic",
        "source_database": str(db_path.resolve()),
        "dataset_sha256": file_hash(db_path),
        "target_vocabulary": str(target_terms_path.resolve()),
        "ontology_sha256": file_hash(target_terms_path),
        "raw_r2rml_mapping": str(original_mapping_path.resolve()),
        "raw_r2rml_mapping_sha256": file_hash(original_mapping_path),
        "sqlite_adapted_r2rml_mapping": str(mapping_path.resolve()),
        "sqlite_adapted_r2rml_mapping_sha256": file_hash(mapping_path),
        "sql_dialect_adaptation_non_sql_triples_isomorphic": True,
        "r2rml_sql_view_count": adapted_sql_query_count,
        "input_ontology": str(input_ontology_path.resolve()),
        "input_ontology_sha256": file_hash(input_ontology_path),
        "generated_ontology": str(generated_ontology_path.resolve()),
        "generated_ontology_sha256": file_hash(generated_ontology_path),
        "generation_record": str(generation_record_path.resolve()),
        "generation_record_sha256": file_hash(generation_record_path),
        "materialized_graph": str(materialized_graph_path.resolve()) if materialized_graph_path else None,
        "materialized_graph_sha256": file_hash(materialized_graph_path) if materialized_graph_path else None,
        "prediction_sha256": file_hash(predictions_path),
        "field_count": len(fields),
        "mapped_field_count": sum(item["target_uri"] is not None for item in mappings),
        "unmapped_field_count": sum(item["target_uri"] is None for item in mappings),
        "unmapped_status_counts": dict(sorted(unmapped_reasons.items())),
        "full_pipeline_reproduction": False,
        "human_gold": False,
        "quality_scored": False,
        "normalization_basis": "Predicate URI must exist unchanged as owl:DatatypeProperty in LLM4VKG's input ontology, generated ontology, and fixed v2 target vocabulary.",
        "reference_independence": "The machine reference is not read during normalization; it is used only by the separate machine_diagnostics scorer.",
        "relation_status": "not_projected; no relation rules were emitted and no edge predictions were fabricated",
        "limitations": [
            "This projects available rr:column predicate maps; it does not infer property mappings from rr:subjectMap templates.",
            "Mappings to ontology terms with different URIs or non-datatype predicates remain null, even if labels appear similar.",
            "A direct URI match can still be semantically wrong for a source field; the second machine reference reports those disagreements without repairing them.",
            "The original LLM4VKG-generated R2RML and ontology files are read-only inputs and remain the raw outputs.",
        ],
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--original-mapping", type=Path, default=DEFAULT_ORIGINAL_MAPPING)
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--input-ontology", type=Path, default=DEFAULT_INPUT_ONTOLOGY)
    parser.add_argument("--generated-ontology", type=Path, default=DEFAULT_GENERATED_ONTOLOGY)
    parser.add_argument("--generation-record", type=Path, default=DEFAULT_GENERATION_RECORD)
    parser.add_argument("--target-terms", type=Path, default=DEFAULT_TARGET_TERMS)
    parser.add_argument("--materialized-graph", type=Path, default=DEFAULT_MATERIALIZED)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    for path in (args.db, args.original_mapping, args.mapping, args.input_ontology, args.generated_ontology,
                 args.generation_record, args.target_terms):
        if not path.is_file():
            parser.error(f"required input does not exist: {path}")
    materialized = args.materialized_graph if args.materialized_graph.is_file() else None
    result = normalize(args.original_mapping.resolve(), args.mapping.resolve(), args.input_ontology.resolve(),
                       args.generated_ontology.resolve(), args.generation_record.resolve(), args.db.resolve(),
                       args.target_terms.resolve(), args.out_dir.resolve(), materialized)
    print(json.dumps({key: result[key] for key in
                      ("method", "dataset", "status", "field_count", "mapped_field_count",
                       "unmapped_field_count", "unmapped_status_counts")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
