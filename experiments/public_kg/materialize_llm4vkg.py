"""Materialize the frozen LLM4VKG GeoNames SQLite mapping with Morph-KGC.

This executes an existing, SQL-dialect-adapted R2RML mapping. It does not
rewrite ontology terms or mapping semantics. Install Morph-KGC in an isolated
environment with ``pip install 'morph-kgc[sqlite]==2.10.0'`` and run this file
with that environment's Python interpreter.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import re
import sqlite3
import time
import traceback
from pathlib import Path

from rdflib import Graph, Namespace, RDF, RDFS, OWL, URIRef


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / "data/public/benchmark_v2/geonames_cn/sample.sqlite"
DEFAULT_MAPPING = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_generated_mapping_sqlite.ttl"
DEFAULT_GENERATED_ONTOLOGY = ROOT / "data/eval/public_kg/external_setup_v2/llm4vkg_geonames_cn_generated_ontology.ttl"
DEFAULT_OUT = ROOT / "data/eval/public_kg/short_plan_external/llm4vkg_geonames"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_only_row_counts(db_path: Path) -> dict[str, int]:
    uri = f"file:{db_path.resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        return {
            table: int(conn.execute(f'SELECT COUNT(*) FROM "{table.replace(chr(34), chr(34) * 2)}"').fetchone()[0])
            for table in tables
        }


def audit_mapping(db_path: Path, mapping_path: Path, generated_ontology: Path) -> dict:
    """Record mapping terms as emitted; this audit does not repair semantics."""
    rr = Namespace("http://www.w3.org/ns/r2rml#")
    mapping = Graph().parse(str(mapping_path), format="turtle")
    ontology = Graph().parse(str(generated_ontology), format="turtle")
    columns = {}
    with sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True) as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns[table] = {r[1]: {"declared_type": r[2] or "", "primary_key": bool(r[5])}
                              for r in conn.execute(f"PRAGMA table_info({quoted})")}

    triples_maps = []
    predicate_iris = set()
    mapped_columns = set()
    datatype_counts = {}
    missing_columns = []
    literal_datatype_non_text_columns = []
    for tm in sorted(set(mapping.subjects(RDF.type, rr.TriplesMap)), key=str):
        logical_tables = list(mapping.objects(tm, rr.logicalTable))
        sql = str(next(iter(mapping.objects(logical_tables[0], rr.sqlQuery)), "")) if logical_tables else ""
        table_match = re.search(r'\bFROM\s+"([^"]+)"', sql, re.IGNORECASE)
        table = table_match.group(1) if table_match else None
        if table not in columns:
            table = None
        subject_maps = list(mapping.objects(tm, rr.subjectMap))
        subject_map = subject_maps[0] if subject_maps else None
        subject_template = str(next(iter(mapping.objects(subject_map, rr.template)), "")) if subject_map else ""
        classes = [str(x) for x in mapping.objects(subject_map, rr["class"])] if subject_map else []
        poms = []
        for pom in mapping.objects(tm, rr.predicateObjectMap):
            predicate = next(iter(mapping.objects(pom, rr.predicate)), None)
            for object_map in mapping.objects(pom, rr.objectMap):
                column = next(iter(mapping.objects(object_map, rr.column)), None)
                datatype = next(iter(mapping.objects(object_map, rr.datatype)), None)
                term_type = next(iter(mapping.objects(object_map, rr.termType)), None)
                if predicate:
                    predicate_iris.add(str(predicate))
                source_column = str(column) if column else None
                if table and source_column:
                    mapped_columns.add((table, source_column))
                    if source_column not in columns[table]:
                        missing_columns.append({"table": table, "column": source_column})
                datatype_iri = str(datatype) if datatype else "(no rr:datatype)"
                datatype_counts[datatype_iri] = datatype_counts.get(datatype_iri, 0) + 1
                source_type = columns.get(table, {}).get(source_column, {}).get("declared_type") if table and source_column else None
                if datatype_iri == str(RDFS.Literal) and source_type and source_type.upper() not in {"TEXT", "CHAR", "VARCHAR", "CLOB"}:
                    literal_datatype_non_text_columns.append({
                        "table": table,
                        "column": source_column,
                        "sqlite_declared_type": source_type,
                    })
                poms.append({
                    "predicate_iri": str(predicate) if predicate else None,
                    "predicate_ontology_types": [str(x) for x in ontology.objects(predicate, RDF.type)] if predicate else [],
                    "predicate_ontology_ranges": [str(x) for x in ontology.objects(predicate, RDFS.range)] if predicate else [],
                    "source_column": source_column,
                    "source_declared_type": source_type,
                    "rr_datatype": datatype_iri,
                    "rr_term_type": str(term_type) if term_type else None,
                })
        triples_maps.append({
            "triples_map_iri": str(tm),
            "source_table": table,
            "subject_template": subject_template,
            "subject_classes": classes,
            "class_ontology_types": {c: [str(x) for x in ontology.objects(URIRef(c), RDF.type)] for c in classes},
            "predicate_object_maps": poms,
        })

    object_property_count = len(set(ontology.subjects(RDF.type, OWL.ObjectProperty)))
    mapped_object_properties = sum(1 for tm in triples_maps for pom in tm["predicate_object_maps"]
                                   if str(OWL.ObjectProperty) in pom["predicate_ontology_types"])
    rdfs_literal_count = datatype_counts.get(str(RDFS.Literal), 0)
    warnings = []
    if rdfs_literal_count:
        warnings.append(f"All {rdfs_literal_count} rr:datatype declarations use rdfs:Literal. This is a class IRI, not a datatype IRI for R2RML rr:datatype; Morph-KGC nevertheless emitted typed literals with it. No datatype correction was applied.")
    if object_property_count and mapped_object_properties == 0:
        warnings.append(f"Generated ontology declares {object_property_count} owl:ObjectProperty terms, but this mapping has no object-property maps.")
    return {
        "triples_map_count": len(triples_maps),
        "predicate_object_map_count": sum(len(tm["predicate_object_maps"]) for tm in triples_maps),
        "mapped_predicate_iri_count": len(predicate_iris),
        "mapped_source_column_count": len(mapped_columns),
        "generated_ontology_object_property_count": object_property_count,
        "mapped_object_property_count": mapped_object_properties,
        "datatype_spec_reference": "https://www.w3.org/TR/r2rml/#datatype",
        "rr_datatype_counts": datatype_counts,
        "rdfs_literal_datatype_mapping_count": rdfs_literal_count,
        "rdfs_literal_datatype_non_text_source_columns": literal_datatype_non_text_columns,
        "missing_source_column_references": missing_columns,
        "triples_maps": triples_maps,
        "warnings": warnings,
    }


def materialize(db_path: Path, mapping_path: Path, out_dir: Path,
                generated_ontology: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    graph_path = out_dir / "materialized.ttl"
    manifest_path = out_dir / "manifest.json"
    before = {
        "source_database_sha256": sha256(db_path),
        "adapted_mapping_sha256": sha256(mapping_path),
        "generated_ontology_sha256": sha256(generated_ontology),
    }
    rows = read_only_row_counts(db_path)
    manifest = {
        "method": "LLM4VKG",
        "status": "running",
        "materializer": "Morph-KGC",
        "materializer_version": importlib.metadata.version("morph-kgc"),
        "r2rml_mapping": str(mapping_path),
        "generated_ontology": str(generated_ontology),
        "source_database": str(db_path),
        "source_database_read_mode": "SQLite URI mode=ro for source inspection and SQLAlchemy URI mode=ro for R2RML execution",
        "hashes": before,
        "source_rows_by_table": rows,
        "source_row_count_total": sum(rows.values()),
        "mapping_audit": audit_mapping(db_path, mapping_path, generated_ontology),
        "output_graph": str(graph_path),
        "full_pipeline_reproduction": False,
        "quality_scored": False,
        "quality_scoring_reason": "No independent human gold; generated concepts and mapping terms were retained as emitted and were not normalized to the fixed v2 evaluation vocabulary.",
        "semantics_modified": False,
        "errors": [],
    }

    started = time.monotonic()
    try:
        from morph_kgc import materialize as morph_materialize
        # The SQLAlchemy URI enables SQLite URI parsing and mode=ro. This is
        # checked by the separate audit probe below as well as used in-engine.
        db_url = f"sqlite:///file:{db_path.resolve()}?mode=ro&uri=true"
        config = f"""[CONFIGURATION]
number_of_processes: 1
logging_level: INFO
output_format: N-TRIPLES

[DataSource1]
mappings: {mapping_path.resolve()}
db_url: {db_url}
"""
        graph = morph_materialize(config)
        graph.serialize(destination=str(graph_path), format="turtle", encoding="utf-8")
        manifest["status"] = "materialization_completed"
        manifest["triple_count"] = len(graph)
        manifest["output_bytes"] = graph_path.stat().st_size
        manifest["output_sha256"] = sha256(graph_path)
        manifest["predicate_triple_counts"] = {
            str(predicate): sum(1 for _ in graph.triples((None, predicate, None)))
            for predicate in sorted(set(graph.predicates()), key=str)
        }
        manifest["subject_class_counts"] = {
            str(class_iri): sum(1 for _ in graph.triples((None, RDF.type, class_iri)))
            for class_iri in sorted(set(graph.objects(None, RDF.type)), key=str)
        }
    except Exception as exc:
        manifest["status"] = "materialization_failed"
        manifest["errors"].append({
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        })
        if graph_path.exists():
            manifest["partial_output_bytes"] = graph_path.stat().st_size
    finally:
        manifest["elapsed_seconds"] = round(time.monotonic() - started, 3)
        after_db_sha = sha256(db_path)
        manifest["source_database_sha256_after"] = after_db_sha
        manifest["source_database_unchanged"] = before["source_database_sha256"] == after_db_sha
        manifest["limitations"] = [
            "Uses the Morph-KGC R2RML/RML processor, not the LLM4VKG project's unavailable Ontop runtime.",
            "The R2RML SQL dialect was previously adapted only from schema-qualified SELECT * to SQLite SELECT *; mapping IRIs and predicates were retained.",
            "Generated ontology semantics remain as emitted by LLM4VKG. No alignment to the fixed v2 ontology or semantic repair was applied.",
            "A successful RDF materialization demonstrates executable mappings only; it does not establish mapping correctness or comparability with Full.",
        ]
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--generated-ontology", type=Path, default=DEFAULT_GENERATED_ONTOLOGY)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    for path in (args.db, args.mapping, args.generated_ontology):
        if not path.is_file():
            parser.error(f"required input does not exist: {path}")
    result = materialize(args.db.resolve(), args.mapping.resolve(), args.out_dir.resolve(),
                         args.generated_ontology.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "materialization_completed" and result["source_database_unchanged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
