"""Small read-only SQL/SPARQL diagnostics for the frozen GeoNames v2 sample.

This is a machine-reference diagnostic, not a gold query benchmark. Query
definitions are fixed from the GeoNames field documentation and frozen schema;
the script never reads model predictions to choose or formulate the queries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

from rdflib import Graph, Literal, URIRef

from experiments.public_kg.artifacts import file_hash, write_json


SPATIAL = "https://w3id.org/rdb2g/public-spatial/"
AUDIT = "https://w3id.org/rdb2g/audit/"
GEONAMES_README = "https://download.geonames.org/export/dump/readme.txt"
STATUS = "machine_reference_diagnostic_only"


def _term(local):
    return SPATIAL + local


def _table_count_sql(table):
    return f"SELECT COUNT(*) AS answer FROM {table}"


def query_specs():
    """Fixed small question set; no method predictions or outputs are inputs."""
    return [
        {
            "query_id": "GNQ01",
            "question": "How many first-level administrative division lookup records are retained?",
            "sql": _table_count_sql("gn_admin1"),
            "sparql": f'''PREFIX audit: <{AUDIT}>
SELECT (COUNT(DISTINCT ?row) AS ?answer) WHERE {{
  ?row audit:sourceTable "gn_admin1" .
}}''',
            "evidence_sql": "SELECT code, name, geonameid FROM gn_admin1 ORDER BY code LIMIT 5",
            "basis": f"GeoNames readme documents the admin1 code lookup table: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ02",
            "question": "How many place records have a positive population value?",
            "sql": "SELECT COUNT(DISTINCT geonameid) AS answer FROM gn_place WHERE population > 0",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:population ?population .
  FILTER(xsd:integer(?population) > 0)
}}''',
            "evidence_sql": "SELECT geonameid, name, population FROM gn_place WHERE population > 0 ORDER BY geonameid LIMIT 5",
            "basis": f"GeoNames defines population as an integer count: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ03",
            "question": "How many place records use broad feature class P?",
            "sql": "SELECT COUNT(DISTINCT geonameid) AS answer FROM gn_place WHERE feature_class = 'P'",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:featureClassCode "P" .
}}''',
            "evidence_sql": "SELECT geonameid, name, feature_class, feature_code FROM gn_place WHERE feature_class = 'P' ORDER BY geonameid LIMIT 5",
            "basis": f"GeoNames defines feature class as a broad code distinct from feature code: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ04",
            "question": "How many feature-code lookup entries classify first-order administrative divisions?",
            "sql": "SELECT COUNT(*) AS answer FROM gn_feature_code WHERE code = 'A.ADM1'",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?entry) AS ?answer) WHERE {{
  ?entry audit:sourceTable "gn_feature_code" ;
         spatial:categoryCode "A.ADM1" .
}}''',
            "evidence_sql": "SELECT code, name, description FROM gn_feature_code WHERE code = 'A.ADM1'",
            "basis": f"GeoNames feature-code lookup defines ADM1 as first-order administrative division: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ05",
            "question": "How many place records resolve to a retained first-level administrative record?",
            "sql": """SELECT COUNT(DISTINCT p.geonameid) AS answer
FROM gn_place AS p
JOIN gn_admin1 AS a
  ON p.country_code || '.' || p.admin1_code = a.code""",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:locatedIn ?admin1 .
  ?admin1 audit:sourceTable "gn_admin1" .
}}''',
            "evidence_sql": """SELECT p.geonameid, p.admin1_code, a.code
FROM gn_place AS p
JOIN gn_admin1 AS a ON p.country_code || '.' || p.admin1_code = a.code
ORDER BY p.geonameid LIMIT 5""",
            "basis": f"GeoNames specifies the country code plus admin1 code context and defines 00 as an unspecified admin1: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ06",
            "question": "How many place records resolve to a retained second-level administrative record?",
            "sql": """SELECT COUNT(DISTINCT p.geonameid) AS answer
FROM gn_place AS p
JOIN gn_admin2 AS a
  ON p.country_code || '.' || p.admin1_code || '.' || p.admin2_code = a.code""",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:locatedIn ?admin2 .
  ?admin2 audit:sourceTable "gn_admin2" .
}}''',
            "evidence_sql": """SELECT p.geonameid, p.admin1_code, p.admin2_code, a.code
FROM gn_place AS p
JOIN gn_admin2 AS a
  ON p.country_code || '.' || p.admin1_code || '.' || p.admin2_code = a.code
ORDER BY p.geonameid LIMIT 5""",
            "basis": f"GeoNames defines admin2 as a second-level division code resolved in country and admin-code context: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ07",
            "question": "How many place records resolve to a feature-code lookup entry using both class and code?",
            "sql": """SELECT COUNT(DISTINCT p.geonameid) AS answer
FROM gn_place AS p
JOIN gn_feature_code AS f
  ON p.feature_class || '.' || p.feature_code = f.code""",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:hasFeatureType ?featureType .
  ?featureType audit:sourceTable "gn_feature_code" .
}}''',
            "evidence_sql": """SELECT p.geonameid, p.feature_class, p.feature_code, f.code
FROM gn_place AS p
JOIN gn_feature_code AS f
  ON p.feature_class || '.' || p.feature_code = f.code
ORDER BY p.geonameid LIMIT 5""",
            "basis": f"GeoNames documents feature class and feature code as separate fields with a combined lookup key: {GEONAMES_README}",
        },
        {
            "query_id": "GNQ08",
            "question": "How many place records can be followed through a second-level division to a first-level division?",
            "sql": """SELECT COUNT(DISTINCT p.geonameid) AS answer
FROM gn_place AS p
JOIN gn_admin2 AS a2
  ON p.country_code || '.' || p.admin1_code || '.' || p.admin2_code = a2.code
JOIN gn_admin1 AS a1
  ON substr(a2.code, 1, length(a1.code) + 1) = a1.code || '.'""",
            "sparql": f'''PREFIX audit: <{AUDIT}>
PREFIX spatial: <{SPATIAL}>
SELECT (COUNT(DISTINCT ?place) AS ?answer) WHERE {{
  ?place audit:sourceTable "gn_place" ;
         spatial:locatedIn ?admin2 .
  ?admin2 audit:sourceTable "gn_admin2" ;
          spatial:parentAdministrativeUnit ?admin1 .
  ?admin1 audit:sourceTable "gn_admin1" .
}}''',
            "evidence_sql": """SELECT p.geonameid, p.admin1_code, p.admin2_code, a2.code, a1.code
FROM gn_place AS p
JOIN gn_admin2 AS a2
  ON p.country_code || '.' || p.admin1_code || '.' || p.admin2_code = a2.code
JOIN gn_admin1 AS a1
  ON substr(a2.code, 1, length(a1.code) + 1) = a1.code || '.'
ORDER BY p.geonameid LIMIT 5""",
            "basis": f"GeoNames describes admin-code hierarchy and composite administrative lookup keys: {GEONAMES_README}",
        },
    ]


def _readonly(db_path):
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _run_sql(conn, sql):
    if not sql.lstrip().upper().startswith("SELECT"):
        raise ValueError("diagnostic SQL must be a read-only SELECT")
    rows = conn.execute(sql).fetchall()
    return [dict(row) for row in rows]


def _scalar(value):
    if hasattr(value, "toPython"):
        value = value.toPython()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _run_sparql(graph, sparql):
    result = graph.query(sparql)
    names = [str(item) for item in result.vars]
    return [{name: _scalar(value) for name, value in zip(names, row)} for row in result]


def _graph_indexes(graph):
    """Build bounded indexes for the fixed diagnostic predicates."""
    table_predicate = URIRef(AUDIT + "sourceTable")
    predicates = {
        URIRef(_term("population")),
        URIRef(_term("featureClassCode")),
        URIRef(_term("categoryCode")),
        URIRef(_term("locatedIn")),
        URIRef(_term("hasFeatureType")),
        URIRef(_term("parentAdministrativeUnit")),
    }
    tables = defaultdict(set)
    values = defaultdict(lambda: defaultdict(set))
    pairs = defaultdict(set)
    for subject, _, obj in graph.triples((None, table_predicate, None)):
        tables[str(obj)].add(subject)
    for predicate in predicates:
        for subject, _, obj in graph.triples((None, predicate, None)):
            if predicate in {URIRef(_term("locatedIn")), URIRef(_term("hasFeatureType")),
                             URIRef(_term("parentAdministrativeUnit"))}:
                pairs[str(predicate)].add((subject, obj))
            else:
                values[str(predicate)][subject].add(obj)
    table_by_node = {}
    for table, nodes in tables.items():
        for node in nodes:
            table_by_node[node] = table
    return {"tables": tables, "table_by_node": table_by_node,
            "values": values, "pairs": pairs}


def _run_graph_index(indexes, query_id):
    """Run the fixed question's graph pattern over rdflib's indexed triples."""
    tables = indexes["tables"]
    table_by_node = indexes["table_by_node"]
    values = indexes["values"]
    pairs = indexes["pairs"]

    if query_id == "GNQ01":
        answer = len(tables["gn_admin1"])
    elif query_id == "GNQ02":
        population = values[str(URIRef(_term("population")))]
        answer = sum(
            1 for node in tables["gn_place"]
            if any((_scalar(value) or 0) > 0 for value in population.get(node, ()))
        )
    elif query_id == "GNQ03":
        classes = values[str(URIRef(_term("featureClassCode")))]
        answer = sum(1 for node in tables["gn_place"]
                     if Literal("P") in classes.get(node, ()))
    elif query_id == "GNQ04":
        codes = values[str(URIRef(_term("categoryCode")))]
        answer = sum(1 for node in tables["gn_feature_code"]
                     if Literal("A.ADM1") in codes.get(node, ()))
    elif query_id in {"GNQ05", "GNQ06"}:
        target_table = "gn_admin1" if query_id == "GNQ05" else "gn_admin2"
        predicate = str(URIRef(_term("locatedIn")))
        answer = len({
            source for source, target in pairs[predicate]
            if source in tables["gn_place"] and target in tables[target_table]
        })
    elif query_id == "GNQ07":
        predicate = str(URIRef(_term("hasFeatureType")))
        answer = len({
            source for source, target in pairs[predicate]
            if source in tables["gn_place"] and target in tables["gn_feature_code"]
        })
    elif query_id == "GNQ08":
        located = pairs[str(URIRef(_term("locatedIn")))]
        parents = pairs[str(URIRef(_term("parentAdministrativeUnit")))]
        admin2_with_admin1_parent = {
            child for child, parent in parents
            if child in tables["gn_admin2"] and parent in tables["gn_admin1"]
        }
        answer = len({
            place for place, admin2 in located
            if place in tables["gn_place"] and admin2 in admin2_with_admin1_parent
        })
    else:
        raise ValueError(f"unknown fixed query id: {query_id}")
    return [{"answer": answer}]


def _canonical_hash(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _compare(sql_rows, graph_rows):
    """Compare ordered normalized answer rows and expose a simple cell match rate."""
    sql_values = [[_scalar(value) for value in row.values()] for row in sql_rows]
    graph_values = [[_scalar(value) for value in row.values()] for row in graph_rows]
    # Both diagnostic query families return a single count cell. Names differ, so
    # compare their first projected scalar while preserving complete results above.
    sql_answer = sql_values[0][0] if sql_values and sql_values[0] else None
    graph_answer = graph_values[0][0] if graph_values and graph_values[0] else None
    exact = sql_answer == graph_answer
    return {
        "sql_answer": sql_answer,
        "graph_answer": graph_answer,
        "exact_match": exact,
        "matching_rate": 1.0 if exact else 0.0,
    }


def run_diagnostics(db_path, ttl_path, tasks=None):
    tasks = query_specs() if tasks is None else tasks
    conn = _readonly(db_path)
    graph = Graph()
    try:
        graph.parse(ttl_path, format="turtle")
        indexes = _graph_indexes(graph)
        results = []
        for spec in tasks:
            try:
                sql_rows = _run_sql(conn, spec["sql"])
                graph_rows = _run_graph_index(indexes, spec["query_id"])
                raw_evidence = _run_sql(conn, spec["evidence_sql"])
                comparison = _compare(sql_rows, graph_rows)
                results.append({
                    "query_id": spec["query_id"],
                    "question": spec["question"],
                    "status": "ok",
                    "sql_result": sql_rows,
                    "graph_result": graph_rows,
                    "graph_execution": "rdflib_fixed_triple_index",
                    "sparql_execution": "not_run_for_full_graph; equivalence tested on fixture graphs",
                    **comparison,
                    "raw_source_values": raw_evidence,
                    "raw_source_values_sha256": _canonical_hash(raw_evidence),
                    "sql_result_sha256": _canonical_hash(sql_rows),
                    "graph_result_sha256": _canonical_hash(graph_rows),
                    "sql_sha256": _canonical_hash(spec["sql"]),
                    "sparql_sha256": _canonical_hash(spec["sparql"]),
                })
            except Exception as exc:  # Explicitly report errors rather than hiding a query.
                results.append({
                    "query_id": spec["query_id"],
                    "question": spec["question"],
                    "status": "error",
                    "exception_type": type(exc).__name__,
                    "exception": str(exc),
                    "sql_sha256": _canonical_hash(spec["sql"]),
                    "sparql_sha256": _canonical_hash(spec["sparql"]),
                })
        successful = [item for item in results if item["status"] == "ok"]
        matches = sum(item["exact_match"] for item in successful)
        return {
            "status": STATUS,
            "database_sha256": file_hash(db_path),
            "ttl_sha256": file_hash(ttl_path),
            "database": str(db_path),
            "ttl": str(ttl_path),
            "query_count": len(results),
            "successful_queries": len(successful),
        "graph_execution": "rdflib_fixed_triple_index",
        "sparql_execution": "fixture_equivalence_tests_only",
            "error_queries": len(results) - len(successful),
            "exact_matches": matches,
            "overall_matching_rate": matches / len(successful) if successful else None,
            "results": results,
        }
    finally:
        conn.close()


def discover_runs(roots, include_full=False, run_filter=None):
    runs = []
    filter_names = set(run_filter or [])
    for root in roots:
        for manifest_path in sorted(Path(root).glob("*/manifest.json")):
            run_dir = manifest_path.parent
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("dataset") != "geonames_cn":
                continue
            name = run_dir.name
            is_full = name == "geonames_cn_full" or name.startswith("geonames_cn_full_") or name.startswith("geonames_full_")
            if is_full and not include_full:
                continue
            if filter_names and name not in filter_names:
                continue
            ttl_path = run_dir / "graph" / "graph.ttl"
            if ttl_path.is_file() and manifest.get("status") == "success":
                runs.append((name, run_dir, manifest, ttl_path))
    return runs


def execute(root_out, db_path, run_roots, include_full=False, run_filter=None):
    root_out = Path(root_out)
    root_out.mkdir(parents=True, exist_ok=True)
    tasks = query_specs()
    tasks_path = root_out / "query_tasks.json"
    write_json(tasks_path, {
        "status": STATUS,
        "dataset": "geonames_cn",
        "human_gold": False,
        "query_count": len(tasks),
        "source_documentation": GEONAMES_README,
        "selection_policy": "Fixed from official field definitions and the frozen sample schema; no method predictions were read.",
        "graph_execution": "Fixed rdflib triple-index traversal; task SPARQL is retained as the equivalent graph pattern and is checked against real SPARQL on test fixtures.",
        "tasks": tasks,
    })
    runs = discover_runs(run_roots, include_full=include_full, run_filter=run_filter)
    run_summaries = []
    for name, run_dir, run_manifest, ttl_path in runs:
        output_dir = root_out / name
        output_dir.mkdir(parents=True, exist_ok=True)
        result = run_diagnostics(db_path, ttl_path, tasks)
        expected_db_hash = run_manifest.get("dataset_sha256")
        result["run_name"] = name
        result["run_manifest"] = str(run_dir / "manifest.json")
        result["run_manifest_sha256"] = file_hash(run_dir / "manifest.json")
        result["source_hash_matches_run_manifest"] = expected_db_hash == result["database_sha256"]
        result["run_method"] = run_manifest.get("method")
        result["diagnostic_status"] = STATUS
        result_path = output_dir / "diagnostic_results.json"
        write_json(result_path, result)
        write_json(output_dir / "manifest.json", {
            "status": STATUS,
            "run_name": name,
            "dataset": "geonames_cn",
            "human_gold": False,
            "query_tasks": str(tasks_path),
            "query_tasks_sha256": file_hash(tasks_path),
            "results": str(result_path),
            "database_sha256": result["database_sha256"],
            "ttl_sha256": result["ttl_sha256"],
            "source_hash_matches_run_manifest": result["source_hash_matches_run_manifest"],
        })
        run_summaries.append({
            "run_name": name,
            "status": result["status"],
            "query_count": result["query_count"],
            "successful_queries": result["successful_queries"],
            "error_queries": result["error_queries"],
            "exact_matches": result["exact_matches"],
            "overall_matching_rate": result["overall_matching_rate"],
            "source_hash_matches_run_manifest": result["source_hash_matches_run_manifest"],
            "results": str(result_path),
        })
    write_json(root_out / "summary.json", {
        "status": STATUS,
        "dataset": "geonames_cn",
        "human_gold": False,
        "run_count": len(run_summaries),
        "query_count": len(tasks),
        "excluded_full_runs": not include_full,
        "runs": run_summaries,
    })
    return run_summaries


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/public/benchmark_v2/geonames_cn/sample.sqlite")
    parser.add_argument("--out-dir", default="data/eval/public_kg/query_reviewed_v2")
    parser.add_argument(
        "--run-root", action="append", dest="run_roots",
        help="Root containing per-run manifests and graph/graph.ttl; repeatable.",
    )
    parser.add_argument("--include-full", action="store_true", help="Include completed Full runs.")
    parser.add_argument("--run", action="append", dest="run_filter", help="Restrict to this run name; repeatable.")
    args = parser.parse_args(argv)
    run_roots = args.run_roots or [
        "data/eval/public_kg/reviewed_v2",
        "data/eval/public_kg/pilot_20261029",
    ]
    summaries = execute(args.out_dir, args.db_path, run_roots, args.include_full, args.run_filter)
    print(json.dumps(summaries, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
