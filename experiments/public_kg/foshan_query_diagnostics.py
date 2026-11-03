"""Fixed SQL/RDF diagnostics for the frozen Foshan v2 sample.

These tasks were derived from the local dataset documentation and frozen
SQLite schema. They are machine-reference diagnostics, not human-gold scores.
Full graphs are traversed through rdflib's indexed triples; SPARQL forms are
retained with each task and checked against small fixtures by the tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from rdflib import Graph, Literal, URIRef

from experiments.public_kg.artifacts import file_hash, write_json


SPATIAL = "https://w3id.org/rdb2g/public-spatial/"
AUDIT = "https://w3id.org/rdb2g/audit/"
LOCAL_SOURCE = "data/public/数据集说明.md"
STATUS = "machine_reference_diagnostic_only"


def _table_count_sql(table: str) -> str:
    return f"SELECT COUNT(*) AS answer FROM {table}"


def query_specs() -> list[dict]:
    """Return the fixed question set without reading method outputs."""
    return [
        {
            "query_id": "FSQ01",
            "task_type": "data_statistic",
            "question": "How many street records are retained in the sample?",
            "sql": _table_count_sql("zs_street_bz"),
            "sparql": f'''PREFIX audit: <{AUDIT}>\nSELECT (COUNT(DISTINCT ?row) AS ?answer) WHERE {{\n  ?row audit:sourceTable "zs_street_bz" .\n}}''',
            "evidence_sql": "SELECT gid, GUID, MC FROM zs_street_bz ORDER BY rowid LIMIT 5",
            "basis": f"Frozen table inventory in {LOCAL_SOURCE}; records are counted by sourceTable.",
        },
        {
            "query_id": "FSQ02",
            "task_type": "data_statistic",
            "question": "How many POI records have a nonempty primary name?",
            "sql": "SELECT COUNT(*) AS answer FROM zs_POI_dm WHERE MC IS NOT NULL AND TRIM(MC) <> ''",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?row) AS ?answer) WHERE {{\n  ?row audit:sourceTable "zs_POI_dm" ; spatial:name ?name .\n  FILTER(STRLEN(STR(?name)) > 0)\n}}''',
            "evidence_sql": "SELECT gid, MC FROM zs_POI_dm WHERE MC IS NOT NULL AND TRIM(MC) <> '' ORDER BY rowid LIMIT 5",
            "basis": f"POI name field and OSM name-based inclusion are documented in {LOCAL_SOURCE}.",
        },
        {
            "query_id": "FSQ03",
            "task_type": "data_statistic",
            "question": "How many doorplate records have a nonempty house-number value?",
            "sql": "SELECT COUNT(*) AS answer FROM zs_MP_bz WHERE MPH IS NOT NULL AND TRIM(MPH) <> ''",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?row) AS ?answer) WHERE {{\n  ?row audit:sourceTable "zs_MP_bz" ; spatial:houseNumber ?number .\n  FILTER(STRLEN(STR(?number)) > 0)\n}}''',
            "evidence_sql": "SELECT gid, MPH, MPQC FROM zs_MP_bz WHERE MPH IS NOT NULL AND TRIM(MPH) <> '' ORDER BY rowid LIMIT 5",
            "basis": f"Doorplate number field is documented in the frozen schema and {LOCAL_SOURCE}.",
        },
        {
            "query_id": "FSQ04",
            "task_type": "semantic_relation",
            "question": "How many doorplates resolve to a retained town or community parent?",
            "sql": """SELECT COUNT(DISTINCT m.rowid) AS answer
FROM zs_MP_bz AS m
WHERE EXISTS (
  SELECT 1 FROM zs_town_bz AS t
  WHERE m.S_DM || '|' || m.S_MC = t.DM || '|' || t.MC
) OR EXISTS (
  SELECT 1 FROM zs_community_bz AS c
  WHERE m.S_DM || '|' || m.S_MC = c.DM || '|' || c.MC
)""",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?doorplate) AS ?answer) WHERE {{\n  ?doorplate audit:sourceTable "zs_MP_bz" ; spatial:locatedIn ?parent .\n  ?parent audit:sourceTable ?parentTable .\n  FILTER(?parentTable IN ("zs_town_bz", "zs_community_bz"))\n}}''',
            "evidence_sql": "SELECT gid, S_DM, S_MC FROM zs_MP_bz WHERE S_DM IS NOT NULL AND S_MC IS NOT NULL ORDER BY rowid LIMIT 5",
            "basis": f"Doorplate parent administrative references and town/community tables are documented in {LOCAL_SOURCE}; this counts only retained, directly resolved parents.",
        },
        {
            "query_id": "FSQ05",
            "task_type": "semantic_relation",
            "question": "How many POI records resolve to a retained district?",
            "sql": """SELECT COUNT(DISTINCT p.rowid) AS answer
FROM zs_POI_dm AS p JOIN zs_district_bz AS d ON p.QXMC = d.MC""",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?poi) AS ?answer) WHERE {{\n  ?poi audit:sourceTable "zs_POI_dm" ; spatial:locatedIn ?district .\n  ?district audit:sourceTable "zs_district_bz" .\n}}''',
            "evidence_sql": "SELECT p.gid, p.QXMC, d.DM, d.MC FROM zs_POI_dm AS p JOIN zs_district_bz AS d ON p.QXMC = d.MC ORDER BY p.rowid LIMIT 5",
            "basis": f"The source assembly documentation states that POI district assignment is written to QXMC in {LOCAL_SOURCE}; district names are unique in this single-city sample.",
        },
        {
            "query_id": "FSQ06",
            "task_type": "semantic_relation",
            "question": "How many community records resolve to a parent town?",
            "sql": """SELECT COUNT(DISTINCT c.rowid) AS answer
FROM zs_community_bz AS c JOIN zs_town_bz AS t
  ON c.S_DM || '|' || c.S_MC = t.DM || '|' || t.MC""",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?community) AS ?answer) WHERE {{\n  ?community audit:sourceTable "zs_community_bz" ; spatial:parentAdministrativeUnit ?town .\n  ?town audit:sourceTable "zs_town_bz" .\n}}''',
            "evidence_sql": "SELECT c.gid, c.S_DM, c.S_MC, t.DM, t.MC FROM zs_community_bz AS c JOIN zs_town_bz AS t ON c.S_DM || '|' || c.S_MC = t.DM || '|' || t.MC ORDER BY c.rowid LIMIT 5",
            "basis": f"The source dictionary describes community-to-town parent assignment in {LOCAL_SOURCE}; this is source-assigned hierarchy, not independent boundary validation.",
        },
        {
            "query_id": "FSQ07",
            "task_type": "semantic_relation",
            "question": "How many AOI records have a same-source POI projection?",
            "sql": """SELECT COUNT(DISTINCT a.rowid) AS answer
FROM zs_AOI_dm AS a JOIN zs_POI_dm AS p ON a.GUID = p.GUID""",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?aoi) AS ?answer) WHERE {{\n  {{ ?aoi audit:sourceTable "zs_AOI_dm" ; spatial:sameSourceEntity ?poi .\n     ?poi audit:sourceTable "zs_POI_dm" . }}\n  UNION\n  {{ ?poi audit:sourceTable "zs_POI_dm" ; spatial:sameSourceEntity ?aoi .\n     ?aoi audit:sourceTable "zs_AOI_dm" . }}\n}}''',
            "evidence_sql": "SELECT a.gid AS aoi_gid, p.gid AS poi_gid, a.GUID FROM zs_AOI_dm AS a JOIN zs_POI_dm AS p ON a.GUID = p.GUID ORDER BY a.rowid LIMIT 5",
            "basis": f"AOI is documented as a way-based subset of POI and uses type-qualified OSM GUIDs in {LOCAL_SOURCE}; sameSourceEntity is treated as an undirected identity relation for counting.",
        },
        {
            "query_id": "FSQ08",
            "task_type": "semantic_relation_multihop",
            "question": "How many POI records can be followed through a district to the city record?",
            "sql": """SELECT COUNT(DISTINCT p.rowid) AS answer
FROM zs_POI_dm AS p
JOIN zs_district_bz AS d ON p.QXMC = d.MC
JOIN zs_city_bz AS c ON d.S_DM || '|' || d.S_MC = c.DM || '|' || c.MC""",
            "sparql": f'''PREFIX audit: <{AUDIT}>\nPREFIX spatial: <{SPATIAL}>\nSELECT (COUNT(DISTINCT ?poi) AS ?answer) WHERE {{\n  ?poi audit:sourceTable "zs_POI_dm" ; spatial:locatedIn ?district .\n  ?district audit:sourceTable "zs_district_bz" ; spatial:parentAdministrativeUnit ?city .\n  ?city audit:sourceTable "zs_city_bz" .\n}}''',
            "evidence_sql": "SELECT p.gid AS poi_gid, d.DM AS district_code, c.DM AS city_code FROM zs_POI_dm AS p JOIN zs_district_bz AS d ON p.QXMC = d.MC JOIN zs_city_bz AS c ON d.S_DM || '|' || d.S_MC = c.DM || '|' || c.MC ORDER BY p.rowid LIMIT 5",
            "basis": f"POI-to-district assignment and district-to-city parent fields are documented in {LOCAL_SOURCE}; this query checks only source-assigned multi-hop edges.",
        },
    ]


def _readonly(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _run_sql(conn: sqlite3.Connection, sql: str) -> list[dict]:
    if not sql.lstrip().upper().startswith("SELECT"):
        raise ValueError("diagnostic SQL must be a read-only SELECT")
    return [dict(row) for row in conn.execute(sql).fetchall()]


def _scalar(value):
    if hasattr(value, "toPython"):
        value = value.toPython()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _run_sparql(graph: Graph, sparql: str) -> list[dict]:
    result = graph.query(sparql)
    names = [str(item) for item in result.vars]
    return [{name: _scalar(value) for name, value in zip(names, row)} for row in result]


def _term(local: str) -> URIRef:
    return URIRef(SPATIAL + local)


def _graph_indexes(graph: Graph) -> dict:
    """Index the small set of graph patterns used by the eight fixed tasks."""
    table_predicate = URIRef(AUDIT + "sourceTable")
    value_predicates = {_term("name"), _term("houseNumber")}
    relation_predicates = {
        _term("locatedIn"), _term("parentAdministrativeUnit"), _term("sameSourceEntity")
    }
    tables = defaultdict(set)
    values = defaultdict(lambda: defaultdict(set))
    pairs = defaultdict(set)
    for subject, _, obj in graph.triples((None, table_predicate, None)):
        tables[str(obj)].add(subject)
    for predicate in value_predicates:
        for subject, _, obj in graph.triples((None, predicate, None)):
            values[str(predicate)][subject].add(obj)
    for predicate in relation_predicates:
        for subject, _, obj in graph.triples((None, predicate, None)):
            pairs[str(predicate)].add((subject, obj))
    return {"tables": tables, "values": values, "pairs": pairs}


def _run_graph_index(indexes: dict, query_id: str) -> list[dict]:
    tables, values, pairs = indexes["tables"], indexes["values"], indexes["pairs"]
    if query_id == "FSQ01":
        answer = len(tables["zs_street_bz"])
    elif query_id == "FSQ02":
        name_values = values[str(_term("name"))]
        answer = sum(1 for node in tables["zs_POI_dm"]
                     if any(str(value).strip() for value in name_values.get(node, ())))
    elif query_id == "FSQ03":
        house_numbers = values[str(_term("houseNumber"))]
        answer = sum(1 for node in tables["zs_MP_bz"]
                     if any(str(value).strip() for value in house_numbers.get(node, ())))
    elif query_id == "FSQ04":
        located = pairs[str(_term("locatedIn"))]
        parents = tables["zs_town_bz"] | tables["zs_community_bz"]
        answer = len({source for source, target in located
                      if source in tables["zs_MP_bz"] and target in parents})
    elif query_id == "FSQ05":
        located = pairs[str(_term("locatedIn"))]
        answer = len({source for source, target in located
                      if source in tables["zs_POI_dm"] and target in tables["zs_district_bz"]})
    elif query_id == "FSQ06":
        parent_edges = pairs[str(_term("parentAdministrativeUnit"))]
        answer = len({source for source, target in parent_edges
                      if source in tables["zs_community_bz"] and target in tables["zs_town_bz"]})
    elif query_id == "FSQ07":
        identity_edges = pairs[str(_term("sameSourceEntity"))]
        aoi, poi = tables["zs_AOI_dm"], tables["zs_POI_dm"]
        answer = len({first for left, right in identity_edges
                      for first, second in ((left, right), (right, left))
                      if first in aoi and second in poi})
    elif query_id == "FSQ08":
        located = pairs[str(_term("locatedIn"))]
        parents = pairs[str(_term("parentAdministrativeUnit"))]
        districts_with_city = {district for district, city in parents
                               if district in tables["zs_district_bz"] and city in tables["zs_city_bz"]}
        answer = len({poi for poi, district in located
                      if poi in tables["zs_POI_dm"] and district in districts_with_city})
    else:
        raise ValueError(f"unknown fixed query id: {query_id}")
    return [{"answer": answer}]


def _canonical_hash(value) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _compare(sql_rows: list[dict], graph_rows: list[dict]) -> dict:
    sql_answer = _scalar(next(iter(sql_rows[0].values()))) if sql_rows else None
    graph_answer = _scalar(next(iter(graph_rows[0].values()))) if graph_rows else None
    exact = sql_answer == graph_answer
    return {"sql_answer": sql_answer, "graph_answer": graph_answer,
            "exact_match": exact, "matching_rate": 1.0 if exact else 0.0}


def run_diagnostics(db_path: str | Path, ttl_path: str | Path,
                    tasks: list[dict] | None = None) -> dict:
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
                    "query_id": spec["query_id"], "task_type": spec["task_type"],
                    "question": spec["question"], "status": "ok",
                    "sql_result": sql_rows, "graph_result": graph_rows,
                    "graph_execution": "rdflib_fixed_triple_index",
                    "sparql_execution": "not_run_for_full_graph; equivalent pattern fixture-tested",
                    **comparison,
                    "raw_source_values": raw_evidence,
                    "raw_source_values_sha256": _canonical_hash(raw_evidence),
                    "sql_result_sha256": _canonical_hash(sql_rows),
                    "graph_result_sha256": _canonical_hash(graph_rows),
                    "sql_sha256": _canonical_hash(spec["sql"]),
                    "sparql_sha256": _canonical_hash(spec["sparql"]),
                })
            except Exception as exc:
                results.append({"query_id": spec["query_id"], "task_type": spec["task_type"],
                                "question": spec["question"], "status": "error",
                                "exception_type": type(exc).__name__, "exception": str(exc),
                                "sql_sha256": _canonical_hash(spec["sql"]),
                                "sparql_sha256": _canonical_hash(spec["sparql"])})
        successful = [item for item in results if item["status"] == "ok"]
        matches = sum(item["exact_match"] for item in successful)
        return {"status": STATUS, "human_gold": False,
                "database_sha256": file_hash(db_path), "ttl_sha256": file_hash(ttl_path),
                "database": str(db_path), "ttl": str(ttl_path),
                "query_count": len(results), "successful_queries": len(successful),
                "error_queries": len(results) - len(successful), "exact_matches": matches,
                "overall_matching_rate": matches / len(successful) if successful else None,
                "results": results}
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/public/benchmark_v2/foshan_osm/sample.sqlite")
    parser.add_argument("--ttl", type=Path, required=True, help="Materialized Turtle graph to diagnose")
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    result = run_diagnostics(args.db_path, args.ttl)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    tasks = query_specs()
    tasks_path = args.out_dir / "query_tasks.json"
    write_json(tasks_path, {"status": STATUS, "human_gold": False, "dataset": "foshan_osm",
                            "source_document": LOCAL_SOURCE, "query_count": len(tasks),
                            "selection_policy": "Fixed from local dataset documentation and frozen SQLite schema; no method predictions were read.",
                            "graph_execution": "rdflib fixed triple index; SPARQL equivalents are checked on fixtures, not run over the full graph.",
                            "tasks": tasks})
    results_path = args.out_dir / "diagnostic_results.json"
    write_json(results_path, result)
    write_json(args.out_dir / "manifest.json", {
        "status": STATUS, "human_gold": False, "dataset": "foshan_osm",
        "database_sha256": result["database_sha256"], "ttl_sha256": result["ttl_sha256"],
        "query_tasks": str(tasks_path), "query_tasks_sha256": file_hash(tasks_path),
        "results": str(results_path), "results_sha256": file_hash(results_path),
    })
    print(json.dumps({key: result[key] for key in
                      ("status", "human_gold", "query_count", "successful_queries",
                       "error_queries", "exact_matches", "overall_matching_rate")},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
