import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from experiments.public_kg.query_diagnostics import (
    AUDIT,
    SPATIAL,
    _graph_indexes,
    _run_graph_index,
    _run_sparql,
    query_specs,
    run_diagnostics,
)


def node(table, key):
    return URIRef(f"https://w3id.org/rdb2g/record/geonames_cn/{table}/pk/{key}")


class QueryDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "sample.sqlite"
        self.ttl = root / "graph.ttl"
        con = sqlite3.connect(self.db)
        con.executescript("""
            CREATE TABLE gn_admin1 (code TEXT PRIMARY KEY, name TEXT, asciiname TEXT, geonameid INTEGER);
            CREATE TABLE gn_admin2 (code TEXT PRIMARY KEY, name TEXT, asciiname TEXT, geonameid INTEGER);
            CREATE TABLE gn_feature_code (code TEXT PRIMARY KEY, name TEXT, description TEXT);
            CREATE TABLE gn_place (
                geonameid INTEGER PRIMARY KEY, name TEXT, asciiname TEXT, alternatenames TEXT,
                latitude REAL, longitude REAL, feature_class TEXT, feature_code TEXT,
                country_code TEXT, cc2 TEXT, admin1_code TEXT, admin2_code TEXT,
                admin3_code TEXT, admin4_code TEXT, population INTEGER, elevation INTEGER,
                dem INTEGER, timezone TEXT, modification_date TEXT
            );
            INSERT INTO gn_admin1 VALUES ('CN.14','Tibet','Tibet',100);
            INSERT INTO gn_admin2 VALUES ('CN.14.5401','Lhasa','Lhasa',200);
            INSERT INTO gn_feature_code VALUES ('P.PPL','populated place','settlement');
            INSERT INTO gn_place VALUES (1,'Test City','Test City','',29,91,'P','PPL','CN',NULL,'14','5401',NULL,NULL,42,NULL,3000,'Asia/Shanghai','2026-01-01');
        """)
        con.commit()
        con.close()
        self.graph = self._build_graph(include_edges=True)
        self.graph.serialize(self.ttl, format="turtle")

    def tearDown(self):
        self.tmp.cleanup()

    def _build_graph(self, include_edges):
        graph = Graph()
        place, admin1, admin2, feature = (
            node("gn_place", "1"),
            node("gn_admin1", "CN.14"),
            node("gn_admin2", "CN.14.5401"),
            node("gn_feature_code", "P.PPL"),
        )
        for subject, table in (
            (place, "gn_place"),
            (admin1, "gn_admin1"),
            (admin2, "gn_admin2"),
            (feature, "gn_feature_code"),
        ):
            graph.add((subject, URIRef(AUDIT + "sourceTable"), Literal(table)))
        graph.add((place, URIRef(SPATIAL + "population"), Literal(42)))
        graph.add((place, URIRef(SPATIAL + "featureClassCode"), Literal("P")))
        graph.add((feature, URIRef(SPATIAL + "categoryCode"), Literal("P.PPL")))
        graph.add((place, URIRef(SPATIAL + "hasFeatureType"), feature))
        if include_edges:
            graph.add((place, URIRef(SPATIAL + "locatedIn"), admin1))
            graph.add((place, URIRef(SPATIAL + "locatedIn"), admin2))
            graph.add((admin2, URIRef(SPATIAL + "parentAdministrativeUnit"), admin1))
        return graph

    def test_sql_and_graph_queries_agree_on_complete_graph(self):
        result = run_diagnostics(self.db, self.ttl)
        self.assertEqual(result["query_count"], 8)
        self.assertEqual(result["error_queries"], 0)
        self.assertEqual(result["exact_matches"], 8)
        self.assertEqual(result["overall_matching_rate"], 1.0)

    def test_fixed_triple_traversal_matches_rdflib_sparql_on_fixture(self):
        indexes = _graph_indexes(self.graph)
        for task in query_specs():
            with self.subTest(query_id=task["query_id"]):
                self.assertEqual(
                    _run_graph_index(indexes, task["query_id"]),
                    _run_sparql(self.graph, task["sparql"]),
                )

    def test_missing_relation_returns_zero_and_is_reported_as_mismatch(self):
        graph = self._build_graph(include_edges=False)
        graph.serialize(self.ttl, format="turtle")
        result = run_diagnostics(self.db, self.ttl)
        by_id = {item["query_id"]: item for item in result["results"]}
        self.assertEqual(by_id["GNQ05"]["graph_answer"], 0)
        self.assertFalse(by_id["GNQ05"]["exact_match"])
        self.assertEqual(by_id["GNQ06"]["graph_answer"], 0)
        self.assertFalse(by_id["GNQ06"]["exact_match"])
        self.assertEqual(by_id["GNQ08"]["graph_answer"], 0)
        self.assertFalse(by_id["GNQ08"]["exact_match"])

    def test_query_set_is_fixed_and_nonempty(self):
        tasks = query_specs()
        self.assertGreaterEqual(len(tasks), 6)
        self.assertTrue(any("second-level division" in task["question"] for task in tasks))
        self.assertTrue(all(task.get("sql") and task.get("sparql") for task in tasks))


if __name__ == "__main__":
    unittest.main()
