import sqlite3
import tempfile
import unittest
from pathlib import Path

from rdflib import Graph, Literal, URIRef

from experiments.public_kg.foshan_query_diagnostics import (
    AUDIT,
    SPATIAL,
    _graph_indexes,
    _run_graph_index,
    _run_sparql,
    query_specs,
    run_diagnostics,
)


def node(table, key):
    return URIRef(f"https://w3id.org/rdb2g/record/foshan_osm/{table}/pk/{key}")


class FoshanQueryDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "sample.sqlite"
        self.ttl = root / "graph.ttl"
        conn = sqlite3.connect(self.db)
        conn.executescript("""
            CREATE TABLE zs_street_bz (gid TEXT, GUID TEXT, MC TEXT);
            CREATE TABLE zs_POI_dm (gid TEXT, MC TEXT, QXMC TEXT, GUID TEXT);
            CREATE TABLE zs_MP_bz (gid TEXT, MPH TEXT, MPQC TEXT, S_DM TEXT, S_MC TEXT);
            CREATE TABLE zs_town_bz (gid TEXT, DM TEXT, MC TEXT);
            CREATE TABLE zs_community_bz (gid TEXT, DM TEXT, MC TEXT, S_DM TEXT, S_MC TEXT);
            CREATE TABLE zs_district_bz (gid TEXT, DM TEXT, MC TEXT, S_DM TEXT, S_MC TEXT);
            CREATE TABLE zs_city_bz (gid TEXT, DM TEXT, MC TEXT);
            CREATE TABLE zs_AOI_dm (gid TEXT, GUID TEXT);
            INSERT INTO zs_street_bz VALUES ('s1','osm-way-s1','测试路');
            INSERT INTO zs_POI_dm VALUES ('p1','测试学校','测试区','osm-way-10');
            INSERT INTO zs_MP_bz VALUES ('m1','12号','测试路12号','t1','测试镇');
            INSERT INTO zs_town_bz VALUES ('t1','t1','测试镇');
            INSERT INTO zs_community_bz VALUES ('c1','c1','测试社区','t1','测试镇');
            INSERT INTO zs_district_bz VALUES ('d1','d1','测试区','city1','佛山市');
            INSERT INTO zs_city_bz VALUES ('city1','city1','佛山市');
            INSERT INTO zs_AOI_dm VALUES ('a1','osm-way-10');
        """)
        conn.commit()
        conn.close()
        self.graph = self._build_graph(include_relations=True)
        self.graph.serialize(self.ttl, format="turtle")

    def tearDown(self):
        self.tmp.cleanup()

    def _build_graph(self, include_relations):
        graph = Graph()
        rows = {
            "zs_street_bz": [("s1", {"name": "测试路"})],
            "zs_POI_dm": [("p1", {"name": "测试学校"})],
            "zs_MP_bz": [("m1", {"houseNumber": "12号"})],
            "zs_town_bz": [("t1", {})],
            "zs_community_bz": [("c1", {})],
            "zs_district_bz": [("d1", {})],
            "zs_city_bz": [("city1", {})],
            "zs_AOI_dm": [("a1", {})],
        }
        for table, table_rows in rows.items():
            for key, values in table_rows:
                subject = node(table, key)
                graph.add((subject, URIRef(AUDIT + "sourceTable"), Literal(table)))
                for predicate, value in values.items():
                    graph.add((subject, URIRef(SPATIAL + predicate), Literal(value)))
        if include_relations:
            graph.add((node("zs_MP_bz", "m1"), URIRef(SPATIAL + "locatedIn"), node("zs_town_bz", "t1")))
            graph.add((node("zs_POI_dm", "p1"), URIRef(SPATIAL + "locatedIn"), node("zs_district_bz", "d1")))
            graph.add((node("zs_community_bz", "c1"), URIRef(SPATIAL + "parentAdministrativeUnit"), node("zs_town_bz", "t1")))
            # Deliberately reverse the identity triple to verify undirected comparison.
            graph.add((node("zs_POI_dm", "p1"), URIRef(SPATIAL + "sameSourceEntity"), node("zs_AOI_dm", "a1")))
            graph.add((node("zs_district_bz", "d1"), URIRef(SPATIAL + "parentAdministrativeUnit"), node("zs_city_bz", "city1")))
        return graph

    def test_sql_and_graph_queries_agree_on_complete_graph(self):
        result = run_diagnostics(self.db, self.ttl)
        self.assertEqual(result["status"], "machine_reference_diagnostic_only")
        self.assertFalse(result["human_gold"])
        self.assertEqual(result["query_count"], 8)
        self.assertEqual(result["error_queries"], 0)
        self.assertEqual(result["exact_matches"], 8)
        self.assertEqual(result["overall_matching_rate"], 1.0)

    def test_fixed_triple_traversal_matches_rdflib_sparql_on_fixture(self):
        indexes = _graph_indexes(self.graph)
        for task in query_specs():
            with self.subTest(query_id=task["query_id"]):
                self.assertEqual(_run_graph_index(indexes, task["query_id"]),
                                 _run_sparql(self.graph, task["sparql"]))

    def test_missing_relation_edges_return_zero_and_fail_comparison(self):
        graph = self._build_graph(include_relations=False)
        graph.serialize(self.ttl, format="turtle")
        result = run_diagnostics(self.db, self.ttl)
        by_id = {item["query_id"]: item for item in result["results"]}
        for query_id in ("FSQ04", "FSQ05", "FSQ06", "FSQ07", "FSQ08"):
            with self.subTest(query_id=query_id):
                self.assertEqual(by_id[query_id]["graph_answer"], 0)
                self.assertFalse(by_id[query_id]["exact_match"])

    def test_task_set_has_data_and_semantic_relation_questions(self):
        tasks = query_specs()
        self.assertEqual(len(tasks), 8)
        self.assertTrue(any(task["task_type"] == "data_statistic" for task in tasks))
        self.assertTrue(any(task["task_type"] == "semantic_relation_multihop" for task in tasks))
        self.assertTrue(all(task.get("sql") and task.get("sparql") for task in tasks))


if __name__ == "__main__":
    unittest.main()
