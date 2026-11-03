"""Focused tests for the transparent relation component baselines."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from experiments.public_kg.artifacts import materialize
from experiments.public_kg.methods import schema_profile
from experiments.public_kg.relation_component_baselines import (
    _class_hierarchy,
    _fk_candidates,
    _ind_candidates,
    _terms,
)


ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY = ROOT / "data/public/benchmark_v2/ontology/target_terms.json"
PLACE = "https://w3id.org/rdb2g/public-spatial/Place"
ADMIN = "https://w3id.org/rdb2g/public-spatial/AdministrativeUnit"


class RelationComponentBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "component.sqlite"
        self.terms = _terms(ONTOLOGY)
        self.is_a = _class_hierarchy(ONTOLOGY)

    def tearDown(self):
        self.temp.cleanup()

    def _create_db(self, sql):
        with sqlite3.connect(self.db) as conn:
            conn.executescript(sql)

    def test_declared_empty_foreign_key_remains_a_rule_and_materializes_zero_edges(self):
        self._create_db("""
            CREATE TABLE admin (id TEXT PRIMARY KEY);
            CREATE TABLE place (admin_id TEXT REFERENCES admin(id));
        """)
        profile = schema_profile(self.db)
        classes = {"place": PLACE, "admin": ADMIN}
        rules, detail = _fk_candidates(self.db, profile, classes, self.terms, self.is_a)

        self.assertEqual(detail["skipped_composite_foreign_keys"], 0)
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["source_table"], "place")
        self.assertEqual(rules[0]["target_table"], "admin")
        self.assertTrue(rules[0]["predicate"].endswith("/locatedIn"))

        prediction = self.root / "predictions.json"
        output = self.root / "graph"
        import json
        prediction.write_text(json.dumps({
            "run_status": "success",
            "table_mappings": [
                {"table": "place", "target_uri": PLACE},
                {"table": "admin", "target_uri": ADMIN}],
            "mappings": [
                {"table": "place", "column": "admin_id", "target_uri": None},
                {"table": "admin", "column": "id", "target_uri": None}],
            "relation_rules": rules,
        }), encoding="utf-8")
        result = materialize(self.db, prediction, ONTOLOGY, output)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rules"], 1)
        self.assertEqual(result["unique_relation_edges"], 0)

    def test_composite_foreign_key_is_reported_as_missed(self):
        self._create_db("""
            CREATE TABLE admin (code_a TEXT, code_b TEXT, PRIMARY KEY (code_a, code_b));
            CREATE TABLE place (admin_a TEXT, admin_b TEXT,
                FOREIGN KEY (admin_a, admin_b) REFERENCES admin(code_a, code_b));
        """)
        rules, detail = _fk_candidates(
            self.db, schema_profile(self.db), {"place": PLACE, "admin": ADMIN},
            self.terms, self.is_a)

        self.assertEqual(rules, [])
        self.assertEqual(detail["skipped_composite_foreign_keys"], 1)

    def test_single_column_ind_uses_exact_values_and_minimum_distinct_counts(self):
        self._create_db("""
            CREATE TABLE admin (id TEXT);
            CREATE TABLE place (admin_code TEXT, empty_code TEXT, constant_code TEXT);
            INSERT INTO admin VALUES ('a'), ('b'), ('c');
            INSERT INTO place VALUES ('a', NULL, 'x'), ('b', NULL, 'x'), ('c', NULL, 'x');
        """)
        rules, evidence = _ind_candidates(
            self.db, schema_profile(self.db), {"place": PLACE, "admin": ADMIN},
            self.terms, self.is_a)

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["source_expr"]["name"], "admin_code")
        self.assertEqual(rules[0]["target_expr"]["name"], "id")
        self.assertAlmostEqual(evidence[0]["source_hit_rate"], 1.0)
        self.assertEqual(evidence[0]["value_comparison"], "exact SQLite-returned values")


if __name__ == "__main__":
    unittest.main()
