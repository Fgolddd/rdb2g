"""Offline regression checks for RDF materialization and gold validation."""

import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD

from experiments.public_kg.artifacts import materialize, score_mapping


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.dataset_dir = self.root / "data set"
        self.dataset_dir.mkdir()
        self.db = self.dataset_dir / "sample.sqlite"
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE records (id TEXT PRIMARY KEY, geom BLOB)")
            conn.execute("CREATE TABLE rowid_records (id TEXT, geom BLOB)")
            conn.execute("INSERT INTO records VALUES (?, ?)", ("a/b %ü", bytes.fromhex("00ff")))
            conn.executemany("INSERT INTO rowid_records VALUES (?, ?)", [
                ("", bytes.fromhex("01")), ("1", bytes.fromhex("02")),
                ("1", bytes.fromhex("03")), (None, bytes.fromhex("04")),
            ])

        self.ontology = self.root / "ontology.json"
        self.ontology.write_text(json.dumps({"terms": [
            {"uri": "urn:Place", "type": "Class"},
            {"uri": "urn:geometry", "type": "Property", "range": "Literal"},
        ]}))
        self.predictions = self.root / "predictions.json"
        self.predictions.write_text(json.dumps({
            "run_status": "success",
            "table_mappings": [{"table": "records", "target_uri": "urn:Place"}],
            "mappings": [{"table": "records", "column": "geom", "target_uri": "urn:geometry"},
                         {"table": "rowid_records", "column": "geom", "target_uri": "urn:geometry"}],
            "relation_rules": [],
        }))

    def tearDown(self):
        self.temp.cleanup()

    def test_materializer_preserves_binary_value_and_escapes_record_uri(self):
        output = self.root / "graph"
        result = materialize(self.db, self.predictions, self.ontology, output)
        self.assertEqual(result["status"], "success")

        graph = Graph().parse(output / "graph.ttl", format="turtle")
        node = URIRef("https://w3id.org/rdb2g/record/data%20set/records/pk/a%2Fb%20%25%C3%BC")
        value = graph.value(node, URIRef("urn:geometry"))
        self.assertEqual(value, Literal("00ff", datatype=XSD.hexBinary))
        self.assertEqual(str(value), "00ff")
        self.assertEqual(graph.value(node, URIRef("https://w3id.org/rdb2g/audit/sourceKey")),
                         Literal("a/b %ü"))
        # A nullable/non-unique key must use row identity. Otherwise the empty
        # string, duplicate values, and NULL-derived offset can collide.
        row_nodes = {subject for subject in graph.subjects(URIRef("urn:geometry"), None)
                     if "/rowid_records/row/" in str(subject)}
        self.assertEqual(len(row_nodes), 4)
        self.assertEqual(len({graph.value(node, URIRef("https://w3id.org/rdb2g/audit/sourceKey"))
                              for node in row_nodes}), 4)

    def write_gold(self, row):
        path = self.root / "gold.csv"
        fields = ["table", "column", "accepted_uris_json", "no_match", "review_status",
                  "reviewer", "evidence"]
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerow(row)
        return path

    def test_score_mapping_waits_for_review_and_scores_approved_label(self):
        prediction = {"mappings": [{"table": "records", "column": "geom",
                                     "target_uri": "urn:geometry"}]}
        row = {"table": "records", "column": "geom", "accepted_uris_json": '["urn:geometry"]',
               "no_match": "no", "review_status": "approved", "reviewer": "reviewer-1",
               "evidence": "checked against source documentation"}
        path = self.write_gold(row)
        self.assertEqual(score_mapping(prediction, path)["accuracy"], 1.0)
        row["review_status"] = "pending"
        self.write_gold(row)
        self.assertEqual(score_mapping(prediction, path)["status"], "pending_human_gold")

    def test_score_mapping_rejects_malformed_boolean_and_empty_uri(self):
        prediction = {"mappings": [{"table": "records", "column": "geom",
                                     "target_uri": "urn:geometry"}]}
        base = {"table": "records", "column": "geom", "accepted_uris_json": '["urn:geometry"]',
                "no_match": "maybe", "review_status": "approved", "reviewer": "reviewer-1",
                "evidence": "reviewed"}
        with self.assertRaisesRegex(ValueError, "no_match"):
            score_mapping(prediction, self.write_gold(base))
        base["no_match"] = "no"
        base["accepted_uris_json"] = '[""]'
        with self.assertRaisesRegex(ValueError, "nonempty URIs"):
            score_mapping(prediction, self.write_gold(base))


if __name__ == "__main__":
    unittest.main()
