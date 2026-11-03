import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from experiments.public_kg.machine_diagnostics import (
    _validate_reference_provenance, build_relation_reference, mapping_diagnostics,
    edge_diagnostics,
)
from experiments.public_kg.artifacts import file_hash


class MachineDiagnosticTests(unittest.TestCase):
    def test_second_reference_manifest_uses_source_database_hash_and_verifies_all_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "sample.sqlite"
            with sqlite3.connect(db) as conn:
                conn.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY)")
            ontology = root / "ontology.json"
            ontology.write_text(json.dumps({"terms": []}))
            mapping = root / "mapping_reference.csv"
            mapping.write_text("table,column\nsample,id\n")
            relation = root / "relation_reference.json"
            relation.write_text(json.dumps({"rules": []}))
            manifest = root / "manifest.json"
            expected = {
                "status": "second_machine_reviewed_reference",
                "human_reviewed": False,
                "dataset": "sample",
                "source_database_sha256": file_hash(db),
                "ontology_sha256": file_hash(ontology),
                "mapping_reference_sha256": file_hash(mapping),
                "relation_reference_sha256": file_hash(relation),
            }
            manifest.write_text(json.dumps(expected))

            provenance = _validate_reference_provenance(
                manifest, "second", "sample", db, ontology, mapping, relation)
            self.assertEqual(provenance["source_database_sha256"], file_hash(db))

            for field in ("source_database_sha256", "ontology_sha256",
                          "mapping_reference_sha256", "relation_reference_sha256"):
                payload = dict(expected)
                payload[field] = "0" * 64
                manifest.write_text(json.dumps(payload))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    _validate_reference_provenance(
                        manifest, "second", "sample", db, ontology, mapping, relation)
            payload = dict(expected)
            payload.pop("source_database_sha256")
            payload["database_sha256"] = file_hash(db)
            manifest.write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "source_database_sha256"):
                _validate_reference_provenance(
                    manifest, "second", "sample", db, ontology, mapping, relation)

    def test_unknown_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'reference.csv'
            fields = ['table', 'column', 'accepted_uris_json', 'no_match', 'review_status', 'evidence', 'include_in_diagnostic']
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for name, include in [('a', 'true'), ('b', 'false')]:
                    writer.writerow(dict(table='t', column=name, accepted_uris_json='["urn:correct"]',
                        no_match='false', review_status='machine_reviewed', evidence='documentation',
                        include_in_diagnostic=include))
            score = mapping_diagnostics({'run_status': 'success', 'mappings': [
                {'table': 't', 'column': 'a', 'target_uri': 'urn:wrong'},
                {'table': 't', 'column': 'b', 'target_uri': None}]}, path)
            self.assertFalse(score['human_gold'])
            self.assertEqual(score['items'], 1)
            self.assertEqual(score['excluded_items'], 1)
            self.assertEqual(score['f1'], 0)

    def test_identity_direction_and_unassessed_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            predicted = Path(directory) / 'pred.jsonl'
            reference = Path(directory) / 'ref.jsonl'
            uri = 'https://w3id.org/rdb2g/public-spatial/sameSourceEntity'
            a, b = 'https://example/record/d/a/pk/1', 'https://example/record/d/b/pk/1'
            reference.write_text(json.dumps(dict(head=a, tail=b, predicate=uri)) + chr(10))
            predicted.write_text(json.dumps(dict(head=b, tail=a, predicate=uri)) + chr(10) +
                json.dumps(dict(head=a, tail='https://example/record/d/c/pk/1', predicate=uri)) + chr(10))
            score = edge_diagnostics(predicted, reference, [dict(source_table='a', target_table='b', predicate=uri)])
            self.assertEqual(score['f1'], 1)
            self.assertEqual(score['unassessed_predictions'], 1)

    def test_reversed_directed_edge_is_assessed_as_error(self):
        with tempfile.TemporaryDirectory() as directory:
            predicted = Path(directory) / 'pred.jsonl'
            reference = Path(directory) / 'ref.jsonl'
            uri = 'https://w3id.org/rdb2g/public-spatial/locatedIn'
            a, b = 'https://example/record/d/a/pk/1', 'https://example/record/d/b/pk/1'
            reference.write_text(json.dumps(dict(head=a, tail=b, predicate=uri)) + chr(10))
            predicted.write_text(json.dumps(dict(head=b, tail=a, predicate=uri)) + chr(10))
            score = edge_diagnostics(predicted, reference, [dict(source_table='a', target_table='b', predicate=uri)])
            self.assertEqual(score['fp'], 1)
            self.assertEqual(score['fn'], 1)
            self.assertEqual(score['unassessed_predictions'], 0)

    def test_unknown_reference_exclusion_is_removed_from_both_sides(self):
        with tempfile.TemporaryDirectory() as directory:
            predicted = Path(directory) / 'pred.jsonl'
            reference = Path(directory) / 'ref.jsonl'
            excluded = Path(directory) / 'excluded.jsonl'
            uri = 'https://w3id.org/rdb2g/public-spatial/locatedIn'
            a, b = 'https://example/record/d/a/pk/1', 'https://example/record/d/b/pk/1'
            x, y = 'https://example/record/d/a/pk/2', 'https://example/record/d/b/pk/2'
            reference.write_text(json.dumps(dict(head=a, tail=b, predicate=uri)) + chr(10))
            excluded.write_text(json.dumps(dict(head=x, tail=y, predicate=uri)) + chr(10))
            predicted.write_text(json.dumps(dict(head=a, tail=b, predicate=uri)) + chr(10) +
                                 json.dumps(dict(head=y, tail=x, predicate=uri)) + chr(10))
            score = edge_diagnostics(predicted, reference,
                [dict(source_table='a', target_table='b', predicate=uri)], excluded)
            self.assertEqual(score['reference_edges'], 1)
            self.assertEqual(score['excluded_reference_edges'], 1)
            self.assertEqual(score['excluded_predictions'], 1)
            self.assertEqual(score['tp'], 1)
            self.assertEqual(score['fp'], 0)
            self.assertEqual(score['fn'], 0)
            self.assertEqual(score['f1'], 1)

    def test_reference_builder_filters_only_declared_unknown_edges(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / 'sample.sqlite'
            conn = sqlite3.connect(db)
            conn.executescript('''
                CREATE TABLE gn_place (geonameid INTEGER PRIMARY KEY, admin_code TEXT);
                CREATE TABLE gn_admin1 (code TEXT PRIMARY KEY, geonameid INTEGER);
                INSERT INTO gn_place VALUES (1, 'CN.01');
                INSERT INTO gn_place VALUES (2, 'CN.02');
                INSERT INTO gn_admin1 VALUES ('CN.01', 1);
                INSERT INTO gn_admin1 VALUES ('CN.02', 20);
            ''')
            conn.commit()
            conn.close()
            ontology = root / 'ontology.json'
            predicate = 'https://example/locatedIn'
            ontology.write_text(json.dumps({'terms': [
                {'uri': predicate, 'type': 'Property', 'property_kind': 'ObjectProperty', 'range': 'Place'}
            ]}))
            reference = root / 'relation_reference.json'
            reference.write_text(json.dumps({
                'status': 'machine_reviewed_reference', 'human_reviewed': False,
                'rules': [{'source_table': 'gn_place', 'target_table': 'gn_admin1',
                           'source_expr': {'op': 'column', 'name': 'admin_code'},
                           'target_expr': {'op': 'column', 'name': 'code'},
                           'predicate': predicate, 'direction': 'forward'}],
                'edge_exclusions': [{'rule_index': 0, 'predicate': predicate,
                                     'condition': 'source_column_equals_target_column',
                                     'source_column': 'geonameid', 'target_column': 'geonameid',
                                     'status': 'unknown_edge_excluded_from_scoring',
                                     'count_in_frozen_sample': 1}],
            }))
            edges = build_relation_reference(db, ontology, reference, root / 'out')
            excluded = edges.parent / 'excluded_edge_evidence.jsonl'
            self.assertEqual(len(edges.read_text().splitlines()), 1)
            self.assertEqual(len(excluded.read_text().splitlines()), 1)
            manifest = json.loads((edges.parent / 'manifest.json').read_text())
            self.assertEqual(manifest['excluded_edge_count'], 1)
            self.assertEqual(manifest['retained_reference_edge_count'], 1)


if __name__ == '__main__':
    unittest.main()
