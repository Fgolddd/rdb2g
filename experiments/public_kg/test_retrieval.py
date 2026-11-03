"""Offline tests for public vector retrieval; no API key or network required."""

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from experiments.public_kg.retrieval import build_candidates, build_schema_queries


class FakeEmbedder:
    model = "fake-offline-embedding"

    def __init__(self):
        self.requests = 0
        self.errors = 0
        self.tokens = {"input": 0, "output": 0, "total": 0, "reported_responses": 0}
        self.batches = []

    def embed_documents(self, texts):
        self.batches.append(len(texts))
        self.requests += 1
        self.tokens["input"] += sum(len(text.split()) for text in texts)
        self.tokens["total"] = self.tokens["input"]
        return [[float(len(text)), float(sum(map(ord, text)) % 101), 1.0] for text in texts]

    def telemetry(self):
        return {"requests": self.requests, "errors": self.errors,
                "token_usage": dict(self.tokens), "elapsed_seconds": 0.0}


class FakeStore:
    def __init__(self, docs):
        self.docs = {item["uri"]: item for item in docs}
        self.queries = []

    def similarity_search_by_vector(self, vector, k, filter=None):
        self.queries.append((vector, k, filter))
        allowed_type = filter["type"]
        return [SimpleNamespace(metadata={"uri": item["uri"], "type": item["type"]})
                for item in self.docs.values() if item["type"] == allowed_type][:k]


class RetrievalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "source" / "sample.sqlite"
        self.db.parent.mkdir()
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                CREATE TABLE places(place_id INTEGER, display_name TEXT, latitude REAL);
                INSERT INTO places VALUES(1, 'Old Town', 23.1),
                                         (2, 'West Park', NULL),
                                         (3, 'River Gate', 23.3);
                CREATE TABLE admin(code TEXT PRIMARY KEY, name TEXT);
                INSERT INTO admin VALUES('CN.1', 'Example');
            """)
        self.ontology = self.root / "target_terms.json"
        terms = [{"uri": f"urn:target:{index}",
                  "type": "Class" if index < 9 else "Property", "label": f"term{index}",
                  "comment": f"Meaning {index}", "range": "Literal"} for index in range(45)]
        self.ontology.write_text(json.dumps({"status": "draft_for_review", "terms": terms}),
                                 encoding="utf-8")
        self.store = FakeStore([])
        self.embedder = FakeEmbedder()

    def tearDown(self):
        self.temp.cleanup()

    def test_queries_include_schema_context_and_nullability(self):
        from experiments.public_kg.methods import schema_profile
        tasks = build_schema_queries(schema_profile(self.db))
        self.assertEqual(len(tasks), 7)  # two class queries plus five column queries
        latitude = next(item for item in tasks if item["column"] == "latitude")
        self.assertIn("type REAL", latitude["text"])
        self.assertIn("non-null rate 0.6667", latitude["text"])
        self.assertIn("23.1", latitude["text"])
        table = next(item for item in tasks if item["table"] == "places" and item["column"] is None)
        self.assertIn("display_name", table["text"])

    def test_batch_embeddings_and_chroma_vector_results_are_serialized(self):
        out = self.root / "eval" / "source"

        def store_factory(index_dir, docs, ids, vectors):
            self.assertEqual(len(docs), 45)
            self.assertEqual(len(ids), len(vectors))
            self.store = FakeStore(docs)
            return self.store

        payload = build_candidates(self.db, self.ontology, out, embedder=self.embedder,
                                   store_factory=store_factory, progress=lambda _: None)
        self.assertEqual(payload["metadata"]["status"], "success")
        self.assertEqual(payload["metadata"]["query_count"], 7)
        self.assertEqual(payload["metadata"]["ontology_sha256"],
                         hashlib.sha256(self.ontology.read_bytes()).hexdigest())
        self.assertEqual(payload["metadata"]["embedding_model"], "fake-offline-embedding")
        self.assertEqual(payload["metadata"]["result_type_filter"],
                         "Class for table tasks, Property for column tasks")
        self.assertTrue(payload["metadata"]["candidate_sha256"])
        self.assertTrue(all(batch <= 10 for batch in self.embedder.batches))
        self.assertEqual(len(self.store.queries), 7)
        self.assertTrue(all(k == 5 for _, k, _ in self.store.queries))
        self.assertTrue(all(len(item["candidate_uris"]) == 5 for item in payload["candidates"]))
        for candidate in payload["candidates"]:
            allowed = "Class" if candidate["column"] is None else "Property"
            self.assertTrue(all(self.store.docs[uri]["type"] == allowed
                                for uri in candidate["candidate_uris"]))
        self.assertEqual(payload["candidates"][0]["column"], None)
        self.assertEqual(json.loads((out / "candidates.json").read_text()), payload)


if __name__ == "__main__":
    unittest.main()
