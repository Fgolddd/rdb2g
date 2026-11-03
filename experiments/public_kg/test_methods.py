"""Offline adapter tests; no credentials or network calls are required."""

import json
import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from experiments.public_kg.methods import (
    _direction_decision, _key_hypotheses, _normalize_state, evaluate_expression,
    probe_relation, run_method, schema_profile,
    validate_expression,
)


class FakeClient:
    def __init__(self, outputs):
        self.outputs = iter(outputs)
        self.calls = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, **kwargs):
        self.calls.append(kwargs)
        output = next(self.outputs)
        if isinstance(output, Exception):
            raise output
        return SimpleNamespace(
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=20, total_tokens=30),
            choices=[SimpleNamespace(message=SimpleNamespace(content=(
                output if isinstance(output, str) else json.dumps(output))))])


class MethodTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "frozen.sqlite"
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                CREATE TABLE child(id INTEGER PRIMARY KEY, parent TEXT, name TEXT);
                CREATE TABLE parent(code TEXT, name TEXT);
                INSERT INTO child VALUES(1,'a','Alice'),(2,NULL,'Bob'),(3,'b','Carol');
                INSERT INTO parent VALUES('a','A'),('b','B');
            """)
        self.ontology = self.root / "ontology.json"
        self.ontology.write_text(json.dumps({"terms": [
            {"uri": "urn:Place", "label": "Place", "type": "Class"},
            {"uri": "urn:AdminUnit", "label": "AdministrativeUnit", "type": "Class"},
            {"uri": "urn:name", "label": "name", "type": "Property", "range": "Literal"},
            {"uri": "urn:parent", "label": "locatedIn", "type": "Property",
             "domain": "Place", "range": "AdministrativeUnit"},
        ]}))
        self.rule = {"source_table": "child", "target_table": "parent",
                     "source_expr": {"op": "column", "name": "parent"},
                     "target_expr": {"op": "column", "name": "code"},
                     "predicate": "urn:parent", "direction": "forward"}
        self.state = {"table_mappings": [{"table": "child", "target_uri": "urn:Place"},
                                         {"table": "parent", "target_uri": "urn:AdminUnit"}],
                      "mappings": [{"table": "child", "column": "name", "target_uri": "urn:name"}],
                      "relation_rules": [self.rule], "done": True}

    def tearDown(self):
        self.temp.cleanup()

    def run_fake(self, method="full", states=None, **kwargs):
        client = FakeClient(states or [self.state] * 12)
        result = run_method(self.db, self.ontology, method, self.root / method,
                            client=client, **kwargs)
        return result, client

    def test_probe_uses_full_values_and_null_propagation(self):
        profile = schema_profile(self.db)
        self.assertEqual(profile["child"]["rows"], 3)
        column = next(item for item in profile["child"]["columns"] if item["name"] == "parent")
        self.assertEqual(column["nonnull_count"], 2)
        self.assertEqual(column["distinct_count"], 2)
        self.assertAlmostEqual(column["nonnull_rate"], 2 / 3)
        evidence = probe_relation(self.db, self.rule, profile)
        self.assertEqual(evidence["matched_pairs"], 2)
        self.assertEqual(evidence["source_nonnull"], 2)
        self.assertEqual(evidence["target_uniqueness"], 1)
        expr = {"op": "concat", "args": [{"op": "literal", "value": "x:"},
                                            {"op": "column", "name": "parent"}]}
        validate_expression(expr, {"parent"})
        self.assertIsNone(evaluate_expression(expr, {"parent": None}))
        self.assertEqual(evaluate_expression(expr, {"parent": "a"}), "x:a")

    def test_direction_uses_ontology_classes_and_leaves_parent_ambiguous_without_data(self):
        terms = json.loads(self.ontology.read_text())["terms"]
        table_mappings = [{"table": "child", "target_uri": "urn:Place"},
                          {"table": "parent", "target_uri": "urn:AdminUnit"}]
        direction = _direction_decision(self.rule | {"direction": "reverse"},
                                        table_mappings, terms)
        self.assertEqual(direction["expected_direction"], "forward")
        self.assertIn("declared domain/range", direction["basis"][0])

        parent_term = {"uri": "urn:parent", "label": "parentAdministrativeUnit",
                       "comment": "Administrative unit has this immediate administrative parent.",
                       "type": "Property", "range": "AdministrativeUnit"}
        parent_rule = {**self.rule, "source_table": "parent", "target_table": "parent",
                       "predicate": "urn:parent", "direction": "reverse"}
        same_class = [{"table": "parent", "target_uri": "urn:AdminUnit"}]
        ambiguous = _direction_decision(parent_rule, same_class, [*terms, parent_term])
        self.assertEqual(ambiguous["status"], "ambiguous")
        evidenced = _direction_decision(parent_rule, same_class, [*terms, parent_term], {
            "matched_pairs": 5, "source_uniqueness": 0.2, "target_uniqueness": 1.0})
        self.assertEqual(evidenced["expected_direction"], "forward")
        self.assertIn("many-to-one", " ".join(evidenced["basis"]))

    def test_composite_key_hypotheses_use_observed_fragments(self):
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                CREATE TABLE composite_source(part_a TEXT, part_b TEXT);
                CREATE TABLE composite_target(composite_code TEXT);
                INSERT INTO composite_source VALUES('01','A'),('02','B');
                INSERT INTO composite_target VALUES('CN.01.A'),('CN.02.B');
            """)
        profile = schema_profile(self.db)
        rule = {"source_table": "composite_source", "target_table": "composite_target",
                "source_expr": {"op": "column", "name": "part_a"},
                "target_expr": {"op": "column", "name": "composite_code"},
                "predicate": "urn:parent", "direction": "forward"}
        hypotheses = _key_hypotheses(rule, profile, {"target_key_samples": [
            "CN.01.A", "CN.02.B"], "source_key_samples": ["01", "02"]})
        expressions = [item["source_expr"] for item in hypotheses if item["side"] == "source"]
        self.assertTrue(any(evaluate_expression(expr, {"part_a": "01", "part_b": "A"})
                            == "CN.01.A" for expr in expressions))
        self.assertTrue(all(item["requires_exact_probe"] for item in hypotheses))

    def test_unsafe_ast_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_expression({"op": "sql", "value": "DROP TABLE child"}, {"parent"})
        with self.assertRaises(ValueError):
            validate_expression({"op": "column", "name": "absent"}, {"parent"})
        bad = {**self.rule, "source_table": 'child";DROP TABLE child;--'}
        with self.assertRaises(ValueError):
            probe_relation(self.db, bad)

    def test_full_has_three_real_roles_and_complete_abstentions(self):
        result, client = self.run_fake()
        self.assertEqual(result["run_status"], "success")
        self.assertEqual(result["usage"]["total_tokens"], 90)
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(result["relation_rules"][0]["direction"], "forward")
        self.assertEqual(len(result["mappings"]), 5)
        self.assertIsNone(next(item["target_uri"] for item in result["mappings"]
                               if item["table"] == "child" and item["column"] == "id"))
        calls = client.calls
        self.assertIn("mapping specialist", calls[0]["messages"][0]["content"])
        self.assertIn("relation specialist", calls[1]["messages"][0]["content"])
        self.assertIn("independent validator", calls[2]["messages"][0]["content"])
        self.assertIn("matched_pairs", calls[1]["messages"][1]["content"])
        trace = [json.loads(line) for line in Path(result["agenttrace"]).read_text().splitlines()]
        self.assertEqual([row["role"] for row in trace if row["event"] == "llm_attempt"],
                         ["mapping", "relation", "validator"])

    def test_single_agent_is_iterative_and_receives_same_probe(self):
        result, client = self.run_fake("single_agent")
        self.assertEqual(result["run_status"], "success")
        self.assertEqual(len(client.calls), 3)
        self.assertGreater(len(client.calls[2]["messages"]), len(client.calls[0]["messages"]))
        self.assertIn("matched_pairs", client.calls[1]["messages"][-1]["content"])
        self.assertTrue(all("sole agent" in call["messages"][0]["content"] for call in client.calls))

    def test_without_probe_ablation_never_reads_values_or_requests_feedback(self):
        result, client = self.run_fake("without_probe")
        self.assertEqual(result["run_status"], "success")
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(result["tool_usage"], {"probe_requests": 0, "probe_executions": 0,
                                                "probe_cache_hits": 0, "probe_rows_read": 0})
        self.assertFalse(result["probe_gate"])
        self.assertEqual(result["relation_rules"], [self.rule])
        self.assertEqual(json.loads(client.calls[0]["messages"][1]["content"])["schema"],
                         schema_profile(self.db))
        self.assertIn("no-probe ablation", client.calls[0]["messages"][0]["content"])
        trace = [json.loads(line) for line in Path(result["agenttrace"]).read_text().splitlines()]
        self.assertFalse(any(row["event"] == "value_probe" for row in trace))
        attempt = next(row for row in trace if row["event"] == "llm_attempt")
        self.assertEqual(attempt["probe_feedback"], [])
        self.assertEqual(attempt["probe_feedback_status"], "disabled_by_method")

    def test_without_probe_rejects_probe_requests_without_using_probe_tool(self):
        state = {**self.state, "probe_requests": [self.rule]}
        result, client = self.run_fake("without_probe", states=[state], max_calls=1,
                                       max_retries=0)
        self.assertEqual(result["run_status"], "retry_limit_exceeded")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(result["tool_usage"]["probe_executions"], 0)

    def test_budget_stops_without_fallback_or_call(self):
        result, client = self.run_fake(token_budget=1)
        self.assertEqual(result["run_status"], "token_budget_exceeded")
        self.assertEqual(client.calls, [])
        self.assertIsNone(result["fallback"])

    def test_retries_are_counted_and_never_become_fallback_success(self):
        result, client = self.run_fake(states=["invalid"] * 3)
        self.assertEqual(result["run_status"], "retry_limit_exceeded")
        self.assertEqual(result["usage"]["failed_attempts"], 3)
        self.assertEqual(result["usage"]["total_tokens"], 90)
        self.assertEqual(len(client.calls), 3)

    def test_invented_uri_rejected(self):
        bad = {**self.state, "mappings": [{"table": "child", "column": "id",
                                           "target_uri": "urn:gold:child.id"}]}
        result, _ = self.run_fake(states=[bad] * 3)
        self.assertEqual(result["run_status"], "retry_limit_exceeded")

    def test_class_cannot_be_a_column_property(self):
        bad = {**self.state, "mappings": [{"table": "child", "column": "name",
                                           "target_uri": "urn:Place"}]}
        result, _ = self.run_fake(states=[bad] * 3)
        self.assertEqual(result["run_status"], "retry_limit_exceeded")

    def test_object_property_cannot_be_mapped_to_a_column(self):
        bad = {**self.state, "mappings": [{"table": "child", "column": "parent",
                                           "target_uri": "urn:parent"}]}
        result, _ = self.run_fake(states=[bad] * 3)
        self.assertEqual(result["run_status"], "retry_limit_exceeded")
        events = [json.loads(line) for line in Path(result["agenttrace"]).read_text().splitlines()]
        failures = [event for event in events if event.get("event") == "llm_attempt"]
        self.assertIn("literal-valued property", failures[0]["error"])

    def test_semantic_constraints_match_across_full_and_single_and_are_traced(self):
        full, full_client = self.run_fake("full")
        single, single_client = self.run_fake("single_agent")
        full_context = json.loads(full_client.calls[0]["messages"][1]["content"])
        single_context = json.loads(single_client.calls[0]["messages"][1]["content"])
        self.assertEqual(full_context["semantic_constraints"],
                         single_context["semantic_constraints"])
        for result in (full, single):
            events = [json.loads(line) for line in Path(result["agenttrace"]).read_text().splitlines()]
            contract = next(event for event in events if event["event"] == "semantic_constraints")
            self.assertEqual(contract["source"], "ontology range declarations")
            self.assertIn("urn:parent", {item["uri"] for item in
                                         contract["constraints"]["object_properties"]})

    def test_no_premature_success_at_call_limit(self):
        result, client = self.run_fake(max_calls=1)
        self.assertEqual(result["run_status"], "max_calls_exceeded")
        self.assertEqual(len(client.calls), 1)

    def test_nonunique_targets_fail_configurable_gate(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO parent VALUES('a','A2')")
        result, _ = self.run_fake(min_target_uniqueness=0.9)
        self.assertEqual(result["relation_rules"], [])

    def test_probe_gate_ablation_keeps_same_probe_access(self):
        rule = {**self.rule, "target_expr": {"op": "literal", "value": "no match"}}
        state = {**self.state, "relation_rules": [rule]}
        full, _ = self.run_fake(states=[state] * 3)
        ablation, client = self.run_fake("w/oProbeGate", states=[state] * 3)
        self.assertEqual(full["relation_rules"], [])
        self.assertEqual(len(ablation["relation_rules"]), 1)
        self.assertIn("matched_pairs", client.calls[1]["messages"][1]["content"])

    def test_without_validator_is_not_validator_under_new_name(self):
        result, client = self.run_fake("w/oValidator")
        self.assertEqual(result["run_status"], "success")
        self.assertEqual(len(client.calls), 4)
        self.assertFalse(any("independent validator" in call["messages"][0]["content"]
                             for call in client.calls))

    def test_deterministic_label_baseline_has_no_llm(self):
        result, client = self.run_fake("deterministic-only")
        self.assertEqual(result["run_status"], "success")
        self.assertEqual(client.calls, [])
        names = [item for item in result["mappings"] if item["column"] == "name"]
        self.assertTrue(all(item["target_uri"] == "urn:name" for item in names))

    def test_existing_trace_is_never_overwritten(self):
        self.run_fake()
        with self.assertRaises(FileExistsError):
            self.run_fake()

    def retrieval_fixture(self, **metadata_overrides):
        path = self.root / "retrieval.json"
        metadata = {"embedding_model": "offline-fixture-vector-model",
                    "ontology_sha256": hashlib.sha256(self.ontology.read_bytes()).hexdigest(),
                    "index_artifact": "fixture-index"}
        metadata.update(metadata_overrides)
        path.write_text(json.dumps({"metadata": metadata, "candidates": [
            {"table": "child", "column": None, "candidate_uris": ["urn:Place"]},
            {"table": "child", "column": "name", "candidate_uris": [
                {"uri": "urn:name", "score": 0.9}, {"uri": "urn:parent", "score": 0.2}]},
        ]}))
        return path

    def test_precomputed_retrieval_identical_for_full_and_single(self):
        path = self.retrieval_fixture()
        full, full_client = self.run_fake(retrieval_path=path)
        single, single_client = self.run_fake("single_agent", retrieval_path=path)
        full_context = json.loads(full_client.calls[0]["messages"][1]["content"])
        single_context = json.loads(single_client.calls[0]["messages"][1]["content"])
        self.assertEqual(full_context["vector_retrieval"], single_context["vector_retrieval"])
        self.assertEqual(full["retrieval"], single["retrieval"])
        self.assertEqual(full["retrieval"]["embedding_model"], "offline-fixture-vector-model")
        self.assertEqual(len(full_context["vector_retrieval"]["candidates"][1]["candidates"]), 2)

    def test_retrieval_rejects_changed_ontology_and_invented_uri(self):
        path = self.retrieval_fixture(ontology_sha256="wrong")
        with self.assertRaisesRegex(ValueError, "ontology_sha256"):
            self.run_fake(retrieval_path=path)
        path = self.retrieval_fixture()
        payload = json.loads(path.read_text())
        payload["candidates"][0]["candidate_uris"] = ["urn:not-public-ontology"]
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError, "candidate URI"):
            self.run_fake(retrieval_path=path)

    def test_probe_cache_counts_requests_but_reads_rows_once(self):
        result, _ = self.run_fake()
        self.assertEqual(result["tool_usage"], {"probe_requests": 3, "probe_executions": 1,
                                               "probe_cache_hits": 2, "probe_rows_read": 5})
        events = [json.loads(line) for line in Path(result["agenttrace"]).read_text().splitlines()]
        probes = [event for event in events if event["event"] == "value_probe"]
        self.assertEqual([event["rows_read"] for event in probes], [5, 0, 0])
        self.assertEqual([event["cache_hit"] for event in probes], [False, True, True])

    def test_global_probe_request_limit_stops_instead_of_silent_fallback(self):
        result, client = self.run_fake(max_probe_requests=2)
        self.assertEqual(result["run_status"], "probe_budget_exceeded")
        self.assertEqual(result["tool_usage"]["probe_requests"], 2)
        self.assertEqual(len(client.calls), 3)

    def test_probe_row_limit_stops_before_reading_rows(self):
        result, _ = self.run_fake(max_probe_rows=4)
        self.assertEqual(result["run_status"], "probe_budget_exceeded")
        self.assertEqual(result["tool_usage"]["probe_rows_read"], 0)
        self.assertEqual(result["tool_usage"]["probe_executions"], 0)

    def test_sparse_state_omissions_are_explicit_abstentions_not_hidden_patch(self):
        cleared = {"table_mappings": [], "mappings": [], "relation_rules": [], "done": True}
        result, client = self.run_fake(states=[self.state, cleared, cleared])
        first_turn = json.loads(client.calls[0]["messages"][1]["content"])
        self.assertEqual(first_turn["current_state"]["mappings"], [])
        second_turn = json.loads(client.calls[1]["messages"][1]["content"])
        self.assertEqual(len(second_turn["current_state"]["mappings"]), 1)
        self.assertTrue(all(item["target_uri"] is None for item in result["mappings"]))
        self.assertEqual(len(result["mappings"]), 5)


if __name__ == "__main__":
    unittest.main()
