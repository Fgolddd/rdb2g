"""Focused unit tests for repeated baseline aggregation and pinned inputs."""
import json
import tempfile
import unittest
from pathlib import Path

from experiments.public_kg.repeat_field_baselines import load_terms, summarize_runs


class RepeatFieldBaselineTests(unittest.TestCase):
    def test_load_terms_checks_fixed_ontology_and_field_scope(self):
        terms_path = Path("data/public/benchmark_v2/ontology/target_terms.json")
        raw, fields = load_terms(terms_path)
        self.assertEqual(len(raw["terms"]), 47)
        self.assertEqual(len(fields), 32)

    def test_failed_runs_are_excluded_not_zero(self):
        runs = [
            {"method": "llmatch", "dataset": "foshan_osm", "replicate": 1,
             "status": "prediction_component_completed",
             "machine_diagnostics": {"status": "machine_reference_diagnostic_only",
                "mapping": {"status": "machine_reference_diagnostic_only", "accuracy": 0.5,
                            "precision": 0.6, "recall": 0.7, "f1": 0.64}}},
            {"method": "llmatch", "dataset": "foshan_osm", "replicate": 2,
             "status": "prediction_component_failed_or_partial",
             "machine_diagnostics": {"status": "inference_not_successful"}},
        ]
        group = summarize_runs(runs)["groups"][0]
        metrics = group["metrics_over_scored_runs_only"]
        self.assertEqual(metrics["accuracy"], {"n_scored": 1, "mean": 0.5, "std_sample": None})
        self.assertEqual(group["successful_diagnostics"], 1)

    def test_sample_standard_deviation(self):
        runs = []
        for replicate, value in enumerate((0.25, 0.75), 1):
            runs.append({"method": "magneto-base", "dataset": "geonames_cn", "replicate": replicate,
                "status": "base_method_completed",
                "machine_diagnostics": {"status": "machine_reference_diagnostic_only",
                    "mapping": {"status": "machine_reference_diagnostic_only", "accuracy": value,
                                "precision": value, "recall": value, "f1": value}}})
        summary = summarize_runs(runs)["groups"][0]["metrics_over_scored_runs_only"]["accuracy"]
        self.assertAlmostEqual(summary["mean"], 0.5)
        self.assertAlmostEqual(summary["std_sample"], 2 ** -0.5 / 2)


if __name__ == "__main__":
    unittest.main()
