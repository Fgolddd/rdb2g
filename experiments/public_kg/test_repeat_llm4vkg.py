"""Unit tests for LLM4VKG repeat accounting and failure reporting."""
import unittest

from experiments.public_kg.repeat_llm4vkg import classify_failure, metric_summary, sanitize


class RepeatLLM4VKGTests(unittest.TestCase):
    def test_mean_and_sample_standard_deviation_ignore_missing_scores(self):
        result = metric_summary([0.5, 1.0, None])
        self.assertEqual(result["n"], 2)
        self.assertAlmostEqual(result["mean"], 0.75)
        self.assertAlmostEqual(result["sample_stddev"], 0.3535533905932738)

    def test_empty_score_summary_is_explicit(self):
        self.assertEqual(metric_summary([None]), {"n": 0, "mean": None, "sample_stddev": None})
        self.assertEqual(metric_summary([0.5]), {"n": 1, "mean": 0.5, "sample_stddev": None})

    def test_token_and_request_budget_failures_are_classified(self):
        self.assertEqual(classify_failure("RuntimeError: next request may exceed the total token cap"),
                         "request_or_token_budget_limited")
        self.assertEqual(classify_failure("RuntimeError: request cap reached for LLM4VKG"),
                         "request_or_token_budget_limited")

    def test_missing_dependency_failure_is_specific(self):
        self.assertEqual(classify_failure("ModuleNotFoundError: No module named 'sentence_transformers'"),
                         "dependency_error")

    def test_secret_redaction_applies_recursively(self):
        value = {"error": "provider rejected abcdefgh0123456789", "nested": ["abcdefgh0123456789"]}
        redacted = sanitize(value, ("abcdefgh0123456789",))
        self.assertNotIn("abcdefgh0123456789", str(redacted))
        self.assertIn("[REDACTED]", redacted["error"])


if __name__ == "__main__":
    unittest.main()
