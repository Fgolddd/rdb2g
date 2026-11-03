"""Focused checks for JOINT-adapted matching and traversal."""
import unittest
from experiments.public_kg.frontier_joint import _jaccard, dijkstra, name_similarity, value_similarity

class FrontierJointTests(unittest.TestCase):
    def test_perfect_overlap_cycle_terminates(self):
        def rule(a,b):
            return {"source_table":a,"target_table":b,"source_expr":{"name":"id"},"target_expr":{"name":"id"},"predicate":"urn:p","joint_adapted":{"fuzzy_value_jaccard":1.0}}
        paths=dijkstra([rule("a","b"),rule("b","a")],["a","b"])
        self.assertEqual(len(paths),2)
        self.assertTrue(all(p["total_weight"] >= 0 for p in paths))

    def test_name_and_value_similarity(self):
        self.assertGreater(name_similarity("admin_code", "admin_id"), 0.2)
        self.assertGreater(value_similarity("Smith John", "John Smith"), 0.99)

    def test_fuzzy_jaccard(self):
        score, matches = _jaccard(["John Smith", "Alice Jones"], ["Smith John", "Alice Jones"])
        self.assertEqual(len(matches), 2)
        self.assertAlmostEqual(score, 1.0)

    def test_dijkstra_prefers_high_overlap_product(self):
        def rule(a, b, overlap):
            return {"source_table": a, "target_table": b, "source_expr": {"name": a + "_key"}, "target_expr": {"name": b + "_key"}, "predicate": "urn:p", "joint_adapted": {"fuzzy_value_jaccard": overlap}}
        paths = dijkstra([rule("a", "b", .9), rule("b", "c", .9), rule("a", "c", .2)], ["a", "b", "c"])
        ac = next(p for p in paths if p["source_table"] == "a" and p["target_table"] == "c")
        self.assertEqual(len(ac["edges"]), 2)
        self.assertAlmostEqual(ac["retained_overlap_product"], .81)

if __name__ == "__main__":
    unittest.main()
