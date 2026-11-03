"""Offline regression checks for the create-only benchmark freezer."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from experiments.public_kg.prepare_data import (
    TARGET, allocate, freeze, select_foshan, select_geonames, sha256,
)


class PreparationTests(unittest.TestCase):
    def test_largest_remainder(self):
        self.assertEqual(allocate({"a": 3, "b": 2, "c": 1}, 4), {"a": 2, "b": 1, "c": 1})
        with self.assertRaises(ValueError):
            allocate({"a": 1}, 2)

    def test_geonames_freeze_is_source_faithful_and_create_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.sqlite"
            connection = sqlite3.connect(source)
            connection.execute("CREATE TABLE gn_place (geonameid INTEGER PRIMARY KEY, name TEXT, "
                               "feature_class TEXT, admin1_code TEXT, feature_code TEXT, population INTEGER)")
            connection.executemany("INSERT INTO gn_place VALUES (?, ?, ?, ?, ?, ?)", [
                (i, None if i % 7 == 0 else str(i), "A" if i % 2 else "P",
                 None if i % 11 == 0 else ("00" if i % 3 else "01"), "ADM1" if i <= 4 else "PPL", i)
                for i in range(1, 12001)])
            for table in ["gn_admin1", "gn_admin2", "gn_feature_code"]:
                connection.execute(f"CREATE TABLE {table} (code TEXT PRIMARY KEY, name TEXT)")
                connection.execute(f"INSERT INTO {table} VALUES ('00', 'unchanged')")
            connection.commit()
            selection, policy = select_geonames(connection, 20261029)
            self.assertEqual(sum(map(len, selection.values())), TARGET)
            self.assertTrue({1, 2, 3, 4} <= selection["gn_place"])
            self.assertEqual(policy["administrative_skeleton_rows"], 4)
            connection.close()
            before = sha256(source)
            output = root / "output"
            manifest = freeze(source, output, "geonames_cn", 20261029)
            self.assertEqual(sha256(source), before)
            self.assertEqual(manifest["sample_total_rows"], TARGET)
            self.assertTrue(manifest["same_seed_ids_reproduced"])
            sample = sqlite3.connect(output / "sample.sqlite")
            self.assertGreater(sample.execute("SELECT count(*) FROM gn_place WHERE admin1_code='00'").fetchone()[0], 0)
            self.assertEqual(len(sample.execute("PRAGMA table_info(gn_place)").fetchall()), 6)
            self.assertEqual(sample.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            sample.close()
            template = json.loads((output / "annotation/gold_schema.template.json").read_text())
            self.assertFalse(template["is_gold"])
            with self.assertRaises(FileExistsError):
                freeze(source, output, "geonames_cn", 20261029)

    def test_foshan_group_and_parent_closure(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        for table in ["zs_AOI_dm", "zs_POI_dm", "zs_street_bz", "zs_town_bz", "zs_empty"]:
            connection.execute(f"CREATE TABLE {table} (gid TEXT, GUID TEXT, DM TEXT, S_DM TEXT, QXMC TEXT, XLMC TEXT)")
        connection.execute("INSERT INTO zs_town_bz VALUES ('admin', 'admin', 'parent', NULL, NULL, NULL)")
        for table in ["zs_AOI_dm", "zs_POI_dm", "zs_street_bz"]:
            connection.executemany(f"INSERT INTO {table} VALUES (?, ?, NULL, 'parent', 'district', 'category')", [
                (str(i), ("street" if table == "zs_street_bz" else "same") + str(i))
                for i in range(4000)])
        first, policy = select_foshan(connection, 20261029)
        second, _ = select_foshan(connection, 20261029)
        self.assertEqual(first, second)
        self.assertEqual(sum(map(len, first.values())), TARGET)
        self.assertEqual(first["zs_AOI_dm"], first["zs_POI_dm"])
        self.assertEqual(len(first["zs_town_bz"]), 1)
        self.assertEqual(first["zs_empty"], set())
        self.assertEqual(policy["sample_induced_missing_S_DM"], [])
        connection.close()

    def _foshan_collision_fixture(self, child_parent_name):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        for table in ["zs_district_bz", "zs_GAFJ_xq", "zs_town_bz", "zs_large_source"]:
            connection.execute(f"CREATE TABLE {table} (gid TEXT, GUID TEXT, DM TEXT, S_DM TEXT, "
                               "MC TEXT, S_MC TEXT)")
        # The two upstream records share a code, but only one matches the
        # child's parent-name evidence. Keep this table above the small-table
        # inclusion limit so we can observe whether closure selects the right
        # candidate or reports the ambiguity.
        connection.executemany("INSERT INTO zs_district_bz VALUES (?, ?, ?, NULL, ?, NULL)", [
            ("district", "district-guid", "440607", "三水区"),
        ] + [(f"other-{i}", f"other-guid-{i}", f"other-code-{i}", f"Other {i}")
             for i in range(198)])
        connection.executemany("INSERT INTO zs_GAFJ_xq VALUES (?, ?, ?, NULL, ?, NULL)", [
            ("proxy", "proxy-guid", "440607", "三水分局"),
        ] + [(f"proxy-other-{i}", f"proxy-other-guid-{i}", f"proxy-code-{i}", f"Proxy {i}")
             for i in range(198)])
        connection.execute("INSERT INTO zs_town_bz VALUES ('town', 'town-guid', NULL, '440607', "
                           "'乐平镇', ?)", (child_parent_name,))
        connection.executemany("INSERT INTO zs_large_source VALUES (?, ?, ?, NULL, ?, NULL)", [
            (f"large-{i}", f"large-guid-{i}", f"large-code-{i}", f"Large {i}")
            for i in range(10000)])
        return connection

    def test_foshan_parent_code_collision_uses_parent_name_evidence(self):
        connection = self._foshan_collision_fixture("三水区")
        selected, policy = select_foshan(connection, 20261029)
        self.assertIn(1, selected["zs_district_bz"])
        self.assertEqual(policy["mandatory_rows_with_closure"], 2)
        self.assertEqual(policy["ambiguous_S_DM_reference_count"], 0)
        self.assertEqual(policy["sample_induced_missing_S_DM"], [])
        connection.close()

    def test_foshan_parent_code_collision_without_evidence_is_reported(self):
        connection = self._foshan_collision_fixture("")
        selected, policy = select_foshan(connection, 20261029)
        self.assertEqual(policy["mandatory_rows_with_closure"], 1)
        self.assertEqual(policy["ambiguous_S_DM_reference_count"], 1)
        example = policy["ambiguous_S_DM_reference_examples"][0]
        self.assertEqual(example["S_DM"], "440607")
        self.assertEqual({item["metadata_match"] for item in example["candidates"]}, {"unknown"})
        self.assertEqual({item["table"] for item in example["candidates"]},
                         {"zs_district_bz", "zs_GAFJ_xq"})
        connection.close()


if __name__ == "__main__":
    unittest.main()
