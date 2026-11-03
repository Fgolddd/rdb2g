"""Evidence-linked machine reference for Foshan; never human gold."""

import csv
import itertools
import json
from pathlib import Path

from experiments.public_kg.artifacts import file_hash, write_json
from experiments.public_kg.methods import probe_relation, schema_profile

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data/public/benchmark_v2"
URI = "https://w3id.org/rdb2g/public-spatial/"
PROXY = {"zs_GAFJ_xq", "zs_GAJ_xq"}
ADMIN = {"zs_city_bz", "zs_district_bz", "zs_town_bz", "zs_community_bz"}
SOURCE = "data/public/数据集说明.md"


def column(name):
    return {"op": "column", "name": name}


def pair_key(first, second):
    return {"op": "concat", "args": [column(first), {"op": "literal", "value": "|"}, column(second)]}


def build():
    db = BASE / "foshan_osm/sample.sqlite"
    profile = schema_profile(db)
    out = BASE / "foshan_osm/machine_review"
    out.mkdir(parents=True, exist_ok=True)
    paths = [out / name for name in ("mapping_reference.csv", "relation_reference.json", "manifest.json")]
    if any(path.exists() for path in paths):
        raise FileExistsError("Do not overwrite a reviewed reference")
    simple = {"gid": "identifier", "GUID": "identifier", "MC": "name",
              "DH": "name", "DHQC": "name", "DHBM": "alternateName",
              "SSMC": "name", "YLMC": "name", "YLQC": "name",
              "DYH": "unitNumber", "DYHBM": "unitNumber", "DYHQC": "addressText",
              "MPH": "houseNumber", "MPQC": "addressText", "DZ": "addressText",
              "BZ": "description", "ing_2000": "longitude", "lat_2000": "latitude",
              "SYZT": "status", "FLMC": "categoryName", "DLMC": "categoryName",
              "ZLMC": "categoryName", "XLMC": "categoryName", "FLDM": "categoryCode",
              "YLBM": "categoryCode", "SSBM": "categoryCode", "SSLX": "categoryName",
              "S_GUID": "parentIdentifier", "S_DM": "parentIdentifier", "S_MC": "parentName",
              "SJMC": "parentName", "SSJMC": "parentName", "QXMC": "parentName",
              "XZQHDM": "administrativeCode", "XZQHMC": "parentName"}
    rows = []
    for table, info in profile.items():
        for item in info["columns"]:
            name = item["name"]
            target = simple.get(name)
            if name == "DM":
                target = "administrativeCode" if table in ADMIN else "identifier"
            if name in {"CJDM", "CJMC"}:
                target = "administrativeLevel" if table in ADMIN else ("categoryCode" if name == "CJDM" else "categoryName")
            if table == "zs_MP_bz" and name == "DHBM":
                target = "houseNumber"
            reason = "local documented schema plus non-null source values"
            include = bool(target and item["nonnull_count"] and table not in PROXY)
            if table in PROXY:
                reason = "administrative boundary proxy for police jurisdiction; excluded"
            elif not info["rows"]:
                reason = "empty source table: insufficient instance evidence"
            elif not item["nonnull_count"]:
                reason = "all-null source column: semantics not independently confirmed"
            elif not target:
                reason = "parent classification or police metadata lacks an unambiguous target property"
            samples = json.dumps(item["samples"], ensure_ascii=False)
            rows.append({"table": table, "column": name,
                         "accepted_uris_json": json.dumps([URI + target] if include else []),
                         "no_match": "false" if include else "", "review_status": "machine_reviewed" if include else "unresolved",
                         "reviewer": "codex_machine_review", "include_in_diagnostic": str(include).lower(),
                         "evidence": f"{SOURCE}; nonnull={item['nonnull_count']}; {reason}; samples={samples}"})
    with paths[0].open("x", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    rules = []

    def add(source, target, left, right, predicate, rationale):
        rule = {"source_table": source, "target_table": target, "source_expr": left,
                "target_expr": right, "predicate": URI + predicate, "direction": "forward"}
        probe = probe_relation(db, rule, profile)
        if probe["matched_pairs"]:
            rules.append({**rule, "evidence": rationale, "probe": probe})

    for source, info in profile.items():
        names = {item["name"] for item in info["columns"]}
        if not info["rows"] or source in PROXY:
            continue
        if {"S_DM", "S_MC"} <= names:
            for target in sorted(ADMIN - {source}):
                add(source, target, pair_key("S_DM", "S_MC"), pair_key("DM", "MC"),
                    "parentAdministrativeUnit" if source in ADMIN else "locatedIn",
                    f"{SOURCE}: restore source-assigned administrative parent; composite code+name avoids collisions. Not independent geographic verification.")
        for source_name in ("QXMC", "XZQHMC"):
            if source_name in names:
                add(source, "zs_district_bz", column(source_name), column("MC"), "locatedIn",
                    f"{SOURCE}: district name assigned during public-data assembly; unique within single-city scope.")
    identity_tables = ["zs_AOI_dm", "zs_POI_dm", "zs_YL_bz", "zs_facility_dm"]
    for source, target in itertools.combinations(identity_tables, 2):
        add(source, target, column("GUID"), column("GUID"), "sameSourceEntity",
            f"{SOURCE}: identical OSM type-qualified identifiers in overlapping thematic projections; not bare numeric IDs.")
    write_json(paths[1], {"status": "machine_reviewed_reference", "human_reviewed": False,
                           "rules": rules, "scope": "documented source recovery; assessed table pairs only; not exhaustive real-world relations"})
    write_json(paths[2], {"status": "machine_reviewed_reference", "human_reviewed": False,
                           "database_sha256": file_hash(db),
                           "ontology_sha256": file_hash(BASE / "ontology/target_terms.json"),
                           "reviewer": "codex_machine_review", "source_document_sha256": file_hash(ROOT / SOURCE),
                           "mapping_items": len(rows), "diagnostic_items": sum(row["include_in_diagnostic"] == "true" for row in rows),
                           "relation_rules": len(rules), "prediction_files_read": False,
                           "limitations": ["Local preprocessing documentation is not an independent official field dictionary.",
                                          "Machine review is not human gold and cannot establish method superiority.",
                                          "Excluded empty columns are unknown, not negative mappings.",
                                          "Relationship recall is restricted to declared rule-defined scope."]})
    print(json.dumps({"out": str(out), "fields": len(rows), "rules": len(rules)}))


if __name__ == "__main__":
    build()
