"""Freeze deterministic, source-faithful 10,000-row public benchmark samples.

Run with .venv/bin/python -m experiments.public_kg.prepare_data.
Outputs are deliberately create-only; pending annotation is not gold evidence.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

SEED = 20261029
TARGET = 10000
PROXIES = {"zs_GAJ_xq", "zs_GAFJ_xq"}


def quote(value):
    return '"' + value.replace('"', '""') + '"'


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def tables(connection):
    return [r[0] for r in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def allocate(counts, budget):
    """Largest-remainder allocation, with stable tie breaking."""
    total = sum(counts.values())
    if not total:
        if budget:
            raise ValueError("Insufficient rows")
        return {key: 0 for key in counts}
    if budget > total:
        raise ValueError("Budget exceeds available rows")
    quotas = {key: budget * count // total for key, count in counts.items()}
    order = sorted(counts, key=lambda key: (-(budget * counts[key] % total), key))
    for key in order[:budget - sum(quotas.values())]:
        quotas[key] += 1
    return quotas


def select_geonames(connection, seed):
    rng = random.Random(seed)
    selected = {}
    for table in tables(connection):
        if table != "gn_place":
            selected[table] = {r[0] for r in connection.execute(f"SELECT rowid FROM {quote(table)}")}
    budget = TARGET - sum(map(len, selected.values()))
    skeleton = set()
    strata = defaultdict(list)
    for rowid, feature, admin1, code in connection.execute(
        "SELECT rowid, feature_class, admin1_code, feature_code FROM gn_place ORDER BY geonameid"):
        if code in {"ADM1", "ADM2"}:
            skeleton.add(rowid)
        else:
            strata[(feature or "", admin1 or "")].append(rowid)
    if len(skeleton) > budget:
        raise ValueError("Administrative skeleton exceeds place budget")
    quotas = allocate({key: len(ids) for key, ids in strata.items()}, budget - len(skeleton))
    chosen = set(skeleton)
    for key in sorted(strata):
        chosen.update(rng.sample(strata[key], quotas[key]))
    selected["gn_place"] = chosen
    return selected, {"place_budget": budget, "administrative_skeleton_rows": len(skeleton),
                      "stratification": ["feature_class", "admin1_code"],
                      "unknown_codes_policy": "Retain source values, including 00; do not impute names."}


def select_foshan(connection, seed):
    rng = random.Random(seed)
    rows = {}
    groups = defaultdict(set)
    row_group = {}
    dm_index = defaultdict(set)
    for table in tables(connection):
        for raw in connection.execute(f"SELECT rowid AS _sample_rowid, * FROM {quote(table)} ORDER BY gid"):
            row = dict(raw)
            key = (table, row.pop("_sample_rowid"))
            rows[key] = row
            group = row.get("GUID") or f"{table}:{key[1]}"
            groups[group].add(key)
            row_group[key] = group
            if row.get("DM"):
                dm_index[row["DM"]].add(key)

    # S_DM often identifies a parent, but is not globally unique. Resolve a
    # collision only when the child carries parent-name/type metadata that
    # identifies exactly one candidate group. Otherwise leave it unresolved
    # and report it instead of claiming a false closure.
    parent_metadata = (
        ("S_MC", "MC"), ("S_FLMC", "FLMC"), ("S_CJMC", "CJMC"),
        ("S_GAMC", "GAMC"), ("S_GADM", "GADM"),
    )

    def resolve_parent(key):
        value = rows[key].get("S_DM")
        candidates = dm_index.get(value, ()) if value else ()
        candidate_groups = defaultdict(set)
        for candidate in candidates:
            candidate_groups[row_group[candidate]].add(candidate)
        if not candidate_groups:
            return set(), None

        states = {}
        for group, members in candidate_groups.items():
            compared = matched = False
            for child_field, parent_field in parent_metadata:
                expected = rows[key].get(child_field)
                if expected in (None, ""):
                    continue
                values = {rows[member].get(parent_field) for member in members
                          if rows[member].get(parent_field) not in (None, "")}
                if not values:
                    continue
                compared = True
                if expected in values:
                    matched = True
            states[group] = "matched" if matched else "conflicting" if compared else "unknown"

        if len(candidate_groups) == 1:
            group, = candidate_groups
            if states[group] != "conflicting":
                return set(candidate_groups[group]), None
        else:
            matched_groups = [group for group, state in states.items() if state == "matched"]
            if len(matched_groups) == 1 and all(
                    state == "conflicting" for group, state in states.items()
                    if group != matched_groups[0]):
                return set(candidate_groups[matched_groups[0]]), None

        detail = {
            "table": key[0], "rowid": key[1], "S_DM": value,
            "S_MC": rows[key].get("S_MC"), "S_FLMC": rows[key].get("S_FLMC"),
            "S_CJMC": rows[key].get("S_CJMC"),
            "candidates": [{"table": candidate[0], "rowid": candidate[1],
                            "GUID": rows[candidate].get("GUID"),
                            "DM": rows[candidate].get("DM"),
                            "MC": rows[candidate].get("MC"),
                            "FLMC": rows[candidate].get("FLMC"),
                            "CJMC": rows[candidate].get("CJMC"),
                            "metadata_match": states[row_group[candidate]]}
                           for candidate in sorted(candidates)],
        }
        return set(), detail

    def closure(group):
        result = set(groups[group])
        todo = list(result)
        while todo:
            key = todo.pop()
            parents, _ = resolve_parent(key)
            for parent in parents:
                for member in groups[row_group[parent]]:
                    if member not in result:
                        result.add(member)
                        todo.append(member)
        return result

    selected = set()
    counts = Counter(table for table, _ in rows)
    for key in rows:
        if counts[key[0]] <= 150:
            selected.update(closure(row_group[key]))
    forced_count = len(selected)
    strata = defaultdict(list)
    for group, members in sorted(groups.items()):
        if members <= selected:
            continue
        key = min(members)
        row = rows[key]
        stratum = (key[0], row.get("QXMC") or row.get("XZQHMC") or row.get("S_DM") or "",
                   row.get("XLMC") or row.get("SSLX") or row.get("FLDM") or "")
        strata[stratum].append(group)
    quotas = allocate({key: sum(len(groups[g] - selected) for g in values)
                       for key, values in strata.items()}, TARGET - len(selected))
    leftovers = []
    for stratum in sorted(strata):
        rng.shuffle(strata[stratum])
        consumed = 0
        for group in strata[stratum]:
            additions = closure(group) - selected
            if consumed + len(additions) <= quotas[stratum] and len(selected) + len(additions) <= TARGET:
                selected.update(additions)
                consumed += len(additions)
            else:
                leftovers.append(group)
    rng.shuffle(leftovers)
    for group in leftovers:
        additions = closure(group) - selected
        if len(selected) + len(additions) <= TARGET:
            selected.update(additions)
    if len(selected) != TARGET:
        raise ValueError(f"Cannot reach exact target while preserving same-source groups: {len(selected)}")
    missing = []
    ambiguities = {}
    source_missing = Counter()
    for key in sorted(selected):
        value = rows[key].get("S_DM")
        if value:
            parents = dm_index.get(value, set())
            if not parents:
                source_missing[key[0]] += 1
            else:
                resolved, ambiguity = resolve_parent(key)
                if ambiguity:
                    ambiguities[(key[0], key[1], value)] = ambiguity
                elif not resolved or not all(
                        groups[row_group[parent]] <= selected for parent in resolved):
                    missing.append({"table": key[0], "rowid": key[1], "S_DM": value,
                                    "resolved_parent_rows": [list(parent) for parent in sorted(resolved)]})
    split_groups = sum(bool(members & selected) and not members <= selected for members in groups.values())
    if missing or split_groups:
        raise AssertionError("Reference/group closure violated")
    result = {table: {rowid for t, rowid in selected if t == table} for table in tables(connection)}
    return result, {"stratification": ["same-source GUID group", "table", "district or parent", "category"],
                    "small_table_max_rows": 150, "mandatory_rows_with_closure": forced_count,
                    "split_same_source_groups": split_groups, "sample_induced_missing_S_DM": missing,
                    "ambiguous_S_DM_reference_count": len(ambiguities),
                    "ambiguous_S_DM_reference_examples": [ambiguities[key] for key in sorted(ambiguities)[:100]],
                    "source_unresolved_S_DM_by_table": dict(source_missing),
                    "proxy_tables_excluded_from_real_semantic_evaluation": sorted(PROXIES)}


def export_sample(connection, selected, out, dataset, seed, source_hash):
    destination = sqlite3.connect(out / "sample.sqlite")
    profile, ids, strata, tasks = {}, [], Counter(), []
    audited = 0
    for table in tables(connection):
        sql = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
        destination.execute(sql)
        columns = [r[1] for r in connection.execute(f"PRAGMA table_info({quote(table)})")]
        source_count = connection.execute(f"SELECT count(*) FROM {quote(table)}").fetchone()[0]
        source_rows = []
        # Batched point reads avoid scanning the million-row source for a small sample.
        for rowid in sorted(selected[table]):
            row = connection.execute(f"SELECT * FROM {quote(table)} WHERE rowid=?", (rowid,)).fetchone()
            values = tuple(row)
            destination.execute(f"INSERT INTO {quote(table)} VALUES ({','.join('?' for _ in columns)})", values)
            source_rows.append(values)
            item = dict(zip(columns, values))
            identifier = item.get("gid", item.get("geonameid", item.get("code")))
            ids.append({"table": table, "source_rowid": rowid, "source_id": identifier})
            strata[(table, item.get("feature_class") or "", item.get("admin1_code") or "",
                    item.get("QXMC") or item.get("XZQHMC") or item.get("S_DM") or "",
                    item.get("XLMC") or item.get("SSLX") or "")] += 1
        actual = [tuple(r) for r in destination.execute(f"SELECT * FROM {quote(table)} ORDER BY rowid")]
        if actual != source_rows:
            raise AssertionError(f"Source-value audit failed for {table}")
        audited += len(actual)
        column_profile = []
        for index, column in enumerate(columns):
            values = [row[index] for row in actual]
            examples = list(dict.fromkeys(value for value in values if value is not None))[:3]
            column_profile.append({"name": column, "null_count": sum(value is None for value in values),
                                   "distinct_non_null": len({v for v in values if v is not None}), "examples": examples})
            tasks.append({"dataset": dataset, "table": table, "column": column, "status": "pending",
                          "semantic_evaluation": "excluded_proxy" if table in PROXIES else "pending",
                          "proposed_mapping": "", "gold_mapping": "", "reviewer": "", "notes": ""})
        profile[table] = {"source_rows": source_count, "sample_rows": len(actual), "create_sql": sql,
                          "columns": column_profile}
    destination.commit()
    integrity = destination.execute("PRAGMA integrity_check").fetchone()[0]
    destination.close()
    if integrity != "ok" or audited != TARGET:
        raise AssertionError((integrity, audited))
    write_csv(out / "ids.csv", ["table", "source_rowid", "source_id"], ids)
    write_csv(out / "strata.csv", ["table", "feature_class", "admin1_code", "region_or_parent", "category", "rows"],
              [dict(zip(["table", "feature_class", "admin1_code", "region_or_parent", "category", "rows"], (*key, count)))
               for key, count in sorted(strata.items())])
    write_json(out / "schema_profile.json", profile)
    write_json(out / "source_value_audit.json", {"status": "passed", "audited_rows": audited,
               "method": "All selected source tuples compared to all output tuples, including nulls and original types",
               "source_sha256": source_hash, "integrity_check": integrity, "seed": seed})
    (out / "annotation").mkdir()
    write_csv(out / "annotation" / "mapping_tasks.csv", list(tasks[0]), tasks)
    write_json(out / "annotation" / "gold_schema.template.json", {
        "status": "pending", "is_gold": False, "dataset": dataset,
        "instructions": "Human review required. Empty templates are not evaluation labels.",
        "table_mappings": [], "column_mappings": [], "relation_mappings": [], "reviewer": None})
    return {table: entry["sample_rows"] for table, entry in profile.items()}


def geonames_reference_audit(sample_path):
    """Report unresolved source codes without interpreting unknowns as real links."""
    connection = sqlite3.connect(sample_path.resolve().as_uri() + "?mode=ro", uri=True)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(gn_place)")}
    if not {"country_code", "admin2_code"} <= columns:
        connection.close()
        return {"status": "not_applicable_to_reduced_fixture"}
    result = {}
    for name, code, table, present in [
        ("admin1", "p.country_code || '.' || p.admin1_code", "gn_admin1", "p.admin1_code"),
        ("admin2", "p.country_code || '.' || p.admin1_code || '.' || p.admin2_code", "gn_admin2", "p.admin2_code"),
        ("feature_code", "p.feature_class || '.' || p.feature_code", "gn_feature_code", "p.feature_code"),
    ]:
        nonblank, unresolved = connection.execute(
            f"SELECT count(*), sum(NOT EXISTS (SELECT 1 FROM {table} c WHERE c.code={code})) "
            f"FROM gn_place p WHERE {present} IS NOT NULL AND {present} != ''").fetchone()
        result[name] = {"nonblank_source_codes": nonblank, "unresolved_against_retained_code_table": unresolved or 0,
                        "sample_induced_code_table_omissions": 0}
    for table in ["gn_admin1", "gn_admin2"]:
        missing = connection.execute(
            f"SELECT count(*) FROM {table} a WHERE NOT EXISTS "
            "(SELECT 1 FROM gn_place p WHERE p.geonameid=a.geonameid)").fetchone()[0]
        result[table + "_geonameid_not_in_place_sample"] = missing
    connection.close()
    result["policy"] = "Unresolved codes are retained verbatim; none are silently cleaned or linked."
    return result


def freeze(source, out, dataset, seed):
    if out.exists():
        raise FileExistsError(f"Refusing to overwrite {out}")
    source_hash = sha256(source)
    out.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="public-kg-freeze-") as temporary:
        source_connection = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)
        snapshot_path = Path(temporary) / "snapshot.sqlite"
        snapshot_writer = sqlite3.connect(snapshot_path)
        source_connection.backup(snapshot_writer)
        snapshot_writer.close()
        source_connection.close()
        connection = sqlite3.connect(snapshot_path.resolve().as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        selector = select_foshan if dataset == "foshan_osm" else select_geonames
        selected, policy = selector(connection, seed)
        repeated, _ = selector(connection, seed)
        if selected != repeated:
            raise AssertionError("Same seed did not reproduce selected IDs")
        counts = export_sample(connection, selected, out, dataset, seed, source_hash)
        if dataset == "geonames_cn":
            policy["reference_audit"] = geonames_reference_audit(out / "sample.sqlite")
        snapshot_hash = sha256(snapshot_path)
        connection.close()
    if sha256(source) != source_hash:
        raise RuntimeError("Source changed while freezing; discard incomplete run")
    manifest = {"dataset": dataset, "created_utc": datetime.now(timezone.utc).isoformat(),
                "source": str(source.resolve()), "source_sha256": source_hash,
                "consistent_backup_snapshot_sha256": snapshot_hash,
                "generation_code_sha256": sha256(Path(__file__)), "seed": seed,
                "sample_total_rows": sum(counts.values()), "table_counts": counts,
                "same_seed_ids_reproduced": True, "source_values_preserved": True,
                "annotation_status": "pending", "gold_available": False, "sampling_policy": policy,
                "artifact_sha256": {str(path.relative_to(out)): sha256(path)
                    for path in sorted(out.rglob("*")) if path.is_file()}}
    write_json(out / "manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("data/public"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/public/benchmark_v1"))
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    if args.out_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing benchmark {args.out_dir}")
    args.out_dir.mkdir(parents=True)
    manifests = {}
    for dataset in ["foshan_osm", "geonames_cn"]:
        manifests[dataset] = freeze(args.source_dir / f"{dataset}.sqlite", args.out_dir / dataset, dataset, args.seed)
        print(dataset, manifests[dataset]["table_counts"], flush=True)
    write_json(args.out_dir / "manifest.json", {"version": "benchmark_v1", "seed": args.seed,
               "datasets": {name: {"rows": item["sample_total_rows"],
                            "manifest_sha256": sha256(args.out_dir / name / "manifest.json")}
                            for name, item in manifests.items()}, "gold_status": "pending"})


if __name__ == "__main__":
    main()
