"""Create independent, unlabelled review worksheets; never call them gold."""

import argparse
import csv
import json
import sqlite3
from pathlib import Path

from experiments.public_kg.artifacts import file_hash


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def task_tables(db_path, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    task = out_dir / "relation_candidates.csv"
    queries = out_dir / "query_tasks.csv"
    manifest = out_dir / "review_tasks_manifest.json"
    if any(path.exists() for path in (task, queries, manifest)):
        raise FileExistsError("Refusing to overwrite review work")
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        columns = {table: [r[1] for r in conn.execute(f"PRAGMA table_info({quoted(table)})")] for table in tables}
        counts = {table: conn.execute(f"SELECT COUNT(*) FROM {quoted(table)}").fetchone()[0] for table in tables}
    finally:
        conn.close()
    # Candidate universe is deliberately high recall and not labelled; a pair is
    # merely a request to consider relationships, not evidence of an actual join.
    key_fragments = ("code", "_dm", "guid", "geonameid", "gid", "flmc", "flldm")
    target_names = {"gid", "guid", "dm", "code", "geonameid"}
    candidates = []
    for source in tables:
        if not counts[source] or source in {"zs_GAFJ_xq", "zs_GAJ_xq"}:
            continue
        sources = [column for column in columns[source]
                   if any(fragment in column.casefold() for fragment in key_fragments)
                   or column.casefold() in target_names]
        for target in tables:
            if source == target or not counts[target] or target in {"zs_GAFJ_xq", "zs_GAJ_xq"}:
                continue
            targets = [column for column in columns[target] if column.casefold() in target_names]
            if sources and targets:
                candidates.append({"source_table": source, "source_candidate_columns": json.dumps(sources, ensure_ascii=False),
                    "target_table": target, "target_candidate_columns": json.dumps(targets, ensure_ascii=False),
                    "source_expression": "", "target_expression": "", "predicate_uri": "", "direction": "",
                    "review_status": "pending", "verdict": "", "reviewer": "", "evidence": "", "notes": ""})
    with task.open("x", encoding="utf-8-sig", newline="") as stream:
        fields = ["source_table", "source_candidate_columns", "target_table", "target_candidate_columns",
                  "source_expression", "target_expression", "predicate_uri", "direction", "review_status",
                  "verdict", "reviewer", "evidence", "notes"]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(candidates)
    with queries.open("x", encoding="utf-8-sig", newline="") as stream:
        fields = ["query_id", "template_id", "question", "sql_query", "cypher_query", "comparison_semantics",
                  "split", "review_status", "reviewer", "evidence"]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for index in range(1, 41):
            writer.writerow({"query_id": f"Q{index:02d}", "review_status": "pending"})
    manifest.write_text(json.dumps({"status": "pending_human_review_not_gold", "database_sha256": file_hash(db_path),
        "relation_candidate_pairs": len(candidates), "query_slots": 40,
        "candidate_scope": "mechanical table-pair review queue; not exhaustive, positive labels, or reference rules",
        "independent_of_method_predictions": True, "excluded_proxy_tables": ["zs_GAFJ_xq", "zs_GAJ_xq"],
        "source_docs": ["data/public/数据集说明.md", "https://download.geonames.org/export/dump/readme.txt"]},
        ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return len(candidates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    print(json.dumps({"candidate_table_pairs": task_tables(args.db, args.out), "status": "pending"}))
