import argparse
import csv
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from collections import Counter, defaultdict
from urllib.parse import urlparse

from neo4j import GraphDatabase


_FIELD_DISPLAY_MAP = None
_TABLE_FIELD_DISPLAY_MAP = None
_DEFAULT_DISPLAY_KB = Path("data/company/zhongshan_rag_terms.json")


def _load_kb_entries(kb_path: Path):
    if not kb_path.exists():
        return []
    try:
        with kb_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict):
        if isinstance(data.get("terms"), list):
            entries = data["terms"]
        elif isinstance(data.get("@graph"), list):
            entries = data["@graph"]
        else:
            entries = [data]
    else:
        entries = []
    return [entry for entry in entries if isinstance(entry, dict)]


def _load_display_maps():
    global _FIELD_DISPLAY_MAP, _TABLE_FIELD_DISPLAY_MAP
    if _FIELD_DISPLAY_MAP is not None and _TABLE_FIELD_DISPLAY_MAP is not None:
        return _FIELD_DISPLAY_MAP, _TABLE_FIELD_DISPLAY_MAP

    field_counters = defaultdict(Counter)
    table_field_map = {}

    for entry in _load_kb_entries(_DEFAULT_DISPLAY_KB):
        label = str(entry.get("label", "")).strip()
        uri = str(entry.get("uri", "")).strip()
        domain = str(entry.get("domain", "")).strip()
        if not label or not uri:
            continue

        tail = uri
        if uri.startswith("http://") or uri.startswith("https://"):
            try:
                tail = urlparse(uri).path.rsplit("/", 1)[-1] or uri.rsplit("/", 1)[-1]
            except Exception:
                tail = uri.rsplit("/", 1)[-1]

        table_name = domain
        field_name = tail
        if "." in tail:
            parts = tail.rsplit(".", 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                table_name = parts[0]
                field_name = parts[1]

        field_name = str(field_name).strip()
        if not field_name:
            continue

        field_counters[field_name][label] += 1
        field_counters[field_name.lower()][label] += 1
        if table_name:
            table_field_map[f"{str(table_name).lower()}.{field_name.lower()}"] = label

    field_map = {
        field: counter.most_common(1)[0][0]
        for field, counter in field_counters.items()
        if counter
    }
    field_map.setdefault("gid", "记录ID")
    field_map.setdefault("geom", "空间几何")
    field_map.setdefault("Shape_Length", "几何长度")
    field_map.setdefault("Shape_Area", "几何面积")
    field_map.setdefault("S_GUID", "上级系统标识码")

    table_field_map.update({
        "zs_mp_bz.dm": "门牌代码",
        "zs_dh_bz.dm": "楼栋代码",
        "zs_dy_bz.dm": "单元代码",
        "zs_fj_bz.dm": "房间代码",
        "zs_yl_bz.dm": "院落代码",
        "zs_mp_bz.dhbm": "门牌别名",
        "zs_dh_bz.dhbm": "栋号别名",
        "zs_gaj_xq.gadm": "公安组织机构_代码",
        "zs_gafj_xq.gadm": "公安组织机构_代码",
        "zs_pcs_xq.gadm": "公安组织机构_代码",
        "zs_jws_xq.gadm": "公安组织机构_代码",
        "zs_gaj_xq.gamc": "公安组织机构_名称",
        "zs_gafj_xq.gamc": "公安组织机构_名称",
        "zs_pcs_xq.gamc": "公安组织机构_名称",
        "zs_jws_xq.gamc": "公安组织机构_名称",
    })

    _FIELD_DISPLAY_MAP = field_map
    _TABLE_FIELD_DISPLAY_MAP = table_field_map
    return _FIELD_DISPLAY_MAP, _TABLE_FIELD_DISPLAY_MAP


def _parse_field_key(key: str):
    table_hint = None
    field_hint = key
    text = str(key).strip()
    if not text:
        return table_hint, field_hint

    if text.startswith("http://") or text.startswith("https://"):
        try:
            tail = urlparse(text).path.rsplit("/", 1)[-1]
            if not tail:
                tail = text.rsplit("/", 1)[-1]
        except Exception:
            tail = text.rsplit("/", 1)[-1]
        text = tail

    if "." in text:
        parts = text.rsplit(".", 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            table_hint = parts[0].lower()
            field_hint = parts[1]
            return table_hint, field_hint

    return table_hint, text


def _display_name_for_key(key: str):
    field_map, table_field_map = _load_display_maps()
    table_hint, field_hint = _parse_field_key(key)
    if table_hint and field_hint:
        mapped = table_field_map.get(f"{table_hint}.{field_hint.lower()}")
        if mapped:
            return mapped
    return field_map.get(field_hint, field_map.get(str(field_hint).lower(), field_hint))


def _preview_to_cn(preview_rows):
    if not isinstance(preview_rows, list):
        return preview_rows

    converted = []
    for row in preview_rows:
        if not isinstance(row, dict):
            converted.append(row)
            continue
        cn_row = {}
        for key, value in row.items():
            display_key = _display_name_for_key(str(key))
            if display_key in cn_row:
                display_key = f"{display_key}<{key}>"
            cn_row[display_key] = value
        converted.append(cn_row)
    return converted


def load_cases(question_bank_path: Path):
    with question_bank_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def is_executable_query(query: str) -> bool:
    query = (query or "").strip()
    return bool(query) and not query.startswith("--")


def run_rdb_eval(cases, db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    results = []

    try:
        for case in cases:
            query = (case.get("sql_query") or "").strip()
            if not is_executable_query(query):
                results.append(
                    {
                        "id": case["id"],
                        "task_type": case.get("task_type", ""),
                        "engine": "rdb",
                        "success": 0,
                        "latency_ms": 0.0,
                        "row_count": 0,
                        "error_type": "not_executable",
                        "result_preview": "",
                        "result_preview_cn": "",
                    }
                )
                continue

            start = time.perf_counter()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                latency_ms = round((time.perf_counter() - start) * 1000, 3)
                preview = [dict(r) for r in rows[:5]]
                preview_cn = _preview_to_cn(preview)
                results.append(
                    {
                        "id": case["id"],
                        "task_type": case.get("task_type", ""),
                        "engine": "rdb",
                        "success": 1,
                        "latency_ms": latency_ms,
                        "row_count": len(rows),
                        "error_type": "",
                        "result_preview": json.dumps(preview, ensure_ascii=False),
                        "result_preview_cn": json.dumps(preview_cn, ensure_ascii=False),
                    }
                )
            except Exception as e:
                latency_ms = round((time.perf_counter() - start) * 1000, 3)
                results.append(
                    {
                        "id": case["id"],
                        "task_type": case.get("task_type", ""),
                        "engine": "rdb",
                        "success": 0,
                        "latency_ms": latency_ms,
                        "row_count": 0,
                        "error_type": type(e).__name__,
                        "result_preview": str(e)[:500],
                        "result_preview_cn": "",
                    }
                )
    finally:
        conn.close()

    return results


def run_kg_eval_neo4j(cases, auth_data: dict):
    results = []

    driver = GraphDatabase.driver(
        auth_data["uri"],
        auth=(auth_data["user"], auth_data["pwd"]),
    )
    try:
        with driver.session(database=auth_data["database"]) as session:
            for case in cases:
                query = (case.get("sparql_or_graph_query") or "").strip()
                if not is_executable_query(query):
                    results.append(
                        {
                            "id": case["id"],
                            "task_type": case.get("task_type", ""),
                            "engine": "kg",
                            "success": 0,
                            "latency_ms": 0.0,
                            "row_count": 0,
                            "error_type": "not_executable",
                            "result_preview": "",
                            "result_preview_cn": "",
                        }
                    )
                    continue

                if re.search(r"PREFIX\\s|SELECT\\s+\\?", query, flags=re.IGNORECASE):
                    results.append(
                        {
                            "id": case["id"],
                            "task_type": case.get("task_type", ""),
                            "engine": "kg",
                            "success": 0,
                            "latency_ms": 0.0,
                            "row_count": 0,
                            "error_type": "not_cypher_query",
                            "result_preview": "当前 KG 路径已改为 Neo4j/Cypher，请将题库中的 KG 查询改为 Cypher。",
                            "result_preview_cn": "当前 KG 路径已改为 Neo4j/Cypher，请将题库中的 KG 查询改为 Cypher。",
                        }
                    )
                    continue

                start = time.perf_counter()
                try:
                    result = session.run(query)
                    records = result.data()
                    summary = result.consume()
                    server_available = getattr(summary, "result_available_after", None)
                    server_consumed = getattr(summary, "result_consumed_after", None)
                    if server_available is not None and server_consumed is not None:
                        latency_ms = round(float(server_available) + float(server_consumed), 3)
                    else:
                        latency_ms = round((time.perf_counter() - start) * 1000, 3)
                    preview = records[:5]
                    preview_cn = _preview_to_cn(preview)
                    results.append(
                        {
                            "id": case["id"],
                            "task_type": case.get("task_type", ""),
                            "engine": "kg",
                            "success": 1,
                            "latency_ms": latency_ms,
                            "row_count": len(records),
                            "error_type": "",
                            "result_preview": json.dumps(preview, ensure_ascii=False),
                            "result_preview_cn": json.dumps(preview_cn, ensure_ascii=False),
                        }
                    )
                except Exception as e:
                    latency_ms = round((time.perf_counter() - start) * 1000, 3)
                    results.append(
                        {
                            "id": case["id"],
                            "task_type": case.get("task_type", ""),
                            "engine": "kg",
                            "success": 0,
                            "latency_ms": latency_ms,
                            "row_count": 0,
                            "error_type": type(e).__name__,
                            "result_preview": str(e)[:500],
                            "result_preview_cn": "",
                        }
                    )
    finally:
        driver.close()

    return results


def write_csv(path: Path, rows, fieldnames):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_combined_rows(cases, rdb_rows, kg_rows):
    rdb_map = {r["id"]: r for r in rdb_rows}
    kg_map = {r["id"]: r for r in kg_rows}
    combined = []
    for case in cases:
        cid = case["id"]
        r = rdb_map.get(cid, {})
        k = kg_map.get(cid, {})
        combined.append(
            {
                "id": cid,
                "task_type": case.get("task_type", ""),
                "sql_success": r.get("success", 0),
                "kg_success": k.get("success", 0),
                "sql_latency_ms": r.get("latency_ms", 0.0),
                "kg_latency_ms": k.get("latency_ms", 0.0),
                "sql_row_count": r.get("row_count", 0),
                "kg_row_count": k.get("row_count", 0),
                "error_type_sql": r.get("error_type", ""),
                "error_type_kg": k.get("error_type", ""),
            }
        )
    return combined


def main():
    parser = argparse.ArgumentParser(description="Run RDB vs KG evaluation queries.")
    parser.add_argument(
        "--question-bank",
        type=Path,
        default=Path("docs/2026-04-09/rdb_vs_kg_eval_sample_cases_100.csv"),
        help="Question bank CSV path.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/company/poi.sqlite"),
        help="SQLite database path for RDB evaluation.",
    )
    parser.add_argument(
        "--neo4j-uri",
        type=str,
        default=os.getenv("NEO4J_URI"),
        help="Neo4j URI for KG evaluation.",
    )
    parser.add_argument(
        "--neo4j-database",
        type=str,
        default=os.getenv("NEO4J_DATABASE"),
        help="Neo4j database name.",
    )
    parser.add_argument(
        "--neo4j-user",
        type=str,
        default=os.getenv("NEO4J_USER"),
        help="Neo4j username.",
    )
    parser.add_argument(
        "--neo4j-pwd",
        type=str,
        default=os.getenv("NEO4J_PWD"),
        help="Neo4j password.",
    )
    parser.add_argument(
        "--engine",
        choices=["rdb", "kg", "both"],
        default="both",
        help="Which engine to execute.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/eval"),
        help="Output directory.",
    )
    args = parser.parse_args()

    cases = load_cases(args.question_bank)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    rdb_rows = []
    kg_rows = []

    if args.engine in ("rdb", "both"):
        rdb_rows = run_rdb_eval(cases, args.db_path)
        write_csv(
            args.out_dir / "eval_rdb_results.csv",
            rdb_rows,
            ["id", "task_type", "engine", "success", "latency_ms", "row_count", "error_type", "result_preview", "result_preview_cn"],
        )
        print(f"RDB results saved: {args.out_dir / 'eval_rdb_results.csv'}")

    if args.engine in ("kg", "both"):
        auth_data = {
            "uri": args.neo4j_uri,
            "database": args.neo4j_database,
            "user": args.neo4j_user,
            "pwd": args.neo4j_pwd,
        }
        if not all(auth_data.values()):
            raise ValueError("Neo4j 连接信息不完整，请检查参数或环境变量。")

        kg_rows = run_kg_eval_neo4j(cases, auth_data)
        write_csv(
            args.out_dir / "eval_kg_results.csv",
            kg_rows,
            ["id", "task_type", "engine", "success", "latency_ms", "row_count", "error_type", "result_preview", "result_preview_cn"],
        )
        print(f"KG results saved: {args.out_dir / 'eval_kg_results.csv'}")

    if args.engine == "both":
        combined_rows = build_combined_rows(cases, rdb_rows, kg_rows)
        write_csv(
            args.out_dir / "eval_rdb_kg_combined.csv",
            combined_rows,
            [
                "id",
                "task_type",
                "sql_success",
                "kg_success",
                "sql_latency_ms",
                "kg_latency_ms",
                "sql_row_count",
                "kg_row_count",
                "error_type_sql",
                "error_type_kg",
            ],
        )
        print(f"Combined results saved: {args.out_dir / 'eval_rdb_kg_combined.csv'}")


if __name__ == "__main__":
    main()
