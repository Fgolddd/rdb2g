from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict, deque
from itertools import cycle
from pathlib import Path


DB_PATH = Path("data/company/zhongshan_10k.sqlite")
OUT_PATH = Path("docs/2026-05-13/rdb_vs_kg_eval_business_kg_advantage_property_graph_v7_kg_advantage.csv")


def esc_sql(text: str) -> str:
    return str(text).replace("'", "''")


def esc_cypher(text: str) -> str:
    return str(text).replace("\\", "\\\\").replace("'", "\\'")


def read_rows(conn: sqlite3.Connection, query: str):
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(query).fetchall()]


def interleave(rows, key_fn):
    buckets = defaultdict(deque)
    for row in rows:
        buckets[key_fn(row)].append(row)
    keys = sorted(buckets.keys(), key=lambda x: str(x))
    out = []
    while any(buckets.values()):
        for key in keys:
            if buckets[key]:
                out.append(buckets[key].popleft())
    return out


def build_address_index(rows):
    index = defaultdict(lambda: defaultdict(deque))
    for row in rows:
        index[row["kind"]][row["street_dm"]].append(row)
    return index


def pop_address_for_roadcross(roadcross_row, address_index, kind, used_address_ids):
    street_codes = [s.strip() for s in str(roadcross_row.get("SZDLDM") or "").split(",") if s.strip()]
    for street_dm in street_codes:
        bucket = address_index[kind].get(street_dm)
        if not bucket:
            continue
        while bucket and bucket[0]["gid"] in used_address_ids:
            bucket.popleft()
        if bucket:
            addr = bucket.popleft()
            used_address_ids.add(addr["gid"])
            return addr, street_dm
    return None, None


def add_case(rows, used_questions, used_signatures, cid, task_type, difficulty, question, sql, cypher, signature, notes):
    if question in used_questions or signature in used_signatures:
        return False
    used_questions.add(question)
    used_signatures.add(signature)
    rows.append(
        {
            "id": cid,
            "task_type": task_type,
            "difficulty": difficulty,
            "nl_question": question,
            "sql_query": sql,
            "sparql_or_graph_query": cypher,
            "gold_answer_json": "",
            "notes": notes,
        }
    )
    return True


def fetch_roadcross_pool(conn: sqlite3.Connection):
    rows = read_rows(
        conn,
        """
        SELECT r.gid, r.LKMC, r.SZDLDM, r.XZQHMC,
               COUNT(DISTINCT s.DM) AS street_cnt,
               COUNT(DISTINCT p.DM) AS pcs_cnt,
               COUNT(DISTINCT b.DM) AS branch_cnt
        FROM zs_roadcross_dm r
        JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0
        JOIN zs_PCS_xq p ON s.GADM = p.DM
        JOIN zs_GAFJ_xq b ON p.S_DM = b.DM
        WHERE r.LKMC IS NOT NULL AND TRIM(r.LKMC) <> ''
        GROUP BY r.gid, r.LKMC, r.SZDLDM, r.XZQHMC
        HAVING COUNT(DISTINCT s.DM) > 0
        ORDER BY street_cnt DESC, pcs_cnt DESC, branch_cnt DESC, r.gid
        """,
    )
    return interleave(rows, lambda r: r.get("XZQHMC") or "")


def fetch_address_rows(conn: sqlite3.Connection, table: str, name_field: str, kind: str, where_level: str):
    rows = read_rows(
        conn,
        f"""
        SELECT x.gid, x.DM, x.{name_field} AS name, x.S_DM AS street_dm, x.GADM AS gadm,
               COUNT(DISTINCT r.gid) AS roadcross_cnt
        FROM {table} x
        LEFT JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||x.S_DM||',') > 0
        WHERE x.{name_field} IS NOT NULL AND TRIM(x.{name_field}) <> ''
          AND x.S_FLDM = '{where_level}'
        GROUP BY x.gid, x.DM, x.{name_field}, x.S_DM, x.GADM
        ORDER BY roadcross_cnt DESC, x.{name_field}, x.gid
        """,
    )
    for row in rows:
        row["kind"] = kind
    return interleave(rows, lambda r: r.get("gadm") or "")


def build():
    conn = sqlite3.connect(DB_PATH)
    try:
        roadcross_pool = fetch_roadcross_pool(conn)
        address_rows = []
        address_rows += fetch_address_rows(conn, "zs_MP_bz", "MPQC", "doorplate", "B-DZ-006")
        address_rows += fetch_address_rows(conn, "zs_DH_bz", "DHQC", "building", "B-DZ-008")
        address_rows += fetch_address_rows(conn, "zs_DY_bz", "DYHQC", "unit", "B-DZ-009")
        address_rows += fetch_address_rows(conn, "zs_FJ_bz", "FJHQC", "room", "B-DZ-011")
        address_rows += fetch_address_rows(conn, "zs_YL_bz", "YLQC", "yard", "B-DZ-007")
        address_index = build_address_index(address_rows)

        used_questions = set()
        used_signatures = set()
        used_address_ids = set()
        rows = []

        branch_cycle = cycle([
            "final_branch",
            "count_branch",
            "list_path",
        ])
        addr_kind_cycle = cycle(["doorplate", "building", "unit", "room", "yard"])

        # 45: 路口 -> 街路 -> 派出所 -> 分局
        used_roadcross = set()
        picked = 0
        for road in roadcross_pool:
            if picked >= 45:
                break
            if road["gid"] in used_roadcross:
                continue
            used_roadcross.add(road["gid"])
            mode = next(branch_cycle)
            road_name = road["LKMC"]
            if mode == "final_branch":
                sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' ORDER BY s.MC, p.MC, b.MC;"
                cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, b.name AS branch_name ORDER BY street_name, police_name, branch_name"
                question = f"{road_name}通过街路和派出所最终归属到哪个公安分局？"
            elif mode == "count_branch":
                sql = f"SELECT r.LKMC AS roadcross_name, COUNT(DISTINCT s.DM) AS street_cnt, COUNT(DISTINCT p.DM) AS police_cnt, COUNT(DISTINCT b.DM) AS branch_cnt FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' GROUP BY r.LKMC;"
                cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, count(DISTINCT s) AS street_cnt, count(DISTINCT p) AS police_cnt, count(DISTINCT b) AS branch_cnt"
                question = f"{road_name}通过街路关联到了多少个街路、派出所和分局？"
            else:
                sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' ORDER BY s.MC, p.MC, b.MC;"
                cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, b.name AS branch_name ORDER BY street_name, police_name, branch_name"
                question = f"{road_name}对应的街路、派出所和分局分别有哪些？"
            add_case(rows, used_questions, used_signatures, f"K1{picked+1:03d}", "roadcross_police_branch", "hard", question, sql, cypher, f"roadcross:{road['gid']}", "路口-街路-派出所-分局")
            picked += 1

        # 25: 路口 -> 街路 <- 门牌/楼栋/单元/房间/院落
        addr_types = cycle(["doorplate", "building", "unit", "room", "yard"])
        picked = 0
        for road in roadcross_pool:
            if picked >= 25:
                break
            if road["gid"] in used_roadcross:
                continue
            kind = next(addr_types)
            addr, street_dm = pop_address_for_roadcross(road, address_index, kind, used_address_ids)
            if not addr:
                continue
            used_roadcross.add(road["gid"])
            road_name = road["LKMC"]
            addr_name = addr["name"]
            type_cn = {"doorplate": "门牌", "building": "楼栋", "unit": "单元", "room": "房间", "yard": "院落"}[kind]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, a.{ 'MPQC' if kind=='doorplate' else 'DHQC' if kind=='building' else 'DYHQC' if kind=='unit' else 'FJHQC' if kind=='room' else 'YLQC' } AS address_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN { 'zs_MP_bz' if kind=='doorplate' else 'zs_DH_bz' if kind=='building' else 'zs_DY_bz' if kind=='unit' else 'zs_FJ_bz' if kind=='room' else 'zs_YL_bz' } a ON a.S_DM=s.DM WHERE r.LKMC='{esc_sql(road_name)}' AND a.{ 'MPQC' if kind=='doorplate' else 'DHQC' if kind=='building' else 'DYHQC' if kind=='unit' else 'FJHQC' if kind=='room' else 'YLQC' }='{esc_sql(addr_name)}' ORDER BY street_name;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:PART_OF_ADDRESS_ENTITY]-(a:Entity {{sourceTable:'{ 'zs_MP_bz' if kind=='doorplate' else 'zs_DH_bz' if kind=='building' else 'zs_DY_bz' if kind=='unit' else 'zs_FJ_bz' if kind=='room' else 'zs_YL_bz' }'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' AND a.name = '{esc_cypher(addr_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, a.name AS address_name ORDER BY street_name"
            question = f"{road_name}所在街路附近有哪些{type_cn}？"
            add_case(rows, used_questions, used_signatures, f"K2{picked+1:03d}", "roadcross_address", "hard", question, sql, cypher, f"roadcross_address:{road['gid']}:{addr['gid']}", f"路口-街路-{type_cn}")
            picked += 1

        # 15: 路口 -> 街路 -> 门牌/楼栋/单元/房间/院落 -> 派出所/分局
        addr_types = cycle(["doorplate", "building", "unit", "room", "yard"])
        picked = 0
        for road in roadcross_pool:
            if picked >= 15:
                break
            if road["gid"] in used_roadcross:
                continue
            kind = next(addr_types)
            addr, street_dm = pop_address_for_roadcross(road, address_index, kind, used_address_ids)
            if not addr:
                continue
            used_roadcross.add(road["gid"])
            road_name = road["LKMC"]
            addr_name = addr["name"]
            table = {"doorplate": "zs_MP_bz", "building": "zs_DH_bz", "unit": "zs_DY_bz", "room": "zs_FJ_bz", "yard": "zs_YL_bz"}[kind]
            field = {"doorplate": "MPQC", "building": "DHQC", "unit": "DYHQC", "room": "FJHQC", "yard": "YLQC"}[kind]
            type_cn = {"doorplate": "门牌", "building": "楼栋", "unit": "单元", "room": "房间", "yard": "院落"}[kind]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, a.{field} AS address_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN {table} a ON a.S_DM=s.DM JOIN zs_PCS_xq p ON a.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' AND a.{field}='{esc_sql(addr_name)}' ORDER BY street_name, police_name, branch_name;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:PART_OF_ADDRESS_ENTITY]-(a:Entity {{sourceTable:'{table}'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' AND a.name = '{esc_cypher(addr_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, a.name AS address_name, p.name AS police_name, b.name AS branch_name ORDER BY street_name, police_name, branch_name"
            question = f"{road_name}所在街路上的{addr_name}由哪个派出所和分局管理？"
            add_case(rows, used_questions, used_signatures, f"K3{picked+1:03d}", "roadcross_address_police_branch", "hard", question, sql, cypher, f"roadcross_address_police:{road['gid']}:{addr['gid']}", f"路口-街路-{type_cn}-派出所-分局")
            picked += 1

        # 10: explainability
        picked = 0
        explain_templates = [
            ("roadcross_branch", "为什么{entity}归属到对应派出所？请返回路口到街路再到派出所和分局的路径。"),
            ("roadcross_police", "为什么{entity}会归属到对应公安分局？请返回路口到街路再到派出所和分局的路径。"),
        ]
        for road in roadcross_pool:
            if picked >= 10:
                break
            if road["gid"] in used_roadcross:
                continue
            used_roadcross.add(road["gid"])
            road_name = road["LKMC"]
            template_key, tpl = explain_templates[picked % 2]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' LIMIT 1;"
            cypher = f"MATCH path=(r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, b.name AS branch_name, [x IN nodes(path) | x.name] AS path_nodes, [rel IN relationships(path) | type(rel)] AS path_rels LIMIT 1"
            question = tpl.format(entity=road_name)
            add_case(rows, used_questions, used_signatures, f"K4{picked+1:03d}", "explainability", "hard", question, sql, cypher, f"explain_roadcross:{road['gid']}", template_key)
            picked += 1

        # 5: complex cross
        picked = 0
        for road in roadcross_pool:
            if picked >= 5:
                break
            if road["gid"] in used_roadcross:
                continue
            kind = next(addr_types)
            addr, street_dm = pop_address_for_roadcross(road, address_index, kind, used_address_ids)
            if not addr:
                continue
            used_roadcross.add(road["gid"])
            road_name = road["LKMC"]
            addr_name = addr["name"]
            table = {"doorplate": "zs_MP_bz", "building": "zs_DH_bz", "unit": "zs_DY_bz", "room": "zs_FJ_bz", "yard": "zs_YL_bz"}[kind]
            field = {"doorplate": "MPQC", "building": "DHQC", "unit": "DYHQC", "room": "FJHQC", "yard": "YLQC"}[kind]
            type_cn = {"doorplate": "门牌", "building": "楼栋", "unit": "单元", "room": "房间", "yard": "院落"}[kind]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, a.{field} AS address_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN {table} a ON a.S_DM=s.DM JOIN zs_PCS_xq p ON a.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road_name)}' AND a.{field}='{esc_sql(addr_name)}' LIMIT 1;"
            cypher = f"MATCH path=(r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:PART_OF_ADDRESS_ENTITY]-(a:Entity {{sourceTable:'{table}'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road_name)}' AND a.name = '{esc_cypher(addr_name)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, a.name AS address_name, p.name AS police_name, b.name AS branch_name, [x IN nodes(path) | x.name] AS path_nodes, [rel IN relationships(path) | type(rel)] AS path_rels LIMIT 1"
            question = f"{road_name}所在街路上的{addr_name}由哪个派出所和分局管理，并返回完整路径？"
            add_case(rows, used_questions, used_signatures, f"K5{picked+1:03d}", "complex_cross", "hard", question, sql, cypher, f"complex:{road['gid']}:{addr['gid']}", f"路口-街路-{type_cn}-派出所-分局-路径")
            picked += 1

        assert len(rows) == 100, len(rows)
        assert len(used_questions) == 100, len(used_questions)
        assert len(used_signatures) == 100, len(used_signatures)

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUT_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["id", "task_type", "difficulty", "nl_question", "sql_query", "sparql_or_graph_query", "gold_answer_json", "notes"],
            )
            writer.writeheader()
            writer.writerows(rows)
        print(OUT_PATH)
        print(f"rows={len(rows)} unique_questions={len(used_questions)} unique_signatures={len(used_signatures)}")
    finally:
        conn.close()


if __name__ == "__main__":
    build()
