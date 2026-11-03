from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict, deque
from pathlib import Path


DB_PATH = Path("data/company/zhongshan_10k.sqlite")
OUT_PATH = Path("docs/2026-05-13/rdb_vs_kg_eval_business_kg_advantage_property_graph_v4_dedup.csv")


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
    ordered_keys = sorted(buckets.keys(), key=lambda x: (str(x) if x is not None else ""))
    result = []
    while any(buckets.values()):
        for key in ordered_keys:
            if buckets[key]:
                result.append(buckets[key].popleft())
    return result


def pick_unique(rows, count, used_keys, key_fn):
    picked = []
    for row in rows:
        key = key_fn(row)
        if not key or key in used_keys:
            continue
        used_keys.add(key)
        picked.append(row)
        if len(picked) >= count:
            break
    return picked


def add_case(rows, used_q, cid, task_type, difficulty, question, sql, cypher, notes):
    if question in used_q:
        return False
    used_q.add(question)
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


def build():
    conn = sqlite3.connect(DB_PATH)
    try:
        roadcross_all = read_rows(
            conn,
            """
            SELECT gid, LKMC, SZDLDM, ZDGADM, XZQHMC, LKLX
            FROM zs_roadcross_dm
            WHERE LKMC IS NOT NULL AND TRIM(LKMC) <> ''
            ORDER BY gid
            """,
        )
        street_all = read_rows(
            conn,
            """
            SELECT s.gid, s.DM, s.MC, s.GADM, s.FLMC,
                   COUNT(DISTINCT r.gid) AS roadcross_cnt,
                   COUNT(DISTINCT m.gid) AS doorplate_cnt
            FROM zs_street_bz s
            LEFT JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0
            LEFT JOIN zs_MP_bz m ON m.S_DM = s.DM AND m.S_FLDM = 'B-DZ-006'
            WHERE s.MC IS NOT NULL AND TRIM(s.MC) <> ''
            GROUP BY s.gid, s.DM, s.MC, s.GADM, s.FLMC
            ORDER BY roadcross_cnt DESC, doorplate_cnt DESC, s.MC
            """,
        )
        doorplate_all = read_rows(
            conn,
            """
            SELECT gid, DM, MPQC, S_DM, GADM
            FROM zs_MP_bz
            WHERE MPQC IS NOT NULL AND TRIM(MPQC) <> ''
            ORDER BY gid
            """,
        )
        building_all = read_rows(
            conn,
            """
            SELECT gid, DM, DHQC, S_DM, GADM
            FROM zs_DH_bz
            WHERE DHQC IS NOT NULL AND TRIM(DHQC) <> ''
            ORDER BY gid
            """,
        )
        pcs_all = read_rows(
            conn,
            """
            SELECT p.gid, p.DM, p.MC, p.S_DM,
                   COUNT(DISTINCT r.gid) AS direct_roadcross_cnt
            FROM zs_PCS_xq p
            LEFT JOIN zs_roadcross_dm r ON r.ZDGADM = p.DM
            WHERE p.MC IS NOT NULL AND TRIM(p.MC) <> ''
            GROUP BY p.gid, p.DM, p.MC, p.S_DM
            ORDER BY direct_roadcross_cnt DESC, p.MC
            """,
        )
        branch_all = read_rows(
            conn,
            """
            SELECT b.gid, b.DM, b.MC,
                   COUNT(DISTINCT r.gid) AS roadcross_cnt
            FROM zs_GAFJ_xq b
            JOIN zs_PCS_xq p ON p.S_DM = b.DM
            JOIN zs_street_bz s ON s.GADM = p.DM
            JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0
            WHERE b.MC IS NOT NULL AND TRIM(b.MC) <> ''
            GROUP BY b.gid, b.DM, b.MC
            ORDER BY roadcross_cnt DESC, b.MC
            """,
        )

        roadcross_all = interleave(roadcross_all, lambda r: r.get("ZDGADM") or r.get("XZQHMC") or "")
        street_all = interleave(street_all, lambda r: r.get("GADM") or r.get("FLMC") or "")
        doorplate_all = interleave(doorplate_all, lambda r: r.get("GADM") or "")
        building_all = interleave(building_all, lambda r: r.get("GADM") or "")
        pcs_all = interleave(pcs_all, lambda r: r.get("S_DM") or "")
        branch_all = interleave(branch_all, lambda r: r.get("DM") or "")

        used_primary = set()
        used_questions = set()
        rows = []

        def take(pool, count, key_fn):
            return pick_unique(pool, count, used_primary, key_fn)

        # 30 lookup cases
        for i, row in enumerate(take(roadcross_all, 12, lambda r: f"roadcross:{r['gid']}"), 1):
            road = row["LKMC"]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0 WHERE r.LKMC='{esc_sql(road)}' ORDER BY s.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name ORDER BY street_name"
            add_case(rows, used_questions, f"U{i:03d}", "specific_entity_lookup", "medium", f"{road}关联了哪些街路？", sql, cypher, f"signature=roadcross:{row['gid']}")

        for i, row in enumerate(take(street_all, 6, lambda r: f"street:{r['gid']}"), 1):
            street = row["MC"]
            sql = f"SELECT s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_street_bz s JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0 WHERE s.MC='{esc_sql(street)}' ORDER BY r.LKMC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}}) WHERE s.name = '{esc_cypher(street)}' RETURN s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            add_case(rows, used_questions, f"U{12+i:03d}", "specific_entity_lookup", "medium", f"{street}关联的道路路口有哪些？", sql, cypher, f"signature=street:{row['gid']}")

        for i, row in enumerate(take(doorplate_all, 6, lambda r: f"doorplate:{r['gid']}"), 1):
            door = row["MPQC"]
            sql = f"SELECT m.MPQC AS doorplate_name, s.MC AS street_name FROM zs_MP_bz m JOIN zs_street_bz s ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' WHERE m.MPQC='{esc_sql(door)}';"
            cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}}) WHERE m.zs_MP_bz__MPQC = '{esc_cypher(door)}' RETURN m.zs_MP_bz__MPQC AS doorplate_name, s.name AS street_name"
            add_case(rows, used_questions, f"U{18+i:03d}", "specific_entity_lookup", "medium", f"{door}挂接在哪条街路上？", sql, cypher, f"signature=doorplate:{row['gid']}")

        for i, row in enumerate(take(building_all, 4, lambda r: f"building:{r['gid']}"), 1):
            building = row["DHQC"]
            sql = f"SELECT d.DHQC AS building_name, p.MC AS police_name, b.MC AS branch_name FROM zs_DH_bz d JOIN zs_PCS_xq p ON d.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE d.DHQC='{esc_sql(building)}';"
            cypher = f"MATCH (d:Entity {{sourceTable:'zs_DH_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE d.zs_DH_bz__DHQC = '{esc_cypher(building)}' RETURN d.zs_DH_bz__DHQC AS building_name, p.name AS police_name, b.name AS branch_name"
            add_case(rows, used_questions, f"U{24+i:03d}", "specific_entity_lookup", "medium", f"{building}由哪个派出所和分局管理？", sql, cypher, f"signature=building:{row['gid']}")

        for i, row in enumerate(take(pcs_all, 2, lambda r: f"pcs:{r['gid']}"), 1):
            pcs = row["MC"]
            sql = f"SELECT p.MC AS police_name, COUNT(r.gid) AS roadcross_cnt FROM zs_PCS_xq p JOIN zs_roadcross_dm r ON r.ZDGADM=p.DM WHERE p.MC='{esc_sql(pcs)}' GROUP BY p.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}}) WHERE p.name = '{esc_cypher(pcs)}' RETURN p.name AS police_name, count(r) AS roadcross_cnt"
            add_case(rows, used_questions, f"U{28+i:03d}", "specific_entity_lookup", "medium", f"{pcs}直接管理了多少个道路路口？", sql, cypher, f"signature=pcs:{row['gid']}")

        # 24 relation path cases
        for i, row in enumerate(take(roadcross_all[12:], 10, lambda r: f"roadcross_path:{r['gid']}"), 1):
            road = row["LKMC"]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0 JOIN zs_PCS_xq p ON s.GADM=p.DM WHERE r.LKMC='{esc_sql(road)}' ORDER BY s.MC, p.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name ORDER BY street_name, police_name"
            add_case(rows, used_questions, f"P{i:03d}", "specific_relation_path", "hard", f"{road}通过街路归属到哪个派出所？", sql, cypher, f"signature=roadcross_path:{row['gid']}")

        for i, row in enumerate(take(roadcross_all[22:], 6, lambda r: f"roadcross_branch:{r['gid']}"), 1):
            road = row["LKMC"]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road)}' ORDER BY s.MC, p.MC, b.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, b.name AS branch_name ORDER BY street_name, police_name, branch_name"
            add_case(rows, used_questions, f"P{10+i:03d}", "specific_relation_path", "hard", f"{road}通过街路和派出所最终归属到哪个公安分局？", sql, cypher, f"signature=roadcross_branch:{row['gid']}")

        for i, row in enumerate(take(doorplate_all[6:], 5, lambda r: f"doorplate_path:{r['gid']}"), 1):
            door = row["MPQC"]
            sql = f"SELECT m.MPQC AS doorplate_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_MP_bz m JOIN zs_street_bz s ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE m.MPQC='{esc_sql(door)}' ORDER BY r.LKMC;"
            cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE m.zs_MP_bz__MPQC = '{esc_cypher(door)}' RETURN m.zs_MP_bz__MPQC AS doorplate_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            add_case(rows, used_questions, f"P{16+i:03d}", "specific_relation_path", "hard", f"{door}所在的街路附近有哪些道路路口？", sql, cypher, f"signature=doorplate_path:{row['gid']}")

        for i, row in enumerate(take(building_all[4:], 3, lambda r: f"building_path:{r['gid']}"), 1):
            building = row["DHQC"]
            sql = f"SELECT d.DHQC AS building_name, p.MC AS police_name, b.MC AS branch_name FROM zs_DH_bz d JOIN zs_PCS_xq p ON d.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE d.DHQC='{esc_sql(building)}';"
            cypher = f"MATCH (d:Entity {{sourceTable:'zs_DH_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE d.zs_DH_bz__DHQC = '{esc_cypher(building)}' RETURN d.zs_DH_bz__DHQC AS building_name, p.name AS police_name, b.name AS branch_name"
            add_case(rows, used_questions, f"P{21+i:03d}", "specific_relation_path", "hard", f"{building}由哪个派出所和分局管理？", sql, cypher, f"signature=building_path:{row['gid']}")

        # 25 aggregation cases
        for i, row in enumerate(take(street_all, 10, lambda r: f"street_agg:{r['gid']}"), 1):
            street = row["MC"]
            sql = f"SELECT s.MC AS street_name, COUNT(r.gid) AS roadcross_cnt FROM zs_street_bz s JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE s.MC='{esc_sql(street)}' GROUP BY s.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}}) WHERE s.name = '{esc_cypher(street)}' RETURN s.name AS street_name, count(r) AS roadcross_cnt"
            add_case(rows, used_questions, f"A{i:03d}", "specific_aggregation", "hard", f"{street}一共关联了多少个道路路口？", sql, cypher, f"signature=street_agg:{row['gid']}")

        for i, row in enumerate(take(pcs_all[2:], 6, lambda r: f"pcs_agg:{r['gid']}"), 1):
            pcs = row["MC"]
            sql = f"SELECT p.MC AS police_name, COUNT(r.gid) AS roadcross_cnt FROM zs_PCS_xq p JOIN zs_roadcross_dm r ON r.ZDGADM=p.DM WHERE p.MC='{esc_sql(pcs)}' GROUP BY p.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}}) WHERE p.name = '{esc_cypher(pcs)}' RETURN p.name AS police_name, count(r) AS roadcross_cnt"
            add_case(rows, used_questions, f"A{10+i:03d}", "specific_aggregation", "medium", f"{pcs}直接管理了多少个道路路口？", sql, cypher, f"signature=pcs_agg:{row['gid']}")

        for i, row in enumerate(take(branch_all, 5, lambda r: f"branch_agg:{r['gid']}"), 1):
            branch = row["MC"]
            sql = f"SELECT b.MC AS branch_name, COUNT(r.gid) AS roadcross_cnt FROM zs_GAFJ_xq b JOIN zs_PCS_xq p ON p.S_DM=b.DM JOIN zs_street_bz s ON s.GADM=p.DM JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE b.MC='{esc_sql(branch)}' GROUP BY b.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE b.name = '{esc_cypher(branch)}' RETURN b.name AS branch_name, count(r) AS roadcross_cnt"
            add_case(rows, used_questions, f"A{16+i:03d}", "specific_aggregation", "hard", f"{branch}下通过街路关联到的道路路口有多少个？", sql, cypher, f"signature=branch_agg:{row['gid']}")

        for i, row in enumerate(take(street_all[10:], 4, lambda r: f"street_doorplate_agg:{r['gid']}"), 1):
            street = row["MC"]
            sql = f"SELECT s.MC AS street_name, COUNT(m.gid) AS doorplate_cnt FROM zs_street_bz s JOIN zs_MP_bz m ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' WHERE s.MC='{esc_sql(street)}' GROUP BY s.MC;"
            cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}}) WHERE s.name = '{esc_cypher(street)}' RETURN s.name AS street_name, count(m) AS doorplate_cnt"
            add_case(rows, used_questions, f"A{21+i:03d}", "specific_aggregation", "hard", f"{street}挂接了多少个门牌？", sql, cypher, f"signature=street_doorplate_agg:{row['gid']}")

        for i, row in enumerate(take(branch_all[5:], 1, lambda r: f"branch_direct_agg:{r['gid']}"), 1):
            branch = row["MC"]
            sql = f"SELECT b.MC AS branch_name, COUNT(r.gid) AS roadcross_cnt FROM zs_GAFJ_xq b JOIN zs_PCS_xq p ON p.S_DM=b.DM JOIN zs_street_bz s ON s.GADM=p.DM JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE b.MC='{esc_sql(branch)}' GROUP BY b.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE b.name = '{esc_cypher(branch)}' RETURN b.name AS branch_name, count(r) AS roadcross_cnt"
            add_case(rows, used_questions, f"A{25+i:03d}", "specific_aggregation", "hard", f"{branch}下通过街路链路关联了多少个道路路口？", sql, cypher, f"signature=branch_direct_agg:{row['gid']}")

        # 10 address context cases
        for i, row in enumerate(take(doorplate_all[11:], 6, lambda r: f"doorplate_ctx:{r['gid']}"), 1):
            door = row["MPQC"]
            sql = f"SELECT m.MPQC AS doorplate_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_MP_bz m JOIN zs_street_bz s ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE m.MPQC='{esc_sql(door)}' ORDER BY r.LKMC;"
            cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE m.zs_MP_bz__MPQC = '{esc_cypher(door)}' RETURN m.zs_MP_bz__MPQC AS doorplate_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            add_case(rows, used_questions, f"D{i:03d}", "specific_address_context", "hard", f"从{door}出发，能通过所在街路找到哪些道路路口？", sql, cypher, f"signature=doorplate_ctx:{row['gid']}")

        for i, row in enumerate(take(building_all[7:], 4, lambda r: f"building_ctx:{r['gid']}"), 1):
            building = row["DHQC"]
            sql = f"SELECT d.DHQC AS building_name, p.MC AS police_name, b.MC AS branch_name FROM zs_DH_bz d JOIN zs_PCS_xq p ON d.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE d.DHQC='{esc_sql(building)}';"
            cypher = f"MATCH (d:Entity {{sourceTable:'zs_DH_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE d.zs_DH_bz__DHQC = '{esc_cypher(building)}' RETURN d.zs_DH_bz__DHQC AS building_name, p.name AS police_name, b.name AS branch_name"
            add_case(rows, used_questions, f"D{6+i:03d}", "specific_address_context", "hard", f"{building}由哪个派出所和分局管理？", sql, cypher, f"signature=building_ctx:{row['gid']}")

        # 10 explainability cases
        for i, row in enumerate(take(roadcross_all[28:], 10, lambda r: f"explain:{r['gid']}"), 1):
            road = row["LKMC"]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM WHERE r.LKMC='{esc_sql(road)}' LIMIT 1;"
            cypher = f"MATCH path=(r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, [x IN nodes(path) | x.name] AS path_nodes, [rel IN relationships(path) | type(rel)] AS path_rels LIMIT 1"
            add_case(rows, used_questions, f"E{i:03d}", "explainability", "hard", f"为什么{road}归属于对应派出所？请返回路口到街路再到派出所的路径。", sql, cypher, f"signature=explain:{row['gid']}")

        assert len(rows) == 100, len(rows)
        assert len(used_questions) == 100, len(used_questions)
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUT_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "id",
                    "task_type",
                    "difficulty",
                    "nl_question",
                    "sql_query",
                    "sparql_or_graph_query",
                    "gold_answer_json",
                    "notes",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        print(OUT_PATH)
        print(f"rows={len(rows)} unique_questions={len(used_questions)}")
    finally:
        conn.close()


if __name__ == "__main__":
    build()
