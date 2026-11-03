from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict, deque
from pathlib import Path


DB_PATH = Path("data/company/zhongshan_10k.sqlite")
OUT_PATH = Path("docs/2026-05-13/rdb_vs_kg_eval_business_kg_advantage_property_graph_v5_multihop.csv")


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


def build():
    conn = sqlite3.connect(DB_PATH)
    try:
        roadcross = read_rows(
            conn,
            """
            SELECT gid, LKMC, SZDLDM, ZDGADM, XZQHMC, LKLX
            FROM zs_roadcross_dm
            WHERE LKMC IS NOT NULL AND TRIM(LKMC) <> ''
            ORDER BY gid
            """,
        )
        street = read_rows(
            conn,
            """
            SELECT s.gid, s.DM, s.MC, s.GADM,
                   COUNT(DISTINCT r.gid) AS roadcross_cnt,
                   COUNT(DISTINCT m.gid) AS doorplate_cnt
            FROM zs_street_bz s
            LEFT JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',') > 0
            LEFT JOIN zs_MP_bz m ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006'
            WHERE s.MC IS NOT NULL AND TRIM(s.MC) <> ''
            GROUP BY s.gid, s.DM, s.MC, s.GADM
            ORDER BY roadcross_cnt DESC, doorplate_cnt DESC, s.MC
            """,
        )
        doorplate = read_rows(
            conn,
            """
            SELECT gid, DM, MPQC, S_DM, GADM
            FROM zs_MP_bz
            WHERE MPQC IS NOT NULL AND TRIM(MPQC) <> ''
            ORDER BY gid
            """,
        )
        building = read_rows(
            conn,
            """
            SELECT gid, DM, DHQC, S_DM, GADM
            FROM zs_DH_bz
            WHERE DHQC IS NOT NULL AND TRIM(DHQC) <> ''
            ORDER BY gid
            """,
        )
        unit = read_rows(
            conn,
            """
            SELECT gid, DM, DYHQC, S_DM, GADM
            FROM zs_DY_bz
            WHERE DYHQC IS NOT NULL AND TRIM(DYHQC) <> ''
            ORDER BY gid
            """,
        )
        room = read_rows(
            conn,
            """
            SELECT gid, DM, FJHQC, S_DM, GADM
            FROM zs_FJ_bz
            WHERE FJHQC IS NOT NULL AND TRIM(FJHQC) <> ''
            ORDER BY gid
            """,
        )
        yard = read_rows(
            conn,
            """
            SELECT gid, DM, YLQC, S_DM, GADM
            FROM zs_YL_bz
            WHERE YLQC IS NOT NULL AND TRIM(YLQC) <> ''
            ORDER BY gid
            """,
        )
        community = read_rows(
            conn,
            """
            SELECT gid, DM, MC, GADM, S_DM
            FROM zs_community_bz
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )
        town = read_rows(
            conn,
            """
            SELECT gid, DM, MC, GADM, S_DM
            FROM zs_town_bz
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )
        police = read_rows(
            conn,
            """
            SELECT gid, DM, MC, S_DM
            FROM zs_PCS_xq
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )
        branch = read_rows(
            conn,
            """
            SELECT gid, DM, MC
            FROM zs_GAFJ_xq
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )
        jws = read_rows(
            conn,
            """
            SELECT gid, DM, MC, S_DM
            FROM zs_JWS_xq
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )
        city = read_rows(
            conn,
            """
            SELECT gid, DM, MC
            FROM zs_city_bz
            WHERE MC IS NOT NULL AND TRIM(MC) <> ''
            ORDER BY gid
            """,
        )

        roadcross = interleave(roadcross, lambda r: r.get("ZDGADM") or r.get("XZQHMC") or "")
        street = interleave(street, lambda r: r.get("GADM") or "")
        doorplate = interleave(doorplate, lambda r: r.get("GADM") or "")
        building = interleave(building, lambda r: r.get("GADM") or "")
        unit = interleave(unit, lambda r: r.get("GADM") or "")
        room = interleave(room, lambda r: r.get("GADM") or "")
        yard = interleave(yard, lambda r: r.get("GADM") or "")
        community = interleave(community, lambda r: r.get("GADM") or "")
        police = interleave(police, lambda r: r.get("S_DM") or "")
        branch = interleave(branch, lambda r: r.get("DM") or "")
        jws = interleave(jws, lambda r: r.get("S_DM") or "")
        city = interleave(city, lambda r: r.get("DM") or "")

        used_questions = set()
        used_signatures = set()
        rows = []

        # 1. 路口 -> 街路 -> 派出所 -> 分局  [25]
        for idx, row in enumerate(roadcross[:25], 1):
            road = row["LKMC"]
            sql = f"SELECT r.LKMC AS roadcross_name, s.MC AS street_name, p.MC AS police_name, b.MC AS branch_name FROM zs_roadcross_dm r JOIN zs_street_bz s ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 JOIN zs_PCS_xq p ON s.GADM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE r.LKMC='{esc_sql(road)}' ORDER BY s.MC, p.MC, b.MC;"
            cypher = f"MATCH (r:Entity {{sourceTable:'zs_roadcross_dm'}})-[:LOCATED_ON_STREET]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:MANAGED_BY_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE r.zs_roadcross_dm__LKMC = '{esc_cypher(road)}' RETURN r.zs_roadcross_dm__LKMC AS roadcross_name, s.name AS street_name, p.name AS police_name, b.name AS branch_name ORDER BY street_name, police_name, branch_name"
            add_case(rows, used_questions, used_signatures, f"M1{idx:03d}", "multihop_roadcross_police", "hard", f"{road}通过街路和派出所最终归属到哪个公安分局？", sql, cypher, f"roadcross_police_branch:{row['gid']}", "roadcross-street-police-branch")

        # 2. 路口 -> 街路 -> 门牌/楼栋/单元/房间/院落  [25]
        combo_pool = []
        for item in doorplate[:10]:
            combo_pool.append(("doorplate", item))
        for item in building[:5]:
            combo_pool.append(("building", item))
        for item in unit[:3]:
            combo_pool.append(("unit", item))
        for item in room[:3]:
            combo_pool.append(("room", item))
        for item in yard[:4]:
            combo_pool.append(("yard", item))
        for idx, (kind, row) in enumerate(combo_pool[:25], 1):
            if kind == "doorplate":
                entity = row["MPQC"]
                sql = f"SELECT m.MPQC AS address_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_MP_bz m JOIN zs_street_bz s ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE m.MPQC='{esc_sql(entity)}' ORDER BY r.LKMC;"
                cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE m.zs_MP_bz__MPQC = '{esc_cypher(entity)}' RETURN m.zs_MP_bz__MPQC AS address_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            elif kind == "building":
                entity = row["DHQC"]
                sql = f"SELECT d.DHQC AS address_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_DH_bz d JOIN zs_street_bz s ON d.S_DM=s.DM AND d.S_FLDM='B-DZ-008' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE d.DHQC='{esc_sql(entity)}' ORDER BY r.LKMC;"
                cypher = f"MATCH (d:Entity {{sourceTable:'zs_DH_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE d.zs_DH_bz__DHQC = '{esc_cypher(entity)}' RETURN d.zs_DH_bz__DHQC AS address_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            elif kind == "unit":
                entity = row["DYHQC"]
                sql = f"SELECT u.DYHQC AS address_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_DY_bz u JOIN zs_street_bz s ON u.S_DM=s.DM AND u.S_FLDM='B-DZ-009' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE u.DYHQC='{esc_sql(entity)}' ORDER BY r.LKMC;"
                cypher = f"MATCH (u:Entity {{sourceTable:'zs_DY_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE u.zs_DY_bz__DYHQC = '{esc_cypher(entity)}' RETURN u.zs_DY_bz__DYHQC AS address_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            elif kind == "room":
                entity = row["FJHQC"]
                sql = f"SELECT f.FJHQC AS address_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_FJ_bz f JOIN zs_street_bz s ON f.S_DM=s.DM AND f.S_FLDM='B-DZ-011' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE f.FJHQC='{esc_sql(entity)}' ORDER BY r.LKMC;"
                cypher = f"MATCH (f:Entity {{sourceTable:'zs_FJ_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE f.zs_FJ_bz__FJHQC = '{esc_cypher(entity)}' RETURN f.zs_FJ_bz__FJHQC AS address_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            else:
                entity = row["YLQC"]
                sql = f"SELECT y.YLQC AS address_name, s.MC AS street_name, r.LKMC AS roadcross_name FROM zs_YL_bz y JOIN zs_street_bz s ON y.S_DM=s.DM AND y.S_FLDM='B-DZ-007' JOIN zs_roadcross_dm r ON instr(','||r.SZDLDM||',', ','||s.DM||',')>0 WHERE y.YLQC='{esc_sql(entity)}' ORDER BY r.LKMC;"
                cypher = f"MATCH (y:Entity {{sourceTable:'zs_YL_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})<-[:LOCATED_ON_STREET]-(r:Entity {{sourceTable:'zs_roadcross_dm'}}) WHERE y.zs_YL_bz__YLQC = '{esc_cypher(entity)}' RETURN y.zs_YL_bz__YLQC AS address_name, s.name AS street_name, r.zs_roadcross_dm__LKMC AS roadcross_name ORDER BY roadcross_name"
            add_case(rows, used_questions, used_signatures, f"M2{idx:03d}", "multihop_address_street", "hard", f"{entity}所在的街路附近有哪些道路路口？", sql, cypher, f"address_street:{kind}:{row['gid']}", "address-street-roadcross")

        # 3. 门牌/楼栋/单元/房间/院落 -> 街路 -> 市  [20]
        admin_pool = []
        for item in doorplate[10:15]:
            admin_pool.append(("doorplate", item))
        for item in building[5:9]:
            admin_pool.append(("building", item))
        for item in unit[3:6]:
            admin_pool.append(("unit", item))
        for item in room[3:7]:
            admin_pool.append(("room", item))
        for item in yard[4:9]:
            admin_pool.append(("yard", item))
        for idx, (kind, row) in enumerate(admin_pool[:20], 1):
            if kind == "doorplate":
                entity = row["MPQC"]
                sql = f"SELECT m.MPQC AS address_name, city.MC AS city_name FROM zs_MP_bz m JOIN zs_street_bz s ON m.S_DM=s.DM AND m.S_FLDM='B-DZ-006' JOIN zs_city_bz city ON s.S_DM=city.DM WHERE m.MPQC='{esc_sql(entity)}';"
                cypher = f"MATCH (m:Entity {{sourceTable:'zs_MP_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE m.zs_MP_bz__MPQC = '{esc_cypher(entity)}' RETURN m.zs_MP_bz__MPQC AS address_name, city.name AS city_name"
            elif kind == "building":
                entity = row["DHQC"]
                sql = f"SELECT d.DHQC AS address_name, city.MC AS city_name FROM zs_DH_bz d JOIN zs_street_bz s ON d.S_DM=s.DM AND d.S_FLDM='B-DZ-008' JOIN zs_city_bz city ON s.S_DM=city.DM WHERE d.DHQC='{esc_sql(entity)}';"
                cypher = f"MATCH (d:Entity {{sourceTable:'zs_DH_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE d.zs_DH_bz__DHQC = '{esc_cypher(entity)}' RETURN d.zs_DH_bz__DHQC AS address_name, city.name AS city_name"
            elif kind == "unit":
                entity = row["DYHQC"]
                sql = f"SELECT u.DYHQC AS address_name, city.MC AS city_name FROM zs_DY_bz u JOIN zs_street_bz s ON u.S_DM=s.DM AND u.S_FLDM='B-DZ-009' JOIN zs_city_bz city ON s.S_DM=city.DM WHERE u.DYHQC='{esc_sql(entity)}';"
                cypher = f"MATCH (u:Entity {{sourceTable:'zs_DY_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE u.zs_DY_bz__DYHQC = '{esc_cypher(entity)}' RETURN u.zs_DY_bz__DYHQC AS address_name, city.name AS city_name"
            elif kind == "room":
                entity = row["FJHQC"]
                sql = f"SELECT f.FJHQC AS address_name, city.MC AS city_name FROM zs_FJ_bz f JOIN zs_street_bz s ON f.S_DM=s.DM AND f.S_FLDM='B-DZ-011' JOIN zs_city_bz city ON s.S_DM=city.DM WHERE f.FJHQC='{esc_sql(entity)}';"
                cypher = f"MATCH (f:Entity {{sourceTable:'zs_FJ_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE f.zs_FJ_bz__FJHQC = '{esc_cypher(entity)}' RETURN f.zs_FJ_bz__FJHQC AS address_name, city.name AS city_name"
            else:
                entity = row["YLQC"]
                sql = f"SELECT y.YLQC AS address_name, city.MC AS city_name FROM zs_YL_bz y JOIN zs_street_bz s ON y.S_DM=s.DM AND y.S_FLDM='B-DZ-007' JOIN zs_city_bz city ON s.S_DM=city.DM WHERE y.YLQC='{esc_sql(entity)}';"
                cypher = f"MATCH (y:Entity {{sourceTable:'zs_YL_bz'}})-[:PART_OF_ADDRESS_ENTITY]->(s:Entity {{sourceTable:'zs_street_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE y.zs_YL_bz__YLQC = '{esc_cypher(entity)}' RETURN y.zs_YL_bz__YLQC AS address_name, city.name AS city_name"
            add_case(rows, used_questions, used_signatures, f"M3{idx:03d}", "multihop_address_city", "medium", f"{entity}属于哪个城市？", sql, cypher, f"address_city:{kind}:{row['gid']}", "address-street-city")

        # 4. 社区 -> 镇街 -> 市  [15]
        for idx, row in enumerate(community[:15], 1):
            name = row["MC"]
            sql = f"SELECT c.MC AS community_name, t.MC AS town_name, city.MC AS city_name FROM zs_community_bz c JOIN zs_town_bz t ON c.S_DM=t.DM JOIN zs_city_bz city ON t.S_DM=city.DM WHERE c.MC='{esc_sql(name)}';"
            cypher = f"MATCH (c:Entity {{sourceTable:'zs_community_bz'}})-[:PARENT_ADMIN_AREA]->(t:Entity {{sourceTable:'zs_town_bz'}})-[:PARENT_ADMIN_AREA]->(city:Entity {{sourceTable:'zs_city_bz'}}) WHERE c.name = '{esc_cypher(name)}' RETURN c.name AS community_name, t.name AS town_name, city.name AS city_name"
            add_case(rows, used_questions, used_signatures, f"M4{idx:03d}", "multihop_community_admin", "medium", f"{name}属于哪个镇街和城市？", sql, cypher, f"community_admin:{row['gid']}", "community-town-city")

        # 5. 派出所 -> 分局 -> 公安局 / 警务室 -> 派出所 -> 分局  [15]
        for idx, row in enumerate(police[:10], 1):
            name = row["MC"]
            sql = f"SELECT p.MC AS police_name, b.MC AS branch_name FROM zs_PCS_xq p JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE p.MC='{esc_sql(name)}';"
            cypher = f"MATCH (p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE p.name = '{esc_cypher(name)}' RETURN p.name AS police_name, b.name AS branch_name"
            add_case(rows, used_questions, used_signatures, f"M5{idx:03d}", "multihop_police_branch", "medium", f"{name}属于哪个分局？", sql, cypher, f"police_branch:{row['gid']}", "police-branch")

        for idx, row in enumerate(jws[:5], 1):
            name = row["MC"]
            sql = f"SELECT j.MC AS jws_name, p.MC AS police_name, b.MC AS branch_name FROM zs_JWS_xq j JOIN zs_PCS_xq p ON j.S_DM=p.DM JOIN zs_GAFJ_xq b ON p.S_DM=b.DM WHERE j.MC='{esc_sql(name)}';"
            cypher = f"MATCH (j:Entity {{sourceTable:'zs_JWS_xq'}})-[:PARENT_POLICE_ORG]->(p:Entity {{sourceTable:'zs_PCS_xq'}})-[:PARENT_POLICE_ORG]->(b:Entity {{sourceTable:'zs_GAFJ_xq'}}) WHERE j.name = '{esc_cypher(name)}' RETURN j.name AS jws_name, p.name AS police_name, b.name AS branch_name"
            add_case(rows, used_questions, used_signatures, f"M6{idx:03d}", "multihop_jws_police_branch", "medium", f"{name}属于哪个派出所和分局？", sql, cypher, f"jws_branch:{row['gid']}", "jws-police-branch")

        # 裁剪到 100 条，若不足则抛错
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
