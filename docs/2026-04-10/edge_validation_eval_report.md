# 关系边专项测评报告（E001-E008）

## 1. 测评目标

验证当前已补强关系边（`in_qx / in_city / in_province / on_street / ref_mp`）是否可被 SQL 与 Cypher 稳定查询，并对比性能与结果一致性。

## 2. 执行范围

- SQL 数据源：`data/company/poi.sqlite`
- KG 数据源：Neo4j（已重新导入增强版 `poi.ttl`）
- 测评条目：E001~E008（8 条）

## 3. 执行结果总览

- SQL：8/8 成功
- Cypher：8/8 成功
- 性能：SQL 显著快于 Cypher（多数条目 SQL 为毫秒级，Cypher 多为 ~300ms）

## 4. 分题结果


| ID   | SQL 行数 / 耗时(ms) | Cypher 行数 / 耗时(ms) | 结果要点             |
| ---- | --------------- | ------------------ | ---------------- |
| E001 | 5 / 3.728       | 5 / 3710.816       | 关系类型都查到；计数存在差异   |
| E002 | 20 / 96.644     | 20 / 342.208       | 门牌→街道样例可查        |
| E003 | 20 / 1.232      | 20 / 333.181       | 街道下门牌聚合可查        |
| E004 | 1 / 185.076     | 1 / 304.912        | 房间→门牌仅 1 条       |
| E005 | 1 / 0.883       | 1 / 324.754        | POI-区县统计可查，计数有差异 |
| E006 | 1 / 1.409       | 1 / 314.196        | AOI-市级统计可查，计数有差异 |
| E007 | 1 / 1487.633    | 1 / 352.342        | 省市区三边交集可查，计数差异明显 |
| E008 | 1 / 2.196       | 2 / 318.148        | 低覆盖关系均可识别        |


### 4.1 Cypher 语句（E001~E008）

**E001：关系类型计数（5类）**
```cypher
MATCH ()-[r]->()
WHERE type(r) IN ['in_qx', 'in_city', 'in_province', 'on_street', 'ref_mp']
RETURN type(r) AS relation_type, count(*) AS cnt
ORDER BY relation_type;
```

**E002：门牌 -> 街道样例（Top20）**
```cypher
MATCH (mp:Resource)-[:on_street]->(street:Resource)
RETURN mp['zs_MP_bz.MPQC'] AS mp_name,
       street['zs_street_bz.MC'] AS street_name
LIMIT 20;
```

**E003：街道下门牌聚合（Top20）**
```cypher
MATCH (mp:Resource)-[:on_street]->(street:Resource)
RETURN street['zs_street_bz.MC'] AS street_name,
       count(mp) AS mp_cnt
ORDER BY mp_cnt DESC
LIMIT 20;
```

**E004：房间 -> 门牌样例**
```cypher
MATCH (fj:Resource)-[:ref_mp]->(mp:Resource)
RETURN fj['zs_FJ_bz.FJHQC'] AS fj_name,
       mp['zs_MP_bz.MPQC'] AS mp_name
LIMIT 20;
```

**E005：POI 关联区县边总量**
```cypher
MATCH (:Resource)-[r:in_qx]->(:Resource)
RETURN count(r) AS cnt;
```

**E006：AOI 关联市级边总量**
```cypher
MATCH (:Resource)-[r:in_city]->(:Resource)
RETURN count(r) AS cnt;
```

**E007：同时具备省/市/区三类边的实体数量**
```cypher
MATCH (x:Resource)-[:in_province]->(:Resource)
MATCH (x)-[:in_city]->(:Resource)
MATCH (x)-[:in_qx]->(:Resource)
RETURN count(DISTINCT x) AS cnt;
```

**E008：低覆盖关系识别（返回2行）**
```cypher
MATCH ()-[r]->()
WHERE type(r) IN ['on_street', 'ref_mp']
RETURN type(r) AS relation_type, count(*) AS cnt
ORDER BY cnt ASC;
```

## 5. 关键差异

### 5.1 E001 关系计数差异

- SQL（关系实例表）：
  - IN_QX = 2474
  - IN_CITY = 2474
  - IN_PROVINCE = 2474
  - ON_STREET = 110
  - REF_MP = 1
- Cypher（Neo4j 实际边）：
  - in_qx = 1645
  - in_city = 1657
  - in_province = 1618
  - on_street = 79
  - ref_mp = 1

### 5.2 行政维度统计差异（E005/E006/E007）

Cypher 侧统计值普遍小于 SQL 侧，说明导入 Neo4j 时发生了部分去重/覆盖或节点属性聚合行为，导致关系边保留数量少于关系实例表。

## 6. 结论

1. 当前补强后的关系边已可被 SQL/Cypher 双端稳定访问（可执行性达标）。
2. SQL 与 Neo4j 在“边数量”上仍存在显著偏差，需继续核查导入语义（去重/覆盖策略）。
3. `on_street` 与 `ref_mp` 可作为第一批可用关系链路；`in_qx/in_city/in_province` 可用于维度分析，但数量一致性需进一步校准。

## 7. 建议下一步

1. 固化“关系实例表 -> TTL -> Neo4j”的一致性校验脚本（按 relation_type 对账）。
2. 对导入逻辑增加关系边不丢失保障（避免 merge/覆盖导致数量收缩）。
3. 将 E001~E008 纳入回归测试，作为关系补强后的基线用例。

