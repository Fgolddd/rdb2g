# RDB vs KG 测评示例（首批）

> 用于快速启动评测，结果值先留空，执行后回填到评分模板。

## A. 基础查询（SQL通常更快）

### Case A1
- 问题：`zs_POI_dm` 总记录数是多少？
- SQL：
```sql
SELECT COUNT(*) AS cnt FROM zs_POI_dm;
```
- KG（SPARQL示意）：
```sparql
SELECT (COUNT(?s) AS ?cnt)
WHERE { ?s <zs_POI_dm.gid> ?gid . }
```

### Case A2
- 问题：列出前 20 条 POI 名称与地址。
- SQL：
```sql
SELECT MC, DZ FROM zs_POI_dm LIMIT 20;
```
- KG：
```sparql
SELECT ?mc ?dz
WHERE {
  ?s <zs_POI_dm.MC> ?mc ;
     <zs_POI_dm.DZ> ?dz .
}
LIMIT 20
```

### Case A3
- 问题：统计每个 `DLMC` 下的 POI 数量（降序）。
- SQL：
```sql
SELECT DLMC, COUNT(*) AS cnt
FROM zs_POI_dm
GROUP BY DLMC
ORDER BY cnt DESC;
```
- KG：
```sparql
SELECT ?dlmc (COUNT(?s) AS ?cnt)
WHERE { ?s <zs_POI_dm.DLMC> ?dlmc . }
GROUP BY ?dlmc
ORDER BY DESC(?cnt)
```

## B. 关联查询（考察跨表能力）

### Case B1
- 问题：找出可关联标准门牌的 POI（`BZ_GUID` 对应 `zs_MP_bz.GUID`）。
- SQL：
```sql
SELECT p.MC AS poi_name, p.BZ_GUID, m.MC AS mp_name
FROM zs_POI_dm p
JOIN zs_MP_bz m ON p.BZ_GUID = m.GUID
LIMIT 50;
```
- KG：
```sparql
SELECT ?poiName ?bzGuid ?mpName
WHERE {
  ?poi <zs_POI_dm.MC> ?poiName ;
       <zs_POI_dm.BZ_GUID> ?bzGuid .
  ?mp  <zs_MP_bz.GUID> ?bzGuid ;
       <zs_MP_bz.MC> ?mpName .
}
LIMIT 50
```

### Case B2
- 问题：同一 `QXMC` 下，POI 与 AOI 的数量对比。
- SQL：
```sql
SELECT p.QXMC,
       COUNT(DISTINCT p.gid) AS poi_cnt,
       COUNT(DISTINCT a.gid) AS aoi_cnt
FROM zs_POI_dm p
LEFT JOIN zs_AOI_dm a ON p.QXMC = a.QXMC
GROUP BY p.QXMC;
```
- KG：
```sparql
SELECT ?qxmc
       (COUNT(DISTINCT ?poi) AS ?poi_cnt)
       (COUNT(DISTINCT ?aoi) AS ?aoi_cnt)
WHERE {
  ?poi <zs_POI_dm.QXMC> ?qxmc .
  OPTIONAL { ?aoi <zs_AOI_dm.QXMC> ?qxmc . }
}
GROUP BY ?qxmc
```

### Case B3
- 问题：在 `SJMC='广东省'` 条件下，按 `SSJMC` 汇总 POI 和 MP。
- SQL：
```sql
SELECT p.SSJMC,
       COUNT(DISTINCT p.gid) AS poi_cnt,
       COUNT(DISTINCT m.gid) AS mp_cnt
FROM zs_POI_dm p
LEFT JOIN zs_MP_bz m ON p.SSJMC = m.SSJMC
WHERE p.SJMC = '广东省'
GROUP BY p.SSJMC;
```
- KG：
```sparql
SELECT ?ssjmc
       (COUNT(DISTINCT ?poi) AS ?poi_cnt)
       (COUNT(DISTINCT ?mp) AS ?mp_cnt)
WHERE {
  ?poi <zs_POI_dm.SJMC> "广东省" ;
       <zs_POI_dm.SSJMC> ?ssjmc .
  OPTIONAL { ?mp <zs_MP_bz.SSJMC> ?ssjmc . }
}
GROUP BY ?ssjmc
```

## C. 语义查询（KG可解释性通常更强）

### Case C1
- 问题：哪些字段表达“地址”语义？
- SQL（困难，通常依赖人工字段名匹配）：
```sql
-- 需人工维护字段字典，SQL本身不具备语义注释查询能力
```
- KG：
```sparql
SELECT ?p ?label ?comment
WHERE {
  ?p rdfs:label ?label ;
     rdfs:comment ?comment .
  FILTER(CONTAINS(STR(?comment), "地址"))
}
```

### Case C2
- 问题：为什么 `zs_POI_dm.BZ` 会映射到“备注”语义？
- SQL：
```sql
-- 关系库通常没有字段映射理由存储
```
- KG：
```sparql
SELECT ?reason
WHERE {
  <zs_POI_dm.BZ> term:mappingReason ?reason .
}
```

### Case C3
- 问题：找出所有“更新时间”相关字段（跨表）。
- SQL：
```sql
-- 通常通过列名规则（如 GXSJ）硬编码
```
- KG：
```sparql
SELECT ?p ?comment
WHERE {
  ?p rdfs:label "更新时间"@zh ;
     rdfs:comment ?comment .
}
```

## D. 多跳/路径查询（KG表达更自然）

### Case D1
- 问题：从 POI 出发，经过标准门牌，再取门牌名称。
- SQL：
```sql
SELECT p.MC AS poi_name, p.BZ_GUID, m.MC AS mp_name
FROM zs_POI_dm p
JOIN zs_MP_bz m ON p.BZ_GUID = m.GUID;
```
- KG：
```sparql
SELECT ?poiName ?mpName
WHERE {
  ?poi <zs_POI_dm.MC> ?poiName ;
       <zs_POI_dm.BZ_GUID> ?guid .
  ?mp  <zs_MP_bz.GUID> ?guid ;
       <zs_MP_bz.MC> ?mpName .
}
```

### Case D2
- 问题：解释某个结果包含哪些语义字段依据（label/comment/reason）。
- SQL：
```sql
-- 需要额外系统实现，不是原生SQL能力
```
- KG：
```sparql
SELECT ?label ?comment ?reason
WHERE {
  <zs_AOI_dm.DZ> rdfs:label ?label ;
                 rdfs:comment ?comment .
  OPTIONAL { <zs_AOI_dm.DZ> term:mappingReason ?reason . }
}
```

---

## 建议

先用以上 11 条跑一轮“冒烟评测”，确认流程和统计口径，再扩展到 50 条正式题集。
