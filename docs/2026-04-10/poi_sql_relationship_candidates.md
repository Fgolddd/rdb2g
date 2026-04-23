# poi.sql 关系抽取与关系表设计（初稿）

## 1. 目标

基于 `data/company/poi.sql` 对现有 5 张表进行字段级关系推断，输出：

1. 可用于建图的候选关系（字段→字段）
2. 当前数据可支撑程度（覆盖率/匹配率）
3. 推荐的“关系表”设计（用于后续 TTL 补边）

涉及表：
- `zs_POI_dm`
- `zs_AOI_dm`
- `zs_MP_bz`
- `zs_FJ_bz`
- `zs_street_bz`

---

## 2. 字段完备性关键信息（摘要）

### 2.1 POI / AOI
- `GUID`, `BZ_GUID`, `BZDZMC`, `ZDGADM`, `ZDGACM` 在当前样本中几乎全空。
- `QXMC/SJMC/SSJMC` 基本全有值，但基数很低（几乎都为“中山市/广东省”）。

### 2.2 MP
- `GUID` 全空（关键问题）
- `DM`, `MPQC`, `DHBM` 完整度高
- `S_GUID` 全空
- `S_DM/S_MC` 完整度高

### 2.3 FJ / street
- `zs_FJ_bz.GUID/DM/S_GUID/S_DM` 完整度高
- `zs_street_bz.GUID/DM` 完整度高

> 结论：**模式层面存在多条潜在主外键关系，但数据层面很多关键桥接字段为空，导致无法稳定落边。**

---

## 3. 候选关系清单（按优先级）

| 优先级 | 候选关系 | 语义 | 当前证据 | 关系可用性 |
|---|---|---|---|---|
| P0 | `zs_POI_dm.BZ_GUID -> zs_MP_bz.GUID` | POI 关联门牌 | 两侧关键字段当前几乎全空 | 低（需补数） |
| P0 | `zs_AOI_dm.BZ_GUID -> zs_MP_bz.GUID` | AOI 关联门牌 | 两侧关键字段当前几乎全空 | 低（需补数） |
| P0 | `zs_MP_bz.S_GUID -> zs_street_bz.GUID` | 门牌归属街路 | `S_GUID` 当前全空 | 低（需补数） |
| P0 | `zs_FJ_bz.S_GUID -> zs_MP_bz.GUID` | 房间归属门牌 | 右侧 `MP.GUID` 当前全空 | 低（需补数） |
| P1 | `zs_MP_bz.S_DM -> zs_street_bz.DM` | 门牌关联街路编码 | 有少量重叠（约 11%） | 中（可先用） |
| P2 | `zs_FJ_bz.S_DM -> zs_MP_bz.DM` | 房间关联门牌编码 | 重叠极低（接近 0） | 低 |
| P2 | `zs_POI_dm.QXMC -> zs_AOI_dm.QXMC` | 行政区同域关联 | 匹配率高但区分度极低 | 中（弱语义） |
| P2 | `zs_POI_dm.SSJMC -> zs_AOI_dm.SSJMC` | 市级同域关联 | 匹配率高但区分度极低 | 中（弱语义） |

---

## 4. 建议关系类型（建图用）

建议先定义关系类型（即使暂时为空），后续可增量补数：

- `(:POI)-[:LOCATED_AT_MP]->(:MP)` 对应 `POI.BZ_GUID = MP.GUID`
- `(:AOI)-[:LOCATED_AT_MP]->(:MP)` 对应 `AOI.BZ_GUID = MP.GUID`
- `(:FJ)-[:BELONGS_TO_MP]->(:MP)` 对应 `FJ.S_GUID = MP.GUID`
- `(:MP)-[:ON_STREET]->(:STREET)` 对应 `MP.S_GUID = STREET.GUID`（或过渡方案 `S_DM -> DM`）
- `(:POI)-[:SAME_QX_AS]->(:AOI)` 对应 `QXMC`（弱关系，可选）

---

## 5. 关系表设计（建议先落库）

建议先在关系库建立“关系元数据表 + 关系实例表”，再驱动 TTL 增边。

### 5.1 关系元数据表（定义规则）

```sql
CREATE TABLE IF NOT EXISTS rel_rule_catalog (
  rule_id TEXT PRIMARY KEY,
  src_table TEXT NOT NULL,
  src_field TEXT NOT NULL,
  dst_table TEXT NOT NULL,
  dst_field TEXT NOT NULL,
  relation_type TEXT NOT NULL,
  priority INTEGER NOT NULL,
  confidence_level TEXT NOT NULL,   -- high/medium/low
  enabled INTEGER NOT NULL DEFAULT 1,
  note TEXT
);
```

### 5.2 关系实例表（保存抽取结果）

```sql
CREATE TABLE IF NOT EXISTS rel_edge_instance (
  edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id TEXT NOT NULL,
  src_table TEXT NOT NULL,
  src_gid TEXT NOT NULL,
  dst_table TEXT NOT NULL,
  dst_gid TEXT NOT NULL,
  relation_type TEXT NOT NULL,
  match_value TEXT,
  evidence_score REAL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

---

## 6. 抽取策略（建议）

### Phase A（可立即落地）
1. 启用 `MP.S_DM -> STREET.DM` 关系抽取（中等可用）。
2. 启用 `QXMC/SSJMC` 的弱关系，仅用于分组/聚合，不用于强实体链接。

### Phase B（补数后启用）
1. 回填 `POI/AOI.BZ_GUID` 与 `MP.GUID`。  
2. 回填 `MP.S_GUID`，打通 `MP -> STREET`。  
3. 启用强关系：`POI/AOI -> MP -> STREET`、`FJ -> MP`。

---

## 7. 对 TTL 重规划的直接建议

1. **先保留属性三元组**（不丢字段）。
2. 新增“关系补边层”（基于 `rel_rule_catalog` + `rel_edge_instance`）。
3. 在 TTL 中显式输出对象属性：
   - `poi:locatedAtMp`
   - `aoi:locatedAtMp`
   - `fj:belongsToMp`
   - `mp:onStreet`
4. 给每个关系打 `evidence_score`，便于后续过滤低置信边。

---

## 8. 结论

- **从 schema 看，关系设计是存在的；从数据看，关键桥接字段缺失严重。**
- 当前最可先落地的强相关候选是：`MP.S_DM -> STREET.DM`（但覆盖有限）。
- 要实现高质量多跳知识图谱，需优先补齐 `GUID/BZ_GUID/S_GUID` 链路字段。
