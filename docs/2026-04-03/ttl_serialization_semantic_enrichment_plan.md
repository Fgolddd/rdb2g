# TTL 序列化语义增强改造方案（先审查）

## 1. 目标

在保持 RDF 合法性的前提下，让 `poi.ttl` 不仅有谓词 URI（如 `<zs_MP_bz.BZ>`），还显式携带最关键语义字段（`rdfs:label`、`rdfs:comment`），并保留映射依据 `reason`。

---

## 2. 现状与问题

当前链路中：

1. 大模型已基于 term 内容（尤其 `comment`）做映射判断；  
2. 但 `graph_builder.py` 在落盘时仅使用 `uri` 作为谓词；  
3. 因此 TTL 里看起来只有 `<zs_MP_bz.BZ>`，缺少“POI 地址/备注”等可读语义层。

这不是映射错误，而是“序列化信息损失”。

---

## 3. 设计原则

1. **谓词仍保留 URI**（RDF 规范要求）。  
2. **语义信息通过属性注释三元组补充**，不替换原谓词。  
3. **一次定义，多次复用**：每个 term URI 只声明一次元信息，避免文件爆炸。  
4. 兼容现有映射结构（字符串 URI / 对象映射）。

---

## 4. 目标 TTL 结构（示意）

### 4.1 实体数据三元组（保持不变）

```ttl
<http://example.org/data/zs_MP_bz/123> <zs_MP_bz.BZ> "示例备注" .
```

### 4.2 新增：term 语义注释三元组（新增）

```ttl
<zs_MP_bz.BZ> a rdf:Property ;
    rdfs:label "备注"@zh ;
    rdfs:comment "备注信息。"@zh .
```

### 4.3 映射决策信息（仅列级，非逐行）

```ttl
<zs_MP_bz.BZ> :mappingReason "列名 BZ 且样本文本符合备注语义" .
```

---

## 5. 代码改造范围（计划）

## 5.1 `graph_builder.py`

新增能力：

1. 从映射对象提取最小必要信息（`uri/label/comment/reason`）。  
2. 增加 `_ensure_term_semantics(term_obj)`，对每个 URI 仅写一次语义注释三元组。  
3. 维持当前数据写入逻辑：行数据仍按 `subject --predicate(uri)--> object/literal` 写入。  

实现要点：

- 新增命名空间（示例）：`RDFS`、自定义 `TERM = Namespace("http://example.org/term-meta/")`。  
- 类内维护 `self._declared_terms: set[str]`，避免重复声明。  
- 当映射值仅为字符串 URI 时，仅写数据三元组；当映射值是对象时，补写语义元信息。

## 5.2 `agents.py`（小改，按需）

为保证 TTL 能写入 `:mappingReason`，需要在 Validator prompt 中要求保留 `reason`（不要求 `confidence`）。

---

## 6. 验收标准

1. `poi.ttl` 中仍有原始谓词 URI（如 `<zs_MP_bz.BZ>`）。  
2. 同时存在对应的 `rdfs:label` / `rdfs:comment` 注释三元组。  
3. 若映射对象含 `reason`，可看到对应 `:mappingReason` 三元组。  
4. 同一 term 的注释只出现一次，无明显冗余膨胀。  
5. 端到端运行成功，TTL 可被 RDF 工具正常解析。

---

## 7. 风险与控制

1. **体积增长风险**：通过“每个 term 只声明一次”控制增量。  
2. **字段缺失风险**：映射对象缺字段时仅写已有信息，不阻断主流程。  
3. **兼容风险**：保留旧格式（字符串 URI）路径，避免影响历史流程。

---

## 8. 实施步骤

1. 先改 `graph_builder.py` 增加 term 注释序列化；  
2. 再跑主流程生成新 TTL；  
3. 检查示例 term（如 `zs_MP_bz.BZ` / `zs_POI_dm.DZ`）是否包含 `label/comment`；  
4. 补 `agents.py` 使 validator 至少保留 `reason`。

---

审查通过后，我再按该方案改代码。
