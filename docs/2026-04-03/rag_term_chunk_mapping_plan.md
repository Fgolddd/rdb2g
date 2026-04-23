# RAG 术语块（Term Chunk）驱动映射改造方案

## 1. 目标

你期望的流程是：

- 向量库切分单元以 `poi.json` 的 `terms` 原始结构为中心（一个 term 一个 chunk）；
- Agent 在映射时利用 `label/comment/domain/range/type` 等语义信息做判断；
- 不再把“只拿 URI”当作核心输入，而是先理解 term，再完成字段映射。

> 说明：知识图谱落盘时谓词仍需要 URI，但 URI 应该是“语义分析后的最终落地字段”，不是检索阶段的唯一信息源。

---

## 2. 当前问题（已定位）

1. 检索结果在 prompt 中被弱化为 “URI + 截断文本”，term 语义信息利用不充分。  
2. 映射阶段虽然拿到候选，但没有把 term 结构化信息作为“主输入对象”。  
3. 输出格式是 `column -> uri`，缺少“为什么选这个 term”的中间可解释层。  

---

## 3. 总体改造思路

### 3.1 向量索引层（vector_store）

将每个 term 作为**完整 JSON chunk**入库（`page_content`），示例：

```json
{
  "uri": "zs_POI_dm.MC",
  "type": "DataProperty",
  "label": "名称",
  "comment": "POI 名称。",
  "domain": "zs_POI_dm",
  "range": "string(255)"
}
```

并在 metadata 中保留可过滤字段：

- `uri`
- `domain`
- `column_code`（从 `uri` 后缀提取，如 `MC`）
- `type`

### 3.2 检索层（RAG retrieval）

对每列检索时返回 `top-k` **term对象列表**（而非仅 URI）：

- 第一优先：`domain + column_code` 精确过滤
- 第二优先：`column_code` 过滤
- 第三优先：语义相似度检索

最终给 Agent 的是结构化候选数组，例如：

```json
{
  "column": "DZ",
  "candidates": [
    {
      "uri": "zs_POI_dm.DZ",
      "label": "地址",
      "comment": "POI 地址。",
      "domain": "zs_POI_dm",
      "range": "string(255)",
      "type": "DataProperty"
    }
  ]
}
```

### 3.3 Agent 输入输出改造（agents）

#### 输入

在 mapping/validator prompt 中把每列候选 term 以 JSON 结构传入，要求模型基于 `label/comment/domain/range` 选择。

#### 输出（建议）

从当前最小输出升级为“可解释映射输出”：

```json
{
  "DZ": {
    "uri": "zs_POI_dm.DZ",
    "label": "地址",
    "comment": "POI 地址。",
    "confidence": 0.93,
    "reason": "列名DZ且样本为详细地址文本"
  }
}
```

然后在落图前再转换为 `column -> uri`（兼容现有 graph_builder）。

### 3.4 约束策略

- 若有候选 term，模型输出必须在候选集合内（白名单约束）。
- 若无候选，则输出 `null`，禁止自动退回公共本体 URI（除非显式开启开关）。

---

## 4. 代码改动范围（计划）

1. `vector_store.py`
   - term 完整 JSON chunk 入库
   - 检索接口返回结构化候选对象
2. `agents.py`
   - `_get_rag_context` 改为返回 `column -> candidates(term objects)`
   - `run_mapping_agent` / `run_validator_agent` 处理结构化候选并输出可解释映射
   - 统一白名单约束逻辑
3. （可选）`graph_builder.py`
   - 增加兼容：若映射值是对象则读取其中 `uri`

---

## 5. 验收标准

1. 日志中每列 RAG 候选显示完整 term 字段（至少 `uri/label/comment/domain/range/type`）。  
2. Mapping 输出可看到与 `comment` 一致的选择依据。  
3. `poi.ttl` 中谓词 URI 来自 `poi.json` term，而非 `ns1:address` 这类外部结果。  
4. 对 `DZ/MC/BZ` 等列，结果能稳定映射到对应 term（如 `zs_POI_dm.DZ` / `zs_POI_dm.MC` / `zs_POI_dm.BZ`）。

---

## 6. 风险与兼容性

- 输出格式从“字符串 URI”改为“对象”时，需保证 `graph_builder` 向后兼容。  
- 检索更严格后，可能出现更多 `null`，这是可控行为（优先正确性）。  

---

## 7. 实施节奏

1. 先完成结构化 chunk + 结构化候选检索；  
2. 再改 Agent 输出为可解释对象；  
3. 最后做落图兼容与回归测试。  

---

确认后我将按此方案实施代码修改。
