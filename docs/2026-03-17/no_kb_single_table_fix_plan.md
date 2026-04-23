# 无知识库场景与单表外键误判修复方案（审阅版）

## 1. 日志问题成因说明

## 1.1 问题1：未传 `schema.org` 仍出现 `schema.org`/`w3.org` URI

根因是：

1. 当前“无知识库模式”只是不做向量检索，不代表模型不会使用其预训练知识。  
2. `Mapping Agent` 提示词仍允许输出“标准术语 URI”，模型会自然偏向常见公共本体（如 Schema.org、LOCN、GeoSPARQL）。  
3. 代码目前没有“无知识库时禁止外部公共本体 URI”的后处理约束。

## 1.2 问题2：单表被误判出外键 `['DLMC', 'ZLMC', ...]`

根因是：

1. 当前 `Relation Agent` 在“无显式外键”时被要求按列名推断关系。  
2. 数据库指纹里没有注入 SQLite 的真实约束信息（`PRAGMA foreign_key_list` / `PRAGMA table_info`）。  
3. 在单表场景下，分类/行政区等属性字段容易被模型误判为“关系列/外键”。

---

## 2. 改造目标

1. **无知识库模式**：允许模型推断语义，但默认不输出公共本体 URI（除非显式允许）。  
2. **关系识别**：优先使用数据库真实约束；单表且无显式外键时，`fks` 应为空。  
3. 保留多表场景的可推断能力，但只在有跨表证据时启用。

---

## 3. 详细改造方案

## 3.1 新增“术语输出策略”配置

在入口增加参数（建议）：

- `--allow-public-uri`（默认 `false`）

行为：

- `false`：无知识库时不允许输出 `schema.org/w3.org/opengis` 等公共 URI；
- `true`：允许模型自由输出公共 URI（兼容旧行为）。

## 3.2 Mapping 结果增加后处理归一化（关键）

新增统一后处理函数（建议放 `agents.py` 或单独 `mapping_utils.py`）：

1. 判断当前模式是否无知识库；
2. 若 `allow_public_uri=false` 且模型输出公共 URI：
   - 改写为本地命名空间 URI（如 `http://example.org/auto/{column_name}`）；
3. 保留 `null`；
4. 输出最终映射供 Validator 与 GraphBuilder 使用。

> 这样可保证“没引用知识库时，不会混入 schema.org”。

## 3.3 注入真实约束信息到指纹

在 `dataloader.py` 新增：

- `get_table_constraints(table_name)`：
  - 读取 `PRAGMA table_info(table)` 提取显式主键；
  - 读取 `PRAGMA foreign_key_list(table)` 提取显式外键；
- `generate_table_fingerprint` 增加字段：
  - `explicit_pk`
  - `explicit_fks`
  - `table_count`
  - `all_tables`

## 3.4 Relation Agent 改为“约束优先 + 受控推断”

更新关系识别逻辑（提示词 + 程序后处理双保险）：

1. 若 `explicit_fks` 非空：直接优先使用显式外键；
2. 若 `table_count == 1` 且 `explicit_fks` 为空：强制 `fks=[]`；
3. 若多表且无显式外键：允许推断，但需满足跨表证据（如列名与其他表主键/表名匹配）；
4. 不能仅凭“像分类/行政区字段”就判定 FK。

## 3.5 Validator 增加单表防误判规则

在 `run_validator_agent` 提示词中加入硬规则：

- 单表无显式外键时，所有列按属性处理，不可强行修正为对象关系。

并在程序侧对 `relations['fks']` 再做一次防御性过滤（与 3.4 同规则）。

## 3.6 GraphBuilder 行为保持兼容

`graph_builder.py` 不改核心逻辑，仅确保可接受本地命名空间 URI（已支持 URIRef）。

---

## 4. 实施文件清单

计划修改：

- `dataloader.py`（注入显式约束）
- `agents.py`（映射后处理、关系规则、Validator 规则）
- `main.py`（新增 `--allow-public-uri` 参数并透传）
- （可选）新增 `mapping_utils.py`（URI 过滤与归一化）

---

## 5. 验证计划

1. 用 `poi.sqlite`（单表）验证：`fks` 必须为空。  
2. 无知识库 + `--allow-public-uri=false` 验证：输出不应包含 `schema.org/w3.org/opengis` 公共 URI。  
3. 多表样例验证：显式外键可被识别；无显式外键时仅在跨表证据充分时推断。  
4. 运行语法检查：`python3 -m py_compile`（受改文件）。

---

以上是针对你这次日志问题的专门修复方案，待你审阅确认后我再开始改代码。
