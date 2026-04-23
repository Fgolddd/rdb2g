# 项目概览（`/home/lyx/llm_graph/rdb2g`）

这是一个**把结构化数据/文本语义转换为图数据（RDF/TTL）并做检索增强**的项目，核心目标看起来是：
- 解析 schema 与原始数据
- 构建图结构（RDF triples）
- 导入图数据库（如 Aura/Neo4j 生态）
- 结合向量库做语义检索

## 从目录推断的核心模块

- `schema_parser.py`：schema 解析（字段、关系、类型约束）
- `graph_builder.py`：图构建逻辑（实体、关系、三元组生成）
- `convert.py`：格式转换入口（可能是 table/json -> ttl/rdf）
- `import_aura.py`：向 Aura 实例导入数据
- `vector_store.py`：向量化存储与检索
- `dataloader.py`：数据加载与预处理
- `generate_ground_truth.py`：生成评测基准（ground truth）
- `main.py` / `agents.py`：主流程编排与 agent 化调用
- `data/ttl/`, `data/chroma_db/`：TTL 产物与 Chroma 持久化数据

## 技术点分析（重点）

1. **知识图谱建模（RDF/TTL）**
   - 使用 TTL 文件（如 `aircraft.ttl`, `car_1.ttl`）说明项目采用语义网表示。
   - 关键技术点是：实体 URI 设计、谓词规范化、类型体系（ontology）与数据一致性。

2. **图构建与转换流水线**
   - `convert.py` + `graph_builder.py` 体现了 ETL 风格管道：
     原始数据 -> 结构抽取 -> 图表示。
   - 难点通常在异构字段映射、关系推断、去重与 ID 对齐。

3. **向量检索（Chroma）与图的融合**
   - `data/chroma_db/chroma.sqlite3` + `vector_store.py` 表明有 RAG/语义召回能力。
   - 技术亮点是将“向量相似度”与“图关系约束”结合，提升检索精度与可解释性。

4. **图数据库导入能力**
   - `import_aura.py` 指向云端图数据库接入（Aura）。
   - 核心技术点：批量导入、事务控制、模式匹配和导入后校验。

5. **评测闭环（ground truth）**
   - `generate_ground_truth.py` 说明项目不只是构建，还关注效果验证。
   - 技术价值在于可复现实验与指标评估（召回、准确率、关系覆盖率）。

## 当前工程状态观察

- Git 状态显示有**未提交的数据库与 TTL 变更**，且有新增目录/文件（`data/company/`, `convert.py` 等）。
- 这通常意味着项目正在进行数据迭代实验，建议后续加强：
  - 数据产物与代码分离（避免把大体积/中间产物直接纳入版本）
  - 增加可复现实验脚本与固定输入样例
