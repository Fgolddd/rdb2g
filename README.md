# rdb2g

将关系型数据库（SQLite）自动化转换为知识图谱（RDF/TTL），并支持基于私域术语库的检索增强映射（RAG）、Neo4j 导入与 RDB/KG 对比评测。

## 项目现状

- 主流程已打通：`SQLite -> 表指纹 -> 多智能体映射 -> TTL`
- 支持两种知识模式：私域知识库模式（`--kb-file`）与无知识库模式
- 支持映射缓存与并行处理（大库可显著提速）
- 支持分步执行：生成 TTL、导入 Neo4j、执行 RDB vs KG 评测
- 当前重点数据集：`data/company/zhongshan.sqlite`

## 核心结构

- `rdb2g/cli/`：命令行入口，包含 TTL 构建、Neo4j 导入、评测和规则生成入口
- `rdb2g/pipeline/`：TTL 构建编排、映射缓存、列选择、关系索引
- `rdb2g/mapping/`：Mapping / Relation / Validator 多智能体逻辑
- `rdb2g/retrieval/`：Qwen Embedding、Chroma 向量检索、Schema.org / 私域 KB 解析
- `rdb2g/graph/`：RDF 三元组构建、TTL 序列化、关系规则
- `rdb2g/data/`：SQLite 数据读取与表指纹生成
- `rdb2g/neo4j/`：TTL 导入 Neo4j 与展示字段增强
- `rdb2g/eval/`：RDB SQL vs KG Cypher 评测执行
- `scripts/`：一次性或数据转换辅助脚本

## 环境准备

约定：所有 `python3` 命令均在虚拟环境中运行。

### 1) 创建并激活虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) 安装依赖

```bash
pip install -r requirements.txt
```

说明：`requirements.txt` 当前不完整。若运行时报缺包，请补装常见运行时依赖，例如 `openai`、`neo4j`、`rdflib-neo4j`。

### 3) 配置环境变量（`.env`）

必需（映射/RAG）：

- `DASHSCOPE_API_KEY`

常用可选：

- `QWEN_CHAT_MODEL`
- `QWEN_EMBEDDING_MODEL`
- `QWEN_EMBEDDING_BATCH_SIZE`
- `QWEN_EMBEDDING_MAX_CHARS`
- `QWEN_BASE_URL`
- `QWEN_ENABLE_THINKING`
- `DEBUG_RAG_RESULTS`

Neo4j（导入/评测）：

- `NEO4J_URI`
- `NEO4J_DATABASE`
- `NEO4J_USER`
- `NEO4J_PWD`

## 快速开始

### 1) 基于中山市数据生成 TTL（推荐）

```bash
python3 -m rdb2g.cli.build_ttl "data/company/zhongshan.sqlite" --kb-file "data/company/zhongshan_rag_terms.json" --relation-rules "data/company/zhongshan_relation_rules.json"
```

输出示例：`data/ttl/zhongshan.ttl`

### 2) 无知识库模式

```bash
python3 -m rdb2g.cli.build_ttl "data/company/zhongshan.sqlite" --allow-public-uri
```

### 3) 导入 TTL 到 Neo4j

```bash
python3 -m rdb2g.cli.import_neo4j "data/ttl/zhongshan.ttl"
```

### 4) 执行 RDB vs KG 评测

```bash
python3 -m rdb2g.cli.rdb_kg_eval --question-bank "docs/2026-04-09/rdb_vs_kg_eval_sample_cases_100.csv" --db-path "data/company/zhongshan.sqlite" --engine both --out-dir "data/eval/zhongshan_eval"
```

推荐分步流程：先用 `rdb2g.cli.build_ttl` 生成 TTL，再用 `rdb2g.cli.import_neo4j` 导入 Neo4j，最后用 `rdb2g.cli.rdb_kg_eval` 对比 SQL 与 Cypher 查询效率。

### 5) 从 0 生成关系规则草案

```bash
python3 -m rdb2g.cli.build_relation_rules "data/company/zhongshan_10k.sqlite" --kb-file "data/company/zhongshan_rag_terms.json" --out "data/company/zhongshan_relation_rules.generated.json" --report "data/company/zhongshan_relation_rules.report.json" --auto-accept-strong --min-hit-rate 0.8 --sample-size 10000
```

说明：`generated.json` 可作为 `--relation-rules` 输入，`report.json` 用于审查命中率、自环、fanout 和样例。

## 数据与缓存说明

- 映射缓存：`data/mapping_cache/<db_stem>/`（表指纹不变时复用）
- 向量索引：`data/chroma_db/<prefix>_<kb_or_schema_name>/`（知识库哈希或 schema 版本变化会触发重建）
- TTL 产物：`data/ttl/<db_stem>.ttl`
- 评测结果：`data/eval/<run_name>/`
- 自动关系规则草案：`data/company/*relation_rules.generated.json` 与验证报告 `*relation_rules.report.json`

## 评测输入规范（重要）

`rdb2g.cli.rdb_kg_eval` 中：

- RDB 查询读取列：`sql_query`
- KG 查询读取列：`sparql_or_graph_query`，但必须写 Cypher
- 若题库里是 SPARQL（如 `PREFIX`、`SELECT ?`），会被标记为 `not_cypher_query`

## 中山市数据集的构建建议

面向“业务查询优先 + 体现 KG 优势”：

- 主干优先：地址对象、POI/设施、行政区、警务辖区
- 关系优先：显式构建跨层级关系（归属、上下级、关联）
- 语义统一：统一分类字段（如 `DLMC/ZLMC/FLMC/SSLX`）
- 评测对齐：优先设计多跳、路径解释、跨域聚合题型

详细方案见：`docs/2026-04-24/kg_business_priority_and_advantage_plan.md`

## 已知注意事项

- 本仓库暂无统一 test/lint/typecheck 流程，建议按改动路径做针对性验证
- `.env` 不应提交到仓库
- `data/` 目录多为本地产物，默认不作为版本管理主体
