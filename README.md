# rdb2g

将关系型数据库（SQLite）自动化转换为知识图谱（RDF/TTL），并支持基于私域术语库的检索增强映射（RAG）、Neo4j 导入与 RDB/KG 对比评测。

## 项目现状

- 主流程已打通：`SQLite -> 表指纹 -> 多智能体映射 -> TTL`
- 支持两种知识模式：私域知识库模式（`--kb-file`）与无知识库模式
- 支持映射缓存与并行处理（大库可显著提速）
- 支持一键全流程：生成 TTL、导入 Neo4j、执行 RDB vs KG 评测
- 当前重点数据集：`data/company/zhongshan.sqlite`

## 核心文件

- `main.py`：主入口，负责 TTL 构建
- `agents.py`：Mapping / Relation / Validator 多智能体逻辑
- `vector_store.py`：Doubao Embedding + Chroma 向量检索
- `graph_builder.py`：RDF 三元组构建与 TTL 序列化
- `dataloader.py`：SQLite 数据读取与表指纹生成
- `schema_parser.py`：Schema.org / 私域 KB 解析
- `run_full_experiment.py`：全流程编排（TTL -> Neo4j -> Eval）
- `run_rdb_kg_eval.py`：评测执行（RDB SQL vs KG Cypher）

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

- `ARK_API_KEY`

常用可选：

- `DOUBAO_CHAT_MODEL`
- `DOUBAO_EMBEDDING_MODEL`
- `DOUBAO_EMBEDDING_BATCH_SIZE`
- `DOUBAO_EMBEDDING_MAX_CHARS`
- `ARK_BASE_URL`
- `DEBUG_RAG_RESULTS`

Neo4j（导入/评测）：

- `NEO4J_URI`
- `NEO4J_DATABASE`
- `NEO4J_USER`
- `NEO4J_PWD`

## 快速开始

### 1) 基于中山市数据生成 TTL（推荐）

```bash
python3 main.py "data/company/zhongshan.sqlite" --kb-file "data/company/zhongshan_rag_terms.json"
```

输出示例：`data/ttl/zhongshan.ttl`

### 2) 无知识库模式

```bash
python3 main.py "data/company/zhongshan.sqlite" --allow-public-uri
```

### 3) 一键全流程（TTL -> Neo4j -> 评测）

```bash
python3 run_full_experiment.py "data/company/zhongshan.sqlite" --question-bank "docs/2026-04-09/rdb_vs_kg_eval_sample_cases_100.csv" --engine both
```

### 4) 仅执行评测

```bash
python3 run_rdb_kg_eval.py --question-bank "docs/2026-04-09/rdb_vs_kg_eval_sample_cases_100.csv" --db-path "data/company/zhongshan.sqlite" --engine both --out-dir "data/eval/zhongshan_eval"
```

## 数据与缓存说明

- 映射缓存：`data/mapping_cache/<db_stem>/`（表指纹不变时复用）
- 向量索引：`data/chroma_db/<prefix>_<kb_or_schema_name>/`（知识库哈希或 schema 版本变化会触发重建）
- TTL 产物：`data/ttl/<db_stem>.ttl`
- 评测结果：`data/eval/<run_name>/`

## 评测输入规范（重要）

`run_rdb_kg_eval.py` 中：

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
