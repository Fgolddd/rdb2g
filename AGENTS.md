# AGENTS.md

## 这个仓库实际在跑什么
- 主入口是 `main.py`：SQLite -> 表指纹 -> 多智能体语义映射 -> 导出 RDF TTL 到 `data/ttl/<db_stem>.ttl`。
- `run_full_experiment.py` 是一键编排：先生成 TTL，再（可选）导入 Neo4j，最后跑评测。
- `run_rdb_kg_eval.py` 负责评测执行：RDB 侧读 `sql_query`，KG 侧读 `sparql_or_graph_query`（但这里必须是 Cypher）。

## 代理最容易漏掉的环境准备
- 映射/向量检索必须有 `ARK_API_KEY`（`main.py` 会通过 `.env` 加载）。
- 常用可选环境变量：`DOUBAO_CHAT_MODEL`、`DOUBAO_EMBEDDING_MODEL`、`DOUBAO_EMBEDDING_BATCH_SIZE`、`DOUBAO_EMBEDDING_MAX_CHARS`、`ARK_BASE_URL`、`DEBUG_RAG_RESULTS`。
- 为兼容旧配置，代码仍支持读取 `QWEN_*` 与 `DASHSCOPE_API_KEY` 作为兜底。
- `requirements.txt` 不完整；若导入失败，补装运行时依赖（如 `openai`、`neo4j`、`rdflib-neo4j`）。

## 常用命令（可直接复用）
- 基于数据库生成 TTL：
  - `python main.py "data/company/zhongshan.sqlite" --kb-file "data/company/zhongshan_rag_terms.json"`
- 无知识库模式（默认本地自动 URI，除非显式允许公共 URI）：
  - `python main.py "<db_path>" --allow-public-uri`
- 全流程（TTL -> Neo4j 导入 -> 评测）：
  - `python run_full_experiment.py "<db_path>" --question-bank "docs/2026-04-09/rdb_vs_kg_eval_sample_cases_100.csv" --engine both`
- 仅评测：
  - `python run_rdb_kg_eval.py --question-bank "<csv>" --db-path "<sqlite>" --engine both --out-dir "data/eval/<run_name>"`

## 行为与结果的关键坑点
- `main.py` 会把每张表的映射缓存到 `data/mapping_cache/<db_stem>/`；表指纹变化只会使对应表缓存失效。
- 向量索引持久化在 `data/chroma_db/<prefix>_<kb_or_schema_name>/`；知识库哈希或 schema 版本变化会触发重建。
- `run_rdb_kg_eval.py` 会把 SPARQL 风格 KG 查询（如 `PREFIX`、`SELECT ?`）标记为 `not_cypher_query`；题库 KG 查询需改为 Cypher。
- `run_rdb_kg_eval.py` 默认 `--db-path` 是 `data/company/poi.sqlite`；该文件不存在时必须显式传 `--db-path`。

## 安全与敏感信息
- `.env` 已在 `.gitignore` 中，保持本地即可；不要提交 API Key。
- 评测脚本里有 Neo4j 默认凭据；本地运行优先用环境变量或 CLI 参数覆盖，不要直接改脚本。

## 仓库约定
- 仓库没有现成的 test/lint/typecheck 配置；改动后用你触达的脚本做针对性验证。
- `.gitignore` 忽略了 `data/`；TTL、评测结果、缓存和索引等产物通常是本地文件。
