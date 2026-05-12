# AGENTS.md

## 这个仓库实际在跑什么
- 主入口是 `python -m rdb2g.cli.build_ttl`：SQLite -> 表指纹 -> 多智能体语义映射 -> 导出 RDF TTL 到 `data/ttl/<db_stem>.ttl`。
- `python -m rdb2g.cli.import_neo4j` 负责把已生成的 TTL 导入 Neo4j。
- `python -m rdb2g.cli.rdb_kg_eval` 负责评测执行：RDB 侧读 `sql_query`，KG 侧读 `sparql_or_graph_query`（但这里必须是 Cypher）。

## 代理最容易漏掉的环境准备
- 映射/向量检索必须有 `DASHSCOPE_API_KEY`（`rdb2g.pipeline.ttl_builder` 会通过 `.env` 加载）。
- 常用可选环境变量：`QWEN_CHAT_MODEL`、`QWEN_EMBEDDING_MODEL`、`QWEN_EMBEDDING_BATCH_SIZE`、`QWEN_EMBEDDING_MAX_CHARS`、`QWEN_BASE_URL`、`QWEN_ENABLE_THINKING`、`DEBUG_RAG_RESULTS`。
- `requirements.txt` 不完整；若导入失败，补装运行时依赖（如 `openai`、`neo4j`、`rdflib-neo4j`）。

## 常用命令（可直接复用）
- 基于数据库生成 TTL：
  - `python -m rdb2g.cli.build_ttl "data/company/zhongshan.sqlite" --kb-file "data/company/zhongshan_rag_terms.json" --relation-rules "data/company/zhongshan_relation_rules.json"`
- 无知识库模式（默认本地自动 URI，除非显式允许公共 URI）：
  - `python -m rdb2g.cli.build_ttl "<db_path>" --allow-public-uri`
- 导入已生成的 TTL 到 Neo4j：
  - `python -m rdb2g.cli.import_neo4j "data/ttl/<db_stem>.ttl"`
- 仅评测：
  - `python -m rdb2g.cli.rdb_kg_eval --question-bank "<csv>" --db-path "<sqlite>" --engine both --out-dir "data/eval/<run_name>"`
- 从 0 生成关系规则草案（允许 KB 作为字段语义参考）：
  - `python -m rdb2g.cli.build_relation_rules "data/company/zhongshan_10k.sqlite" --kb-file "data/company/zhongshan_rag_terms.json" --out "data/company/zhongshan_relation_rules.generated.json" --report "data/company/zhongshan_relation_rules.report.json" --auto-accept-strong --min-hit-rate 0.8 --sample-size 10000`

## 行为与结果的关键坑点
- `rdb2g.pipeline.ttl_builder` 会把每张表的映射缓存到 `data/mapping_cache/<db_stem>/`；表指纹变化只会使对应表缓存失效。
- `rdb2g.cli.build_relation_rules` 不读取 seed rules 时会从 schema profile + KB + LLM 候选 + 数据 probe 生成规则草案；`generated.json` 给机器使用，`report.json` 给人工审核。
- 向量索引持久化在 `data/chroma_db/<prefix>_<kb_or_schema_name>/`；知识库哈希或 schema 版本变化会触发重建。
- `rdb2g.cli.rdb_kg_eval` 会把 SPARQL 风格 KG 查询（如 `PREFIX`、`SELECT ?`）标记为 `not_cypher_query`；题库 KG 查询需改为 Cypher。
- `rdb2g.cli.rdb_kg_eval` 默认 `--db-path` 是 `data/company/poi.sqlite`；该文件不存在时必须显式传 `--db-path`。

## 安全与敏感信息
- `.env` 已在 `.gitignore` 中，保持本地即可；不要提交 API Key。
- 评测脚本里有 Neo4j 默认凭据；本地运行优先用环境变量或 CLI 参数覆盖，不要直接改脚本。

## 仓库约定
- 仓库没有现成的 test/lint/typecheck 配置；改动后用你触达的脚本做针对性验证。
- `.gitignore` 忽略了 `data/`；TTL、评测结果、缓存和索引等产物通常是本地文件。
- 所有 `python3` 运行命令均在虚拟环境下运行。
- 每次在 `docs/` 下新增文档时，必须先创建当天日期目录（格式 `YYYY-MM-DD`），并将新文档归档到该日期目录下。
