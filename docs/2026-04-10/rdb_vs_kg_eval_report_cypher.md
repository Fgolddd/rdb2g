# RDB vs KG 测评报告（Cypher 版本）

## 1. 本次目标

将题库中的 KG 查询从 SPARQL 风格改为 Cypher，并基于同一批 100 条问题重新执行 RDB/KG 对比测试。

## 2. 本次变更

- 新 Cypher 题库：`docs/2026-04-10/rdb_vs_kg_eval_sample_cases_100_cypher.csv`
- 执行脚本：`run_rdb_kg_eval.py`（KG 路径改为 Neo4j 直连）
- 输出结果：
  - `data/eval/eval_rdb_results.csv`
  - `data/eval/eval_kg_results.csv`
  - `data/eval/eval_rdb_kg_combined.csv`

## 3. 执行命令

```bash
/home/lyx/llm_graph/rdb2g/.venv/bin/python /home/lyx/llm_graph/rdb2g/run_rdb_kg_eval.py \
  --engine both \
  --question-bank /home/lyx/llm_graph/rdb2g/docs/2026-04-10/rdb_vs_kg_eval_sample_cases_100_cypher.csv \
  --db-path /home/lyx/llm_graph/rdb2g/data/company/poi.sqlite \
  --out-dir /home/lyx/llm_graph/rdb2g/data/eval
```

## 4. 总体结果（100 条）

- SQL 成功率：**100%**
- KG 成功率：**100%**
- SQL 平均时延：**65.507 ms**
- KG 平均时延：**404.225 ms**
- SQL P95：**365.093 ms**
- KG P95：**1014.101 ms**

## 5. 分类型结果

- **basic (26)**
  - SQL: 100%, 平均 0.424 ms
  - KG: 100%, 平均 387.526 ms
- **join (30)**
  - SQL: 100%, 平均 65.017 ms
  - KG: 100%, 平均 422.732 ms
- **semantic (20)**
  - SQL: 100%, 平均 0.135 ms
  - KG: 100%, 平均 280.334 ms
- **multihop (24)**
  - SQL: 100%, 平均 191.103 ms
  - KG: 100%, 平均 502.424 ms

## 6. 结果解读

1. Cypher 改造后，KG 侧已可稳定执行（无语法错误）。
2. 在当前实现下，KG 在各类任务上的平均时延均高于 SQL，特别是 join/multihop。
3. RDB 与 KG 的“成功率”均为 100%，但这只代表查询可执行，不等于语义正确性完全一致。

## 7. 注意事项

- Neo4j 执行中出现了大量“属性键不存在”的 warning（非致命），原因是不同类型实体拥有不同属性集合。
- 当前对比主要体现“可执行性+性能”，若要做严格准确率评估，建议补充每题 Gold 答案自动比对规则。

## 8. 建议下一步

1. 对 KG 题库做人工复核，重点检查 join/multihop 是否与 SQL 语义完全一致。
2. 在 Neo4j 侧增加常用过滤字段索引，并避免大范围笛卡尔匹配。
3. 增加 accuracy/F1 自动判分脚本，形成可复现的完整评测闭环。
