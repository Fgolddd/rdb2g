# RDB vs KG 测评报告（口语化题库）

## 1. 评测范围

- 题库：`docs/2026-04-10/rdb_vs_kg_eval_sample_cases_100_cypher.csv`
- 题量：100（口语化 `nl_question`）
- 执行脚本：`run_rdb_kg_eval.py`
- 输出文件：
  - `data/eval/eval_rdb_results.csv`
  - `data/eval/eval_kg_results.csv`
  - `data/eval/eval_rdb_kg_combined.csv`

## 2. 执行命令

```bash
/home/lyx/llm_graph/rdb2g/.venv/bin/python /home/lyx/llm_graph/rdb2g/run_rdb_kg_eval.py \
  --engine both \
  --question-bank /home/lyx/llm_graph/rdb2g/docs/2026-04-10/rdb_vs_kg_eval_sample_cases_100_cypher.csv \
  --db-path /home/lyx/llm_graph/rdb2g/data/company/poi.sqlite \
  --out-dir /home/lyx/llm_graph/rdb2g/data/eval
```

## 3. 总体结果

| 指标 | SQL | KG |
|---|---:|---:|
| 成功率 | 100% | 100% |
| 平均时延 | 66.020 ms | 645.028 ms |
| P95 时延 | 355.595 ms | 2465.385 ms |
| 0 结果条数 | 25 | 26 |

## 4. 分类型结果

| task_type | 样本数 | SQL成功率 | KG成功率 | SQL均值(ms) | KG均值(ms) | SQL零结果 | KG零结果 |
|---|---:|---:|---:|---:|---:|---:|---:|
| basic | 26 | 100% | 100% | 0.962 | 705.095 | 0 | 0 |
| join | 30 | 100% | 100% | 71.351 | 567.818 | 12 | 13 |
| semantic | 20 | 100% | 100% | 0.128 | 422.095 | 0 | 0 |
| multihop | 24 | 100% | 100% | 184.747 | 862.246 | 13 | 13 |

## 5. 慢查询（KG Top 8）

1. C018（3584.912 ms）麻烦帮我看下zs_POI_dm各中类有多少条。  
2. C001（3475.844 ms）麻烦帮我看下zs_POI_dm总共有多少条。  
3. C099（2967.989 ms）按区县帮我看麻烦帮我看下跨域多跳中的唯一门牌有多少条。  
4. C051（2956.904 ms）麻烦帮我看下无法关联门牌的房间有多少条。  
5. C002（2935.701 ms）麻烦帮我看下zs_AOI_dm总共有多少条。  
6. C092（2440.632 ms）按市级帮我看麻烦帮我看下POI->AOI->门牌关联路径。  
7. C087（2036.356 ms）从POI到AOI再到门牌关联路径示例。  
8. C094（1921.116 ms）麻烦帮我看下跨域多跳中的唯一POI数。  

## 6. 结论

1. 口语化题库不影响执行稳定性：RDB/KG 成功率均为 100%。
2. KG 性能仍显著慢于 SQL（平均约 9.8 倍，P95 约 6.9 倍）。
3. join/multihop 的 0 结果较多，仍受源数据关键桥接字段缺失影响（如 `BZ_GUID`/`GUID` 链路）。
4. 当前可用于“可执行性与性能”评估；若要提升“准确性对比”，需继续补齐关系字段并精修 Cypher 语义。
