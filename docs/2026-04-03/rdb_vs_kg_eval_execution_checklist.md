# RDB vs KG 评测执行清单（可直接落地）

## Phase 0：准备

- 确认数据版本：`data/company/poi.sqlite`
- 确认图谱版本：`data/ttl/poi.ttl`
- 固定测试环境（CPU/内存/网络）
- 约定统一超时时间（建议 30s）
- 约定统一重试策略（建议不重试或仅1次）

## Phase 1：构建问题集

- 使用 `rdb_vs_kg_eval_question_bank_template.csv` 建立问题集
- 每条问题补齐 SQL / SPARQL（或图查询）答案
- 产出 Gold 标准答案（JSON）
- 覆盖四类任务：基础/关联/语义/多跳

## Phase 2：执行 SQL 基线

- 对全部问题执行 SQL
- 记录每条问题耗时、是否成功、结果
- 保存到 `eval_sql_results.csv`

## Phase 3：执行 KG 基线

- 对全部问题执行 KG 查询
- 记录每条问题耗时、是否成功、结果
- 保存到 `eval_kg_results.csv`

## Phase 4：人工可解释性评分

- 按评分模板对每条问题打分
- SQL、KG分别评分（1~5）
- 保存到 `eval_explainability_scores.csv`

## Phase 5：自动汇总

- 按 `rdb_vs_kg_eval_automation_spec.md` 统计指标
- 输出 `eval_summary_by_type.csv`
- 输出 `eval_summary_overall.csv`
- 生成最终结论页（优劣势 + 适用场景）

