# RDB vs KG 自动统计脚本结构说明

## 1. 输入文件

1. `rdb_vs_kg_eval_question_bank_template.csv`（问题元信息）
2. `rdb_vs_kg_eval_scoring_template.csv`（执行与评分结果）

## 2. 输出文件

1. `eval_summary_overall.csv`
2. `eval_summary_by_type.csv`
3. `eval_error_breakdown.csv`

## 3. 指标计算

## 3.1 Overall

- `sql_accuracy = SUM(sql_answer_correct) / N`
- `kg_accuracy = SUM(kg_answer_correct) / N`
- `sql_success_rate = SUM(sql_success) / N`
- `kg_success_rate = SUM(kg_success) / N`
- `sql_latency_avg = AVG(sql_latency_ms)`
- `kg_latency_avg = AVG(kg_latency_ms)`
- `sql_latency_p95 = P95(sql_latency_ms)`
- `kg_latency_p95 = P95(kg_latency_ms)`
- `sql_explainability_avg = AVG(sql_explainability_score)`
- `kg_explainability_avg = AVG(kg_explainability_score)`

## 3.2 By task_type

按 `task_type in {basic, join, semantic, multihop}` 分组，重复上述指标。

## 4. 脚本建议结构

```text
scripts/
  eval/
    aggregate_eval.py
    utils.py
```

## 4.1 `aggregate_eval.py` 伪代码

```python
load question_bank
load scoring
df = scoring.merge(question_bank[["id", "task_type"]], on="id", how="left")

overall = calc_metrics(df)
by_type = df.groupby("task_type").apply(calc_metrics)
error_breakdown = build_error_stats(df)

save overall/by_type/error_breakdown
```

## 5. 结果判读规则（建议）

1. 若 `KG准确率 - SQL准确率 >= 10%` 且 `KG可解释性均分更高`，判定 KG 在该类任务显著更优。  
2. 若 `SQL P95` 显著小于 `KG P95` 且准确率差异不大，判定该类任务优先 SQL。  
3. 推荐最终输出“任务分层结论”，而非单一全局胜负。
