"""Render the three-repeat aggregation with explicit failed-attempt records."""

from pathlib import Path
import json

from experiments.public_kg.summarize_semantic_repeats import BASE, ROOT


def number(value):
    return f'{value:.4f}' if value is not None else 'NA'


def main():
    summary = json.loads((BASE / 'summary.json').read_text())
    lines = [
        '# 语义映射三次独立重复实验结果', '',
        '日期：2026-10-30。四个方法均在佛山 OSM 10k 与 GeoNames CN 10k 冻结样本上安排三次新的独立运行；旧实验保留，不与本轮择优拼接。', '',
        '## 1. 评价与统计口径', '',
        '沿用已确认的第二轮机器审核参考，佛山评价 152/493 个字段，GeoNames 评价 29/30 个字段。其来源仍是机器审核，不是独立人工金标。评价只针对字段映射；LLM4VKG 使用原始 R2RML 字面值列到固定目标属性的严格 URI 投影。', '',
        '每次新建请求和预测目录，不复用旧预测。LLMatch 采用作者字段预测组件，Magneto Base 采用 MiniLM 与二分图重排；LLM4VKG 采用 OC/MG 适配，而非整篇论文完整流程。Full 是现有公开数据联合推理实现的字段输出。', '',
        '每次先计算 precision、recall、F1，再分别取三次算术平均；平均 F1 不等于平均 precision 和平均 recall 的调和平均。标准差为样本标准差（ddof=1）。失败不计为零分、不进入质量均值，必须同时报告成功次数；只有一次有效结果时不能估计样本标准差。', '',
        '## 2. 本轮均值', '',
        '| 数据集 | 方法 | 可评分/尝试 | Precision 均值 | Recall 均值 | F1 均值 | F1 样本标准差 |',
        '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
    names = {'foshan_osm': '佛山 OSM', 'geonames_cn': 'GeoNames CN'}
    for group in summary['groups']:
        lines.append(f"| {names[group['dataset']]} | {group['method']} | {group['scored']}/{group['attempts']} | {number(group['precision']['mean'])} | {number(group['recall']['mean'])} | {number(group['f1']['mean'])} | {number(group['f1']['sample_std'])} |")
    lines.extend(['', '本轮 Full 的字段 F1 均值在两库均低于 LLMatch：佛山低约 3.47 个百分点，GeoNames 低约 1.19 个百分点。Full 在两库均高于当前 Magneto Base 适配，在 GeoNames 高于 LLM4VKG 字段投影约 12.10 个百分点；佛山 LLM4VKG 无有效分数，不能计作被 Full 击败。', '',
                  '佛山 Full 的平均 precision 为 0.8791，高于 LLMatch 的 0.7559；平均 recall 为 0.4123，低于 LLMatch 的 0.4890，说明当前 Full 更偏向保守输出，覆盖不足仍是主要问题。GeoNames Full 三次 F1 一致，而 LLMatch 有一次更高；三次样本不足以声称差异统计显著。重复实验缩小了仅凭单次结果判断的风险，但没有证明多智能体在字段 F1 上全面领先。'])
    lines.extend(['', '## 3. 每次运行', '',
                  '| 数据集 | 方法 | 次数 | 状态 | Precision | Recall | F1 |',
                  '| --- | --- | ---: | --- | ---: | ---: | ---: |'])
    for row in summary['runs']:
        lines.append(f"| {names[row['dataset']]} | {row['method']} | {row['repeat']} | {row['status']} | {number(row['precision'])} | {number(row['recall'])} | {number(row['f1'])} |")
    lines.extend(['', '## 4. LLM4VKG 完成情况及原因', ''])
    lines.extend(['佛山三次均在 ontology completion 阶段发送 60 次真实请求，第 61 次被请求上限拦截，尚未生成完整 R2RML；三次已记录用量为 24,776、24,881、24,779 tokens，远低于 300,000 token 上限，因此本轮直接原因是请求次数限制。60 次请求的提示内容不同，不是将同一失败响应反复计算；这反映当前细粒度 OC 调用方式在本配置下尚未走完，不能推断该方法在更充分预算下无法完成。', '',
                  'GeoNames 三次分别发送 39、39、40 次请求，用量为 15,511、15,485、15,758 tokens，均生成了 R2RML。初次投影因 Python 子进程缺少仓库根路径而失败；后来只修复后处理并对原输出评分，没有重新请求模型。各次 attempt_initial_postprocess_failure.json 与 postprocess_recovery.json 保留原失败和恢复证据。', ''])
    for row in summary['runs']:
        if row['method'] != 'LLM4VKG':
            continue
        lines.append(f"### {names[row['dataset']]} r{row['repeat']}")
        lines.append('')
        lines.append(f"状态：{row['status']}。")
        if row['error']:
            lines.append('')
            lines.append(f"原始错误摘要：`{str(row['error']).replace(chr(10), ' ')}`")
        lines.extend(['', f"完整阶段、请求、用量和错误记录：`{row['attempt_manifest']}`。", ''])
    lines.extend(['## 5. 可复核材料', '',
                  '本轮结果目录：`data/eval/public_kg/semantic_repeats_20261030/`。`summary.json` 包含聚合结果和源文件 SHA256，`per_run.csv` 是逐次结果，`means.csv` 是均值与标准差。各方法目录保留预测、评分、独立运行清单及请求或组件记录。', '',
                  '本轮没有修改冻结数据、本体或机器参考，也没有以分数决定是否保留某次有效运行。Magneto 的三次执行若得到相同映射，其零标准差只说明当前固定实现结果一致，不代表消除了方法适配或标注误差。'])
    lines.extend(['', '## 6. 运行配置与失败轮留存', '',
                  'Full 沿用 qwen3.5-flash、temperature=0、9 次调用上限、100 万 token 预算、每请求 120 秒、24,000 输出 token 上限；每库执行三次新的联合推理。外部字段组件与 Full 任务范围、输入画像和预算不同，质量比较是本地适配组件比较，不能声称等计算成本或论文完整方法排名。', '',
                  'LLMatch 首轮为每请求 60 秒、16,000 输出上限，两库各三次：佛山 1/3 可评分、GeoNames 3/3。第二轮统一改为 180 秒、16,000 输出上限：佛山 1/3、GeoNames 3/3，佛山两个 JSONDecodeError 的输出均恰为 16,000 tokens，符合截断现象。最终补充轮对两库统一采用每请求 180 秒、32,000 输出上限，两库均 3/3 可评分，本文主表使用这一整轮全部三次，未混合择优。每轮仍保留 60 次请求、300,000 总 token 预算与 1,800 秒整轮墙钟上限。原两轮保存在 external/llmatch 与 external_timeout180/llmatch；最终轮为 external_timeout180_output32k/llmatch。失败不被最终轮覆盖。', '',
                  'Magneto Base 的正式输出位于 external_magneto_final/magneto-base；使用原 MiniLM、topk=20、sampling_size=10、二分图重排、关闭 GPT 重排。三次各自执行，两个库各自的归一化预测 SHA256 完全相同。依赖预检和模型权重本地缓存不等于复用预测缓存。Valentine 1.1.0 的 MatcherResults 缺少旧代码使用的 pop 接口，改用兼容的 0.4.1 后执行；未更改匹配算法或官方源码。', '',
                  'LLM4VKG 使用 qwen3.5-flash、CPU BGE-M3、每请求 60 秒、每轮 60 请求与 300,000 token 上限。缺 zhipuai 等依赖的零请求预检单独保留，不计作正式方法失败。正式三次都实际进入 OC/MG，佛山预算中止与 GeoNames 成功分别报告。', '',
                  '本地依赖在准备及恢复阶段补齐，transport SDK 也有版本变化，环境并非从任务开始始终固定；最终环境清单保存在 environment_final.txt。模型、prompt、数据和目标词表未据分数调整，32,000 输出上限轮是明确标识的容量补充实验。该差异应保留在复现说明中，不能称为所有方法完全相同预算的严格受控实验。', '',
                  '论文可以据此写：在当前公开数据适配与机器参考评价下，Full 对部分对照具有优势，佛山映射 precision 较高，但字段 F1 均值未超过 LLMatch。三次重复不是显著性检验，也不能替代单智能体及机制消融来证明角色协作的独立贡献。本轮未重新运行隐式关系对照或端到端查询，相关旧结果不由本轮字段均值替换。'])
    out = ROOT / 'docs/2026-10-30/semantic_mapping_three_repeat_results.md'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(out)


if __name__ == '__main__':
    main()
