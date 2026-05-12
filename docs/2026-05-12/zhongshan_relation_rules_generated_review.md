# 中山关系规则自动生成结果评审

## 背景

本报告对比以下两份关系规则：

- 人工规则：`data/company/zhongshan_relation_rules.json`
- 自动生成规则：`/tmp/opencode/zhongshan_relation_rules.generated.json`
- 自动生成报告：`/tmp/opencode/zhongshan_relation_rules.report.json`

自动生成方式为无 seed 规则模式，仅允许使用 `data/company/zhongshan_rag_terms.json` 作为字段语义参考，并通过 schema profile、LLM 候选和数据 probe 生成规则草案。

## 总体对比


| 项       | 人工规则        | 自动生成规则      |
| ------- | ----------- | ----------- |
| 规则条数    | 7           | 32          |
| 自动启用强规则 | 人工控制        | 18          |
| 弱规则     | 1 条，禁用      | 2 条，禁用      |
| 覆盖方式    | 每类关系合并成一条规则 | 多数按源表拆成多条规则 |
| 可读性     | 更清晰         | 更详细但较碎      |
| 稳定性     | 高           | 中等，需要审核     |


自动生成报告摘要：

```json
{
  "candidate_rules": 32,
  "accepted_strong_rules": 18,
  "rejected_or_disabled_rules": 12,
  "weak_candidate_rules": 2
}
```

## 自动生成效果好的部分

自动生成流程能从 0 发现多类高置信强关系，说明“KB 语义参考 + schema profile + 数据 probe”的路线有效。

### `managedByPoliceOrg`

自动启用源表包括：

- `zs_DH_bz`
- `zs_DY_bz`
- `zs_MP_bz`
- `zs_YL_bz`
- `zs_city_bz`
- `zs_community_bz`
- `zs_facility_dm`
- `zs_roadcross_dm`
- `zs_street_bz`
- `zs_town_bz`

多数命中率在 `0.95` 以上，典型结果：


| 源表                | 命中率    | 主要命中对                          |
| ----------------- | ------ | ------------------------------ |
| `zs_MP_bz`        | 1.0    | `zs_MP_bz -> zs_PCS_xq`        |
| `zs_DH_bz`        | 0.9492 | `zs_DH_bz -> zs_PCS_xq`        |
| `zs_facility_dm`  | 0.9913 | `zs_facility_dm -> zs_PCS_xq`  |
| `zs_roadcross_dm` | 0.9647 | `zs_roadcross_dm -> zs_PCS_xq` |


### `parentPoliceOrg`

自动启用：

- `zs_GAFJ_xq -> zs_GAJ_xq`
- `zs_JWS_xq -> zs_PCS_xq`
- `zs_PCS_xq -> zs_GAFJ_xq`

命中率均为 `1.0`。

### `parentAdminArea`

自动启用：

- `zs_street_bz -> zs_city_bz`
- `zs_town_bz -> zs_city_bz`

命中率均为 `1.0`。

### `partOfAddressEntity`

自动启用：

- `zs_DH_bz -> zs_YL_bz`，命中率 `0.9086`
- `zs_DY_bz -> zs_DH_bz`，命中率 `0.8162`

### `locatedInAdminArea`

自动启用：

- `zs_roadcross_dm -> zs_city_bz`，命中率 `1.0`

## 漏掉或未启用但人工规则有价值的部分


| 关系                                      | 自动生成状态 | 评审                                                           |
| --------------------------------------- | ------ | ------------------------------------------------------------ |
| `locatedOnStreet`                       | 发现但未启用 | `hit_rate=0.5887`，低于 `0.8`，但可产生 `11774` 条有效边，业务上有价值          |
| `partOfAddressEntity` for `zs_FJ_bz`    | 未启用    | `hit_rate=0.6165`，但可产生 `5813` 条 `Room -> Building`           |
| `partOfAddressEntity` for `zs_MP_bz`    | 未启用    | `hit_rate=0.3185`，但可产生 `3185` 条 `Doorplate -> Street`        |
| `partOfAddressEntity` for `zs_YL_bz`    | 未启用    | `hit_rate=0.0186`，价值较低，自动禁用合理                                |
| `parentAdminArea` for `zs_community_bz` | 未启用    | 命中率 `1.0`，但包含 `zs_community_bz -> zs_community_bz` 风险，自动禁用合理 |


`locatedOnStreet` 的 probe 结果：

```json
{
  "source_table": "zs_roadcross_dm",
  "match_mode": "split_exact",
  "source_values": 20000,
  "hits": 11774,
  "hit_rate": 0.5887,
  "fanout_by_pair": {
    "zs_roadcross_dm->zs_street_bz": 11774
  }
}
```

这说明统一使用 `--min-hit-rate 0.8` 会误伤一部分“命中率不够高但边数量大且业务有效”的规则。

## 自动生成比人工规则更谨慎的地方

自动生成器会按数据证据禁用空字段或低命中规则。例如：

- `managedByPoliceOrg` for `zs_AOI_dm`：非空率 `0`
- `managedByPoliceOrg` for `zs_POI_dm`：非空率 `0`
- `managedByPoliceOrg` for `zs_FJ_bz`：源字段非空但命中 `0`
- `locatedInAdminArea` for `zs_facility_dm`：非空率 `0`
- `parentPoliceOrg` for `zs_GAJ_xq`：无有效上级命中

这比人工规则更保守，能减少空字段带来的无效规则，但也会漏掉部分低命中业务关系。

## 主要问题

1. 自动规则过于保守。

当前只按统一 `--min-hit-rate 0.8` 决定是否启用强规则，导致 `locatedOnStreet`、`Room -> Building` 等有效关系被禁用。

1. 自动规则拆分较碎。

人工规则把多个 source table 合并成一条 `managedByPoliceOrg`；自动规则多数按源表拆成多条。机器可读没有问题，但人工维护成本更高。

1. 实体类型命名仍需规范化。

人工规则使用稳定类型名，如 `POI`、`AOI`、`RoadCross`、`PoliceOrg`。自动规则可能生成 `PointOfInterest`、`AreaOfInterest`、`RoadCrossing`、`PoliceOffice` 等变体，后续查询模板和类型聚合会受影响。

1. 自动生成器还不能充分理解“低命中但业务有效”。

这类规则需要结合边数量、业务字段语义、目标唯一性和样例共同判断，而不能只看 hit rate。

## 改进建议

1. 为不同关系类型设置不同启用阈值。

- `managedByPoliceOrg`、`parentPoliceOrg`：继续使用高阈值，如 `0.8`。
- `split_exact`：增加 `min_hit_count` 或 `subject_hit_rate`，避免 `locatedOnStreet` 被误禁。
- `partOfAddressEntity`：允许按层级分别判断，而不是对整条规则使用统一 hit rate。

1. 增加 `review_recommended` 状态。

对于 `hit_rate` 未达标但 `hit_count` 很高的规则，不直接启用，也不普通禁用，而是标记为人工重点审核。

1. 合并同名同结构规则。

把多个 source table 的同类规则合并回人工版结构，提高可读性和维护性。

1. 固定 canonical entity type。

将自动生成类型映射到稳定集合：

- `PointOfInterest -> POI`
- `AreaOfInterest -> AOI`
- `RoadCrossing -> RoadCross`
- `PoliceOffice/PoliceStation/PoliceSubstation -> PoliceOrg`

1. 保留现有人工规则作为生产基线。

自动生成结果更适合作为候选发现和质量审计工具，不建议直接替换人工规则。

## 结论

自动生成器已经能从 0 发现较多高置信强关系，尤其是公安机构管理、公安机构上下级、部分行政层级、部分地址层级和路口行政归属关系。

但当前生成规则还不能直接替代人工规则，主要原因是：低命中但业务有效的规则会被禁用，规则拆分较碎，实体类型命名不够稳定。建议短期定位为“关系规则候选发现 + 数据验证报告生成”工具，生产构图仍以人工版 `data/company/zhongshan_relation_rules.json` 为主。