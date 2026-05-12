import json
import time

from rdb2g.mapping.chat_client import QwenChatClient
from rdb2g.mapping.json_utils import parse_json_output
from rdb2g.profiling.schema_profiler import compact_profile_for_agent


class RelationRuleAgent:
    def __init__(self):
        self.chat_client = QwenChatClient()

    def propose_rules(self, schema_profile):
        compact = compact_profile_for_agent(schema_profile)
        system_prompt = (
            "你是一名关系型数据库到知识图谱的关系规则设计专家。"
            "请从数据库字段、样本和 KB 语义中推断可验证的关系规则。"
            "只返回 JSON，不要输出 Markdown。"
        )
        user_prompt = f"""
数据库 profile:
{json.dumps(compact, ensure_ascii=False)}

请从 0 推断 relation_rules 配置草案，输出 JSON 对象：
{{
  "table_entity_types": {{"表名": "实体类型英文名"}},
  "entity_key_priority": {{"表名": ["候选ID字段"]}},
  "name_field_priority": {{"表名": ["候选显示字段"]}},
  "relation_rules": [
    {{
      "name": "关系英文名",
      "source_tables": ["源表"],
      "source_key" 或 "source_key_candidates" 或 "source_key_priority": "字段或字段列表",
      "source_label_field": "可选字段",
      "source_level_field": "可选字段",
      "target_tables": ["目标表"],
      "target_key" 或 "target_key_priority": "字段或字段列表",
      "target_tables_by_level": {{"层级值": ["目标表"]}},
      "match_mode": "exact|split_exact|text_candidate",
      "split_delimiter": ",",
      "edge_confidence": "strong|strong_if_hit_rate_high|weak",
      "enabled": false,
      "agent_reason": "为什么推断这条规则"
    }}
  ]
}}

规则：
1. 只使用 profile 中存在的表和字段。
2. 强关系优先 exact 或 split_exact；文本相似只能 text_candidate 且 edge_confidence=weak。
3. 不要虚构公共本体 URI。
4. 不要输出超过 30 条 relation_rules。
5. 如果不确定，enabled=false，并在 agent_reason 说明风险。
"""
        started = time.perf_counter()
        print(f"Relation Rule Agent Chat 开始，timeout={self.chat_client.timeout:.0f}s")
        content = self.chat_client.complete([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ])
        elapsed = time.perf_counter() - started
        print(f"Relation Rule Agent Chat 完成，耗时 {elapsed:.1f}s")
        parsed = parse_json_output(content, fallback={})
        return parsed if isinstance(parsed, dict) else {}
