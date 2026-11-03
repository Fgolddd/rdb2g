import json
import os
from pathlib import Path

from dotenv import load_dotenv

from rdb2g.data.sqlite_loader import SpiderDataLoader
from rdb2g.mapping.relation_rule_agent import RelationRuleAgent
from rdb2g.mapping.rule_candidate_normalizer import (
    apply_validation,
    merge_candidate_rules,
    normalize_agent_output,
)
from rdb2g.profiling.relation_probe import discover_deterministic_candidates, probe_rules
from rdb2g.profiling.schema_profiler import build_schema_profile
from rdb2g.retrieval.schema_parser import parse_private_kb


load_dotenv()


def _write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_relation_rules(
    db_path,
    kb_file=None,
    out_path="data/company/relation_rules.generated.json",
    report_path="data/company/relation_rules.report.json",
    auto_accept_strong=False,
    min_hit_rate=0.8,
    sample_size=10000,
):
    kb_terms = parse_private_kb(kb_file) if kb_file else []
    loader = SpiderDataLoader(db_path)
    try:
        print("=== Step 1: Schema profiling ===")
        schema_profile = build_schema_profile(loader, kb_terms=kb_terms, sample_size=sample_size)

        print("=== Step 2: Deterministic candidate discovery ===")
        deterministic_rules = discover_deterministic_candidates(schema_profile)
        print(f"确定性候选规则: {len(deterministic_rules)}")

        print("=== Step 3: Relation Rule Agent ===")
        agent_output = RelationRuleAgent().propose_rules(schema_profile)
        normalized = normalize_agent_output(agent_output, schema_profile)

        normalized["relation_rules"] = merge_candidate_rules(
            deterministic_rules,
            normalized.get("relation_rules", []),
        )
        normalized = normalize_agent_output(normalized, schema_profile)
        print(f"合并后候选规则: {len(normalized.get('relation_rules', []))}")

        print("=== Step 4: Probe and validation ===")
        validations = probe_rules(loader, normalized.get("relation_rules", []), sample_size=sample_size)
        summary = apply_validation(
            normalized,
            validations,
            min_hit_rate=min_hit_rate,
            auto_accept_strong=auto_accept_strong,
        )

        report_rules = []
        for idx, rule in enumerate(normalized.get("relation_rules", [])):
            validation = validations[idx] if idx < len(validations) else {}
            report_rules.append({
                "index": idx,
                "name": rule.get("name"),
                "enabled": rule.get("enabled", False),
                "edge_confidence": rule.get("edge_confidence"),
                "match_mode": rule.get("match_mode"),
                "source_tables": rule.get("source_tables"),
                "target_tables": rule.get("target_tables"),
                "agent_reason": rule.get("agent_reason", ""),
                "validation": validation,
            })

        report = {
            "summary": summary,
            "inputs": {
                "db_path": str(db_path),
                "kb_file": str(kb_file) if kb_file else None,
                "sample_size": int(sample_size),
                "min_hit_rate": float(min_hit_rate),
                "auto_accept_strong": bool(auto_accept_strong),
                "seed_rules": None,
            },
            "rules": report_rules,
        }

        _write_json(out_path, normalized)
        _write_json(report_path, report)
        print(f"Generated relation rules: {out_path}")
        print(f"Validation report: {report_path}")
        return normalized, report
    finally:
        loader.close()
