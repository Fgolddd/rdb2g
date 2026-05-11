import json
import urllib.parse
from pathlib import Path


class RelationRuleSet:
    def __init__(self, config=None):
        self.config = config or {}
        self.version = str(self.config.get("version", "") or "")
        self.namespace = str(self.config.get("namespace", "http://example.org/zhongshan/") or "").rstrip("/") + "/"
        self.entity_base_uri = str(self.config.get("entity_base_uri", "http://example.org/data/") or "").rstrip("/") + "/"
        self.table_entity_types = self.config.get("table_entity_types") or {}
        self.entity_key_priority = self.config.get("entity_key_priority") or {}
        self.name_field_priority = self.config.get("name_field_priority") or {}
        self.relation_rules = [r for r in self.config.get("relation_rules", []) if isinstance(r, dict)]

    @classmethod
    def load(cls, path=None):
        if not path:
            return cls({})
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"关系规则文件不存在: {config_path}")
        with config_path.open("r", encoding="utf-8") as f:
            return cls(json.load(f))

    def enabled(self):
        return bool(self.config)

    def entity_type_for_table(self, table_name):
        return str(self.table_entity_types.get(table_name, "") or "")

    def entity_key_fields(self, table_name):
        fields = self.entity_key_priority.get(table_name) or self.entity_key_priority.get("default") or ["GUID", "DM", "gid"]
        return [str(f) for f in fields if str(f).strip()]

    def name_fields(self, table_name):
        fields = self.name_field_priority.get(table_name) or self.name_field_priority.get("default") or ["MC", "DM", "GUID", "gid"]
        return [str(f) for f in fields if str(f).strip()]

    def pick_first_value(self, row, fields):
        for field in fields:
            if field not in row:
                continue
            value = row.get(field)
            if value is None:
                continue
            text = str(value).strip()
            if text and text.lower() not in {"null", "none", "nan"}:
                return field, text
        return None, None

    def entity_id_for_row(self, table_name, row, fallback=None):
        _, value = self.pick_first_value(row, self.entity_key_fields(table_name))
        if value:
            return value
        return str(fallback if fallback is not None else "row")

    def label_for_row(self, table_name, row, fallback=None):
        _, value = self.pick_first_value(row, self.name_fields(table_name))
        if value:
            return value
        return str(fallback if fallback is not None else self.entity_id_for_row(table_name, row))

    def uri_for_entity(self, table_name, entity_id):
        safe_table = urllib.parse.quote(str(table_name or "unknown"), safe="")
        safe_id = urllib.parse.quote(str(entity_id or "row"), safe="")
        return f"{self.entity_base_uri}{safe_table}/{safe_id}"

    def relation_uri(self, relation_name):
        return f"{self.namespace}{urllib.parse.quote(str(relation_name), safe='')}"

    def type_uri(self, entity_type):
        return f"{self.namespace}{urllib.parse.quote(str(entity_type), safe='')}"

    def source_key_candidates(self, rule):
        keys = []
        if rule.get("source_key"):
            keys.append(rule.get("source_key"))
        keys.extend(rule.get("source_key_candidates") or [])
        keys.extend(rule.get("source_key_priority") or [])
        seen = set()
        result = []
        for key in keys:
            key = str(key or "").strip()
            if key and key not in seen:
                seen.add(key)
                result.append(key)
        return result

    def source_label_candidates(self, rule):
        keys = []
        if rule.get("source_label_field"):
            keys.append(rule.get("source_label_field"))
        keys.extend(rule.get("source_label_candidates") or [])
        return [str(k) for k in keys if str(k).strip()]

    def target_specs_for_rule(self, rule, row=None):
        if rule.get("target_tables_by_level"):
            level_field = rule.get("source_level_field")
            level_value = str((row or {}).get(level_field, "") or "").strip()
            target_tables = rule.get("target_tables_by_level", {}).get(level_value, [])
            target_keys = rule.get("target_key_priority") or []
            return [(table, key) for table in target_tables for key in target_keys]

        target_tables = rule.get("target_tables") or []
        target_keys = []
        if rule.get("target_key"):
            target_keys.append(rule.get("target_key"))
        target_keys.extend(rule.get("target_key_priority") or [])
        return [(table, key) for table in target_tables for key in target_keys]

    def target_index_specs(self):
        specs = set()
        for rule in self.relation_rules:
            if rule.get("enabled") is False:
                continue
            if rule.get("target_tables_by_level"):
                target_keys = rule.get("target_key_priority") or []
                for tables in (rule.get("target_tables_by_level") or {}).values():
                    for table in tables:
                        for key in target_keys:
                            specs.add((str(table), str(key)))
                continue

            target_tables = rule.get("target_tables") or []
            target_keys = []
            if rule.get("target_key"):
                target_keys.append(rule.get("target_key"))
            target_keys.extend(rule.get("target_key_priority") or [])
            for table in target_tables:
                for key in target_keys:
                    specs.add((str(table), str(key)))
        return sorted(specs)
