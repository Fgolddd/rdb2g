from copy import deepcopy


DEFAULT_NAMESPACE = "http://example.org/zhongshan/"
DEFAULT_ENTITY_BASE_URI = "http://example.org/data/"
CANONICAL_RELATION_NAMES = {
    "managedByPoliceOrg",
    "parentPoliceOrg",
    "parentAdminArea",
    "locatedInAdminArea",
    "partOfAddressEntity",
    "locatedOnStreet",
    "possibleStandardAddressTextMatch",
}


def _existing_tables(schema_profile):
    return {table.get("name") for table in schema_profile.get("tables", [])}


def _columns_by_table(schema_profile):
    result = {}
    for table in schema_profile.get("tables", []):
        result[table.get("name")] = {col.get("name") for col in table.get("columns", [])}
    return result


def _list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def _dedupe(values):
    result = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def _filter_fields(fields, source_tables, columns_by_table):
    if not fields:
        return []
    result = []
    for field in fields:
        if any(field in columns_by_table.get(table, set()) for table in source_tables):
            result.append(field)
    return _dedupe(result)


def _filter_rule(rule, schema_profile):
    if not isinstance(rule, dict):
        return None
    tables = _existing_tables(schema_profile)
    cols = _columns_by_table(schema_profile)
    normalized = deepcopy(rule)

    source_tables = [t for t in _list(rule.get("source_tables")) if t in tables]
    target_tables = [t for t in _list(rule.get("target_tables")) if t in tables]
    if not source_tables:
        return None
    normalized["source_tables"] = source_tables
    if target_tables:
        normalized["target_tables"] = target_tables

    if rule.get("target_tables_by_level"):
        by_level = {}
        for level, level_tables in (rule.get("target_tables_by_level") or {}).items():
            kept = [t for t in _list(level_tables) if t in tables]
            if kept:
                by_level[str(level)] = kept
        if by_level:
            normalized["target_tables_by_level"] = by_level
        elif not target_tables:
            return None
    elif not target_tables:
        return None

    for key in ("source_key", "source_label_field", "source_level_field"):
        value = str(rule.get(key) or "").strip()
        if value and any(value in cols.get(table, set()) for table in source_tables):
            normalized[key] = value
        else:
            normalized.pop(key, None)

    for key in ("source_key_candidates", "source_key_priority"):
        fields = _filter_fields(_list(rule.get(key)), source_tables, cols)
        if fields:
            normalized[key] = fields
        else:
            normalized.pop(key, None)

    candidate_source_fields = []
    candidate_source_fields.extend(_list(normalized.get("source_key")))
    candidate_source_fields.extend(_list(normalized.get("source_key_candidates")))
    candidate_source_fields.extend(_list(normalized.get("source_key_priority")))
    if not candidate_source_fields:
        return None

    target_field_tables = target_tables
    if normalized.get("target_tables_by_level"):
        target_field_tables = []
        for values in normalized["target_tables_by_level"].values():
            target_field_tables.extend(values)

    match_mode = str(normalized.get("match_mode") or "exact").strip()
    if match_mode not in {"exact", "split_exact", "text_candidate"}:
        match_mode = "exact"
    normalized["match_mode"] = match_mode

    for key in ("target_key",):
        value = str(rule.get(key) or "").strip()
        if value and any(value in cols.get(table, set()) for table in target_field_tables):
            normalized[key] = value
        else:
            normalized.pop(key, None)

    for key in ("target_key_priority",):
        fields = _filter_fields(_list(rule.get(key)), target_field_tables, cols)
        if fields:
            normalized[key] = fields
        else:
            normalized.pop(key, None)

    target_name_fields = _filter_fields(_list(rule.get("target_name_field_priority")), target_field_tables, cols)
    if target_name_fields:
        normalized["target_name_field_priority"] = target_name_fields
    else:
        normalized.pop("target_name_field_priority", None)

    has_target_key = bool(normalized.get("target_key") or normalized.get("target_key_priority"))
    has_text_target = match_mode == "text_candidate" and bool(normalized.get("target_name_field_priority"))
    if not has_target_key and not has_text_target:
        return None

    if match_mode == "split_exact" and not normalized.get("split_delimiter"):
        normalized["split_delimiter"] = ","

    normalized["name"] = canonical_relation_name(normalized)
    normalized["enabled"] = False
    return normalized


def canonical_relation_name(rule):
    raw_name = str(rule.get("name") or "").strip()
    if raw_name in CANONICAL_RELATION_NAMES:
        return raw_name
    text = " ".join([
        raw_name,
        str(rule.get("agent_reason") or ""),
        " ".join(_list(rule.get("source_tables"))),
        " ".join(_list(rule.get("target_tables"))),
    ]).lower()
    match_mode = str(rule.get("match_mode") or "exact")
    confidence = str(rule.get("edge_confidence") or "").lower()

    if confidence == "weak" or match_mode == "text_candidate" or "text" in text or "address_link" in text:
        return "possibleStandardAddressTextMatch"
    if "street" in text and ("szdldm" in text or match_mode == "split_exact" or "road" in text or "intersection" in text):
        return "locatedOnStreet"
    if "police" in text or "pcs" in text or "jws" in text or "gafj" in text or "gaj" in text:
        if "parent" in text or "branch" in text or "station" in text or "precinct" in text or "squad" in text:
            return "parentPoliceOrg"
        return "managedByPoliceOrg"
    if "admin" in text or "community" in text or "town" in text or "city" in text:
        if "parent" in text or "administration" in text:
            return "parentAdminArea"
        return "locatedInAdminArea"
    if any(token in text for token in ("doorplate", "yard", "building", "unit", "room", "address", "compound")):
        return "partOfAddressEntity"
    return raw_name or "candidateRelation"


def normalize_agent_output(agent_output, schema_profile):
    tables = _existing_tables(schema_profile)
    cols = _columns_by_table(schema_profile)
    entity_type_hints = schema_profile.get("entity_type_hints", {})

    table_entity_types = {}
    for table in tables:
        value = str((agent_output.get("table_entity_types") or {}).get(table) or entity_type_hints.get(table) or "Entity").strip()
        table_entity_types[table] = value or "Entity"

    entity_key_priority = {"default": ["GUID", "DM", "gid"]}
    name_field_priority = {"default": ["MC", "DM", "GUID", "gid"]}
    for table in tables:
        table_cols = cols.get(table, set())
        key_fields = [f for f in _list((agent_output.get("entity_key_priority") or {}).get(table)) if f in table_cols]
        if not key_fields:
            key_fields = [f for f in ["DM", "GUID", "gid"] if f in table_cols]
        if key_fields:
            entity_key_priority[table] = key_fields

        name_fields = [f for f in _list((agent_output.get("name_field_priority") or {}).get(table)) if f in table_cols]
        if not name_fields:
            preferred = ["MC", "MPQC", "DZ", "LKMC", "SSMC", "GAMC", "DM", "GUID", "gid"]
            name_fields = [f for f in preferred if f in table_cols]
        if name_fields:
            name_field_priority[table] = name_fields

    rules = []
    seen = set()
    for rule in agent_output.get("relation_rules", []) or []:
        normalized = _filter_rule(rule, schema_profile)
        if not normalized:
            continue
        sig = str({k: normalized.get(k) for k in ("name", "source_tables", "source_key", "source_key_candidates", "source_key_priority", "target_tables", "target_key", "target_key_priority", "target_tables_by_level", "match_mode")})
        if sig in seen:
            continue
        seen.add(sig)
        rules.append(normalized)

    return {
        "version": "auto-generated-v1",
        "namespace": DEFAULT_NAMESPACE,
        "entity_base_uri": DEFAULT_ENTITY_BASE_URI,
        "table_entity_types": dict(sorted(table_entity_types.items())),
        "entity_key_priority": entity_key_priority,
        "name_field_priority": name_field_priority,
        "relation_rules": rules,
    }


def merge_candidate_rules(*rule_lists):
    merged = []
    seen = set()
    for rules in rule_lists:
        for rule in rules or []:
            sig = str({k: rule.get(k) for k in ("name", "source_tables", "source_key", "source_key_candidates", "source_key_priority", "target_tables", "target_key", "target_key_priority", "target_tables_by_level", "match_mode")})
            if sig in seen:
                continue
            seen.add(sig)
            merged.append(rule)
    return merged


def apply_validation(generated_config, validations, min_hit_rate=0.8, auto_accept_strong=False):
    rules = generated_config.get("relation_rules", [])
    accepted = 0
    weak = 0
    rejected = 0
    for idx, rule in enumerate(rules):
        validation = validations[idx] if idx < len(validations) else {}
        confidence = str(rule.get("edge_confidence", "")).lower()
        match_mode = str(rule.get("match_mode", "exact"))
        relation_name = str(rule.get("name") or "")
        is_weak = confidence == "weak" or match_mode == "text_candidate"
        if is_weak:
            rule["enabled"] = False
            weak += 1
            continue
        hit_rate = float(validation.get("hit_rate", 0.0) or 0.0)
        self_loop_count = int(validation.get("self_loop_count", 0) or 0)
        target_ok = True
        for target in (validation.get("target_indexes") or {}).values():
            if float(target.get("unique_rate", 0.0) or 0.0) < 0.5:
                target_ok = False
                break
        should_accept = (
            auto_accept_strong
            and relation_name in CANONICAL_RELATION_NAMES
            and hit_rate >= float(min_hit_rate)
            and self_loop_count == 0
            and target_ok
        )
        rule["enabled"] = bool(should_accept)
        if should_accept:
            accepted += 1
        else:
            rejected += 1
    return {
        "candidate_rules": len(rules),
        "accepted_strong_rules": accepted,
        "rejected_or_disabled_rules": rejected,
        "weak_candidate_rules": weak,
    }
