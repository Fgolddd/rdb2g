from collections import Counter, defaultdict


def _clean(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nan"}:
        return ""
    return text


def _available_columns(loader, table):
    return {row[1] for row in loader.get_table_columns(table)}


def _sample_values(loader, table, columns, sample_size):
    available = _available_columns(loader, table)
    selected = [col for col in columns if col in available]
    if not selected:
        return []
    projected = ", ".join(loader._quote_identifier(col) for col in selected)
    cursor = loader.conn.cursor()
    cursor.execute(f"SELECT {projected} FROM `{table}` LIMIT {max(int(sample_size), 1)}")
    rows = []
    for raw in cursor.fetchall():
        rows.append({col: raw[idx] for idx, col in enumerate(selected)})
    return rows


def _target_index(loader, table, key, sample_size):
    rows = _sample_values(loader, table, [key], sample_size)
    counts = Counter(_clean(row.get(key)) for row in rows if _clean(row.get(key)))
    values = set(counts)
    duplicate_values = sum(1 for count in counts.values() if count > 1)
    return {
        "values": values,
        "counts": counts,
        "row_count": len(rows),
        "non_empty_count": sum(counts.values()),
        "distinct_count": len(values),
        "duplicate_values": duplicate_values,
        "unique_rate": round((len(values) / sum(counts.values())), 4) if counts else 0.0,
    }


def _source_values_for_rule(rule, row):
    keys = []
    if rule.get("source_key"):
        keys.append(rule.get("source_key"))
    keys.extend(rule.get("source_key_candidates") or [])
    keys.extend(rule.get("source_key_priority") or [])
    split_mode = str(rule.get("match_mode", "exact")) == "split_exact"
    delimiter = str(rule.get("split_delimiter", ",") or ",")
    values = []
    for key in keys:
        value = _clean(row.get(key))
        if not value:
            continue
        if split_mode:
            values.extend(part.strip() for part in value.split(delimiter) if part.strip())
        else:
            values.append(value)
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _target_specs(rule, row=None):
    if rule.get("target_tables_by_level"):
        level_field = rule.get("source_level_field")
        level_value = _clean((row or {}).get(level_field))
        target_tables = (rule.get("target_tables_by_level") or {}).get(level_value, [])
        target_keys = rule.get("target_key_priority") or []
        return [(table, key) for table in target_tables for key in target_keys]
    target_tables = rule.get("target_tables") or []
    target_keys = []
    if rule.get("target_key"):
        target_keys.append(rule.get("target_key"))
    target_keys.extend(rule.get("target_key_priority") or [])
    return [(table, key) for table in target_tables for key in target_keys]


def probe_rule(loader, rule, sample_size=10000, max_examples=8):
    match_mode = str(rule.get("match_mode", "exact"))
    if match_mode not in {"exact", "split_exact"}:
        return {
            "status": "not_probed",
            "reason": f"match_mode {match_mode!r} is not deterministic exact matching",
            "match_mode": match_mode,
        }

    source_tables = rule.get("source_tables") or []
    target_index_cache = {}
    per_source = []
    total_values = 0
    total_hits = 0
    total_rows = 0
    non_empty_rows = 0
    self_loop_count = 0
    fanout = Counter()
    sample_hits = []
    sample_misses = []

    source_columns = []
    if rule.get("source_key"):
        source_columns.append(rule.get("source_key"))
    source_columns.extend(rule.get("source_key_candidates") or [])
    source_columns.extend(rule.get("source_key_priority") or [])
    if rule.get("source_level_field"):
        source_columns.append(rule.get("source_level_field"))
    source_columns.extend(["DM", "GUID", "gid"])
    source_columns = [col for idx, col in enumerate(source_columns) if col and col not in source_columns[:idx]]

    for source_table in source_tables:
        rows = _sample_values(loader, source_table, source_columns, sample_size)
        source_values = 0
        source_hits = 0
        source_non_empty_rows = 0
        for row_index, row in enumerate(rows):
            values = _source_values_for_rule(rule, row)
            if values:
                source_non_empty_rows += 1
            row_hits = 0
            for value in values:
                total_values += 1
                source_values += 1
                matched = False
                for target_table, target_key in _target_specs(rule, row):
                    cache_key = (target_table, target_key)
                    if cache_key not in target_index_cache:
                        target_index_cache[cache_key] = _target_index(loader, target_table, target_key, sample_size)
                    target_index = target_index_cache[cache_key]
                    if value in target_index["values"]:
                        matched = True
                        row_hits += 1
                        fanout[(source_table, target_table)] += 1
                        if source_table == target_table:
                            # Approximate self-loop risk when the source value equals its own entity key.
                            for own_key in ("DM", "GUID", "gid"):
                                if _clean(row.get(own_key)) == value:
                                    self_loop_count += 1
                                    break
                        if len(sample_hits) < max_examples:
                            sample_hits.append({
                                "source_table": source_table,
                                "source_value": value,
                                "target_table": target_table,
                                "target_key": target_key,
                            })
                        break
                if matched:
                    total_hits += 1
                    source_hits += 1
                elif len(sample_misses) < max_examples:
                    sample_misses.append({
                        "source_table": source_table,
                        "source_value": value,
                    })
            total_rows += 1
            if values:
                non_empty_rows += 1

        per_source.append({
            "source_table": source_table,
            "sampled_rows": len(rows),
            "non_empty_rows": source_non_empty_rows,
            "source_values": source_values,
            "hits": source_hits,
            "hit_rate": round(source_hits / source_values, 4) if source_values else 0.0,
            "source_non_empty_rate": round(source_non_empty_rows / len(rows), 4) if rows else 0.0,
        })

    target_summaries = {
        f"{table}.{key}": {
            "row_count": data["row_count"],
            "non_empty_count": data["non_empty_count"],
            "distinct_count": data["distinct_count"],
            "duplicate_values": data["duplicate_values"],
            "unique_rate": data["unique_rate"],
        }
        for (table, key), data in target_index_cache.items()
    }
    return {
        "status": "probed",
        "match_mode": match_mode,
        "sample_size": int(sample_size),
        "sampled_rows": total_rows,
        "non_empty_rows": non_empty_rows,
        "source_non_empty_rate": round(non_empty_rows / total_rows, 4) if total_rows else 0.0,
        "source_values": total_values,
        "hits": total_hits,
        "hit_rate": round(total_hits / total_values, 4) if total_values else 0.0,
        "self_loop_count": self_loop_count,
        "fanout_by_pair": {f"{src}->{dst}": count for (src, dst), count in fanout.most_common()},
        "target_indexes": target_summaries,
        "per_source": per_source,
        "sample_hits": sample_hits,
        "sample_misses": sample_misses,
    }


def probe_rules(loader, rules, sample_size=10000):
    return [probe_rule(loader, rule, sample_size=sample_size) for rule in rules]


def discover_deterministic_candidates(schema_profile):
    """Generate cheap deterministic candidate rules before asking the LLM."""
    candidates = []
    tables = schema_profile.get("tables", [])
    table_names = schema_profile.get("table_names", [])
    type_hints = schema_profile.get("entity_type_hints", {})
    columns_by_table = {t["name"]: [c["name"] for c in t.get("columns", [])] for t in tables}

    def tables_by_type(*types):
        allowed = set(types)
        return [table for table in table_names if type_hints.get(table) in allowed]

    police_tables = tables_by_type("PoliceOrg")
    admin_tables = tables_by_type("City", "Town", "Community")
    street_tables = tables_by_type("Street")
    address_tables = tables_by_type("Doorplate", "Yard", "Building", "Unit", "Room")
    poi_tables = tables_by_type("POI", "AOI")
    road_cross_tables = tables_by_type("RoadCross")

    address_level_targets = {
        "B-DZ-006": street_tables,
        "B-DZ-007": tables_by_type("Doorplate"),
        "B-DZ-008": tables_by_type("Yard"),
        "B-DZ-009": tables_by_type("Building"),
        "B-DZ-010": tables_by_type("Unit"),
        "B-DZ-011": tables_by_type("Room"),
    }
    address_level_targets = {level: targets for level, targets in address_level_targets.items() if targets}

    for table in tables:
        source_table = table["name"]
        source_cols = columns_by_table.get(source_table, [])
        source_type = type_hints.get(source_table)
        for col in source_cols:
            upper = col.upper()
            if upper in {"GADM", "ZDGADM"} and police_tables:
                    candidates.append({
                        "name": "managedByPoliceOrg",
                        "source_tables": [source_table],
                        "source_key_candidates": [col],
                        "target_tables": police_tables,
                        "target_key": "DM",
                        "match_mode": "exact",
                        "edge_confidence": "strong",
                        "agent_reason": f"{source_table}.{col} looks like a police organization code.",
                    })
            if upper == "S_DM" and source_type == "PoliceOrg" and police_tables:
                    candidates.append({
                        "name": "parentPoliceOrg",
                        "source_tables": [source_table],
                        "source_key": col,
                        "source_label_field": "S_MC" if "S_MC" in source_cols else None,
                        "target_tables": [t for t in police_tables if t != source_table] or police_tables,
                        "target_key": "DM",
                        "match_mode": "exact",
                        "edge_confidence": "strong_if_hit_rate_high",
                        "agent_reason": f"{source_table}.{col} looks like a parent police organization code.",
                    })
            if upper == "S_DM" and source_type in {"Town", "Community", "Street"} and admin_tables:
                    candidates.append({
                        "name": "parentAdminArea",
                        "source_tables": [source_table],
                        "source_key": col,
                        "source_label_field": "S_MC" if "S_MC" in source_cols else None,
                        "target_tables": admin_tables,
                        "target_key": "DM",
                        "match_mode": "exact",
                        "edge_confidence": "strong_if_hit_rate_high",
                        "agent_reason": f"{source_table}.{col} looks like an upper administrative area code.",
                    })
            if upper == "S_DM" and source_type in {"Doorplate", "Yard", "Building", "Unit", "Room"} and address_level_targets:
                    candidates.append({
                        "name": "partOfAddressEntity",
                        "source_tables": [source_table],
                        "source_key_priority": [col],
                        "source_label_field": "S_MC" if "S_MC" in source_cols else None,
                        "source_level_field": "S_FLDM" if "S_FLDM" in source_cols else None,
                        "target_tables_by_level": address_level_targets,
                        "target_key_priority": ["DM"],
                        "match_mode": "exact",
                        "edge_confidence": "strong_if_hit_rate_high",
                        "agent_reason": f"{source_table}.{col} plus S_FLDM can resolve address hierarchy parents.",
                    })
            if upper in {"S_DM", "XZQHDM", "XZCDM"} and source_type in {"Doorplate", "RoadCross", "Facility"}:
                    target_tables = admin_tables + street_tables
                    if target_tables:
                        candidates.append({
                            "name": "locatedInAdminArea",
                            "source_tables": [source_table],
                            "source_key_candidates": [col],
                            "source_label_candidates": [label for label in ["S_MC", "XZQHMC", "XZCMC"] if label in source_cols],
                            "target_tables": target_tables,
                            "target_key": "DM",
                            "match_mode": "exact",
                            "edge_confidence": "strong_if_hit_rate_high",
                            "agent_reason": f"{source_table}.{col} looks like an administrative/location code.",
                        })
            if upper == "SZDLDM" and street_tables:
                    candidates.append({
                        "name": "locatedOnStreet",
                        "source_tables": [source_table],
                        "source_key": col,
                        "target_tables": street_tables,
                        "target_key": "DM",
                        "match_mode": "split_exact",
                        "split_delimiter": ",",
                        "edge_confidence": "strong_if_hit_rate_high",
                        "agent_reason": f"{source_table}.{col} looks like comma-separated street codes.",
                    })
        if source_table in poi_tables:
            source_keys = [col for col in ["BZDZMC", "DZ", "BZ", "MC"] if col in source_cols]
            target_tables = address_tables
            if source_keys and target_tables:
                candidates.append({
                    "name": "possibleStandardAddressTextMatch",
                    "source_tables": [source_table],
                    "source_key_priority": source_keys,
                    "target_tables": target_tables,
                    "target_name_field_priority": ["MPQC", "YLQC", "DHQC", "DYHQC", "FJHQC", "DHBM", "FJHBM"],
                    "match_mode": "text_candidate",
                    "edge_confidence": "weak",
                    "enabled": False,
                    "max_candidates": 5,
                    "agent_reason": f"{source_table} has address text fields that may weakly match standard address entities.",
                })
    return candidates
