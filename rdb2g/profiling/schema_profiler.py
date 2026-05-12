from collections import Counter

from rdb2g.common.ignored_columns import is_ignored_rag_column


def _clean_value(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nan"}:
        return ""
    return text


def _column_profile(loader, table, column_name, row_count, sample_size, sample_limit=8):
    quoted = loader._quote_identifier(column_name)
    table_q = f"`{table}`"
    cursor = loader.conn.cursor()
    cursor.execute(f"SELECT {quoted} FROM {table_q} LIMIT {max(int(sample_size), 1)}")
    values = [_clean_value(row[0]) for row in cursor.fetchall()]
    non_empty = [v for v in values if v]
    sample_values = []
    seen_samples = set()
    for value in non_empty:
        shown = value if len(value) <= 180 else value[:180] + "..."
        if shown in seen_samples:
            continue
        seen_samples.add(shown)
        sample_values.append(shown)
        if len(sample_values) >= sample_limit:
            break

    distinct_values = set(non_empty)
    comma_count = sum(1 for value in non_empty if "," in value)
    digit_like_count = sum(1 for value in non_empty if value.replace(",", "").replace("-", "").isdigit())
    upper_name = str(column_name).upper()
    return {
        "name": column_name,
        "sampled_count": len(values),
        "non_empty_count": len(non_empty),
        "non_null_rate": round(len(non_empty) / len(values), 4) if values else 0.0,
        "distinct_sample_count": len(distinct_values),
        "sample_unique_rate": round(len(distinct_values) / len(non_empty), 4) if non_empty else 0.0,
        "samples": sample_values,
        "is_split_candidate": bool(non_empty) and comma_count / len(non_empty) >= 0.5,
        "digit_like_rate": round(digit_like_count / len(non_empty), 4) if non_empty else 0.0,
        "name_hints": {
            "is_code": upper_name in {"DM", "GADM", "ZDGADM", "S_DM", "XZQHDM", "XZCDM", "SZDLDM"} or upper_name.endswith("DM"),
            "is_label": upper_name in {"MC", "MPQC", "DZ", "LKMC", "SSMC", "GAMC", "S_MC", "SZDLMC"} or upper_name.endswith("MC"),
            "is_parent": upper_name.startswith("S_") or upper_name in {"S_DM", "S_GUID"},
        },
        "row_count": row_count,
    }


def build_schema_profile(loader, kb_terms=None, sample_size=10000):
    """Build a compact full-database profile for relation-rule discovery."""
    kb_by_table_column = {}
    for term in kb_terms or []:
        if not isinstance(term, dict):
            continue
        domain = str(term.get("domain", "")).strip()
        uri = str(term.get("uri", "")).strip()
        if not domain or "." not in uri:
            continue
        column = uri.rsplit(".", 1)[-1]
        kb_by_table_column[(domain, column)] = {
            "label": str(term.get("label", "")).strip(),
            "comment": str(term.get("comment", "")).strip(),
            "role": str(term.get("role", "")).strip(),
            "priority": str(term.get("priority", "")).strip(),
        }

    tables = loader.get_all_table_names()
    table_profiles = []
    entity_type_hints = {}
    for table in tables:
        row_count = int(loader._fetch_scalar(f"SELECT COUNT(*) FROM `{table}`") or 0)
        constraints = loader.get_table_constraints(table)
        columns = []
        for col_info in loader.get_table_columns(table):
            column_name = col_info[1]
            if is_ignored_rag_column(column_name):
                continue
            profile = _column_profile(loader, table, column_name, row_count, sample_size)
            profile["dtype"] = str(col_info[2] or "TEXT")
            profile["kb"] = kb_by_table_column.get((table, column_name), {})
            columns.append(profile)

        table_profiles.append({
            "name": table,
            "row_count": row_count,
            "explicit_pk": constraints["explicit_pk"],
            "explicit_fks": constraints["explicit_fks"],
            "explicit_fk_details": constraints["explicit_fk_details"],
            "columns": columns,
        })
        entity_type_hints[table] = infer_entity_type(table, columns)

    return {
        "tables": table_profiles,
        "table_names": tables,
        "entity_type_hints": entity_type_hints,
        "sample_size": int(sample_size),
    }


def infer_entity_type(table, columns):
    table_lower = str(table or "").lower()
    text = " ".join(f"{col.get('name','')} {col.get('kb',{}).get('label','')}" for col in columns)
    if any(token in table_lower for token in ("gaj", "gafj", "pcs", "jws")):
        return "PoliceOrg"
    if "roadcross" in table_lower:
        return "RoadCross"
    if "street" in table_lower:
        return "Street"
    if "community" in table_lower:
        return "Community"
    if "town" in table_lower:
        return "Town"
    if "city" in table_lower:
        return "City"
    if "poi" in table_lower:
        return "POI"
    if "aoi" in table_lower:
        return "AOI"
    if "facility" in table_lower:
        return "Facility"
    if "zs_mp" in table_lower:
        return "Doorplate"
    if "zs_yl" in table_lower:
        return "Yard"
    if "zs_dh" in table_lower:
        return "Building"
    if "zs_dy" in table_lower:
        return "Unit"
    if "zs_fj" in table_lower:
        return "Room"
    if "设施" in text:
        return "Facility"
    return "Entity"


def compact_profile_for_agent(schema_profile, max_columns_per_table=18, sample_limit=4):
    tables = []
    for table in schema_profile.get("tables", []):
        columns = []
        for col in table.get("columns", [])[:max_columns_per_table]:
            columns.append({
                "name": col.get("name"),
                "dtype": col.get("dtype"),
                "non_null_rate": col.get("non_null_rate"),
                "sample_unique_rate": col.get("sample_unique_rate"),
                "is_split_candidate": col.get("is_split_candidate"),
                "name_hints": col.get("name_hints"),
                "samples": (col.get("samples") or [])[:sample_limit],
                "kb": col.get("kb") or {},
            })
        tables.append({
            "name": table.get("name"),
            "row_count": table.get("row_count"),
            "entity_type_hint": schema_profile.get("entity_type_hints", {}).get(table.get("name")),
            "explicit_pk": table.get("explicit_pk", []),
            "columns": columns,
        })
    return {
        "tables": tables,
        "table_names": schema_profile.get("table_names", []),
        "sample_size": schema_profile.get("sample_size"),
    }
