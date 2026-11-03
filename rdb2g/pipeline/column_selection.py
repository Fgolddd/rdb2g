from rdb2g.common.ignored_columns import is_ignored_rdf_property


def relation_rule_columns_for_table(table, relation_rules):
    if not relation_rules or not relation_rules.enabled():
        return []
    columns = []

    def add(name):
        name = str(name or "").strip()
        if name and name not in columns:
            columns.append(name)

    for col in relation_rules.entity_key_fields(table):
        add(col)
    for col in relation_rules.name_fields(table):
        add(col)
    for rule in relation_rules.relation_rules:
        if rule.get("enabled") is False:
            continue
        if table not in (rule.get("source_tables") or []):
            continue
        for col in relation_rules.source_key_candidates(rule):
            add(col)
        for col in relation_rules.source_label_candidates(rule):
            add(col)
        add(rule.get("source_level_field"))
    return columns


def select_columns_for_table(loader, table, fingerprint, relations, final_mapping, relation_rules=None):
    available_columns = {row[1] for row in loader.get_table_columns(table)}
    selected = []

    def add_column(name):
        if not name or name not in available_columns or name in selected:
            return
        selected.append(name)

    pk = relations.get("pk")
    if isinstance(pk, list):
        for col in pk:
            add_column(col)
    else:
        add_column(pk)

    for col in relations.get("fks", []):
        add_column(col)

    for col in ("MC", "MPQC", "DZ", "SSMC", "LKMC", "FLMC", "XLMC", "ZLMC", "DLMC", "DM", "gid"):
        add_column(col)

    for col, mapping_value in (final_mapping or {}).items():
        if not mapping_value:
            continue
        if is_ignored_rdf_property(col):
            continue
        add_column(col)

    for col in relation_rule_columns_for_table(table, relation_rules):
        add_column(col)

    if not selected:
        for col in fingerprint.get("explicit_pk", []):
            add_column(col)
    return selected
