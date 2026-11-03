from rdb2g.common.env import env_int
from rdb2g.common.progress import ProgressBar


def build_relation_entity_index(loader, tables, relation_rules):
    if not relation_rules or not relation_rules.enabled():
        return {}

    specs_by_table = {}
    for table, key in relation_rules.target_index_specs():
        specs_by_table.setdefault(table, set()).add(key)

    entity_index = {}
    progress = ProgressBar(total=len(specs_by_table), label="关系实体索引", unit="表")
    for table in tables:
        keys = specs_by_table.get(table)
        if not keys:
            continue
        available = {row[1] for row in loader.get_table_columns(table)}
        selected = []
        for col in relation_rules.entity_key_fields(table) + sorted(keys):
            if col in available and col not in selected:
                selected.append(col)
        if not selected:
            continue

        chunk_size = max(env_int("RELATION_INDEX_CHUNK_SIZE", 10000) or 10000, 1)
        for df in loader.get_dataframe(table, columns=selected, chunksize=chunk_size):
            for row_index, row in df.iterrows():
                row_dict = row.to_dict()
                entity_id = relation_rules.entity_id_for_row(table, row_dict, fallback=f"row_{row_index}")
                entity_uri = relation_rules.uri_for_entity(table, entity_id)
                for key in keys:
                    value = row_dict.get(key)
                    if value is None:
                        continue
                    value_text = str(value).strip()
                    if not value_text or value_text.lower() in {"null", "none", "nan"}:
                        continue
                    entity_index[(table, key, value_text)] = entity_uri
        progress.update(detail=f"{table} keys={','.join(sorted(keys))}", force=True)
    progress.close(detail=f"索引项 {len(entity_index)}")
    return entity_index
