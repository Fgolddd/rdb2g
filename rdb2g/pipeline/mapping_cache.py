import hashlib
import json
import os

from rdb2g.common.paths import build_mapping_cache_dir, safe_name


MAPPING_CACHE_VERSION = "zhongshan-domain-strict-v1"


def fingerprint_digest(fingerprint):
    slim = {
        "table_name": fingerprint.get("table_name"),
        "row_count": fingerprint.get("row_count"),
        "columns": [
            {
                "name": c.get("name"),
                "dtype": c.get("dtype"),
                "unique_count": c.get("unique_count"),
                "null_ratio": c.get("null_ratio"),
            }
            for c in fingerprint.get("columns", [])
            if isinstance(c, dict)
        ],
        "explicit_pk": fingerprint.get("explicit_pk", []),
        "explicit_fks": fingerprint.get("explicit_fks", []),
        "explicit_fk_details": fingerprint.get("explicit_fk_details", []),
    }
    payload = json.dumps(slim, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def mapping_cache_file(cache_dir, table):
    return os.path.join(cache_dir, f"{safe_name(table)}.json")


def load_mapping_cache(cache_file):
    if not os.path.exists(cache_file):
        return None
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_mapping_cache(cache_file, digest, relations, final_mapping):
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    payload = {
        "mapping_version": MAPPING_CACHE_VERSION,
        "fingerprint_digest": digest,
        "relations": relations,
        "final_mapping": final_mapping,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def load_cached_mapping_results(db_path, tables, fingerprints, progress):
    mapping_results = {}
    pending_tables = []
    cache_dir = build_mapping_cache_dir(db_path)
    os.makedirs(cache_dir, exist_ok=True)

    for table in tables:
        fingerprint = fingerprints[table]
        digest = fingerprint_digest(fingerprint)
        cache_file = mapping_cache_file(cache_dir, table)
        cached = load_mapping_cache(cache_file)
        if (
            isinstance(cached, dict)
            and cached.get("mapping_version") == MAPPING_CACHE_VERSION
            and cached.get("fingerprint_digest") == digest
            and isinstance(cached.get("relations"), dict)
            and isinstance(cached.get("final_mapping"), dict)
        ):
            print(f"\n>>> 命中映射缓存: {table}")
            mapping_results[table] = {
                "fingerprint": fingerprint,
                "relations": cached["relations"],
                "final_mapping": cached["final_mapping"],
            }
        else:
            pending_tables.append(table)
        progress.update(detail=table)
    return cache_dir, mapping_results, pending_tables
