import os


def safe_name(value):
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(value))


def build_index_dir(source_file, prefix):
    filename = os.path.splitext(os.path.basename(source_file))[0]
    return os.path.join("data", "chroma_db", f"{prefix}_{safe_name(filename)}")


def build_output_path(db_path):
    db_filename = os.path.basename(db_path)
    ttl_filename = os.path.splitext(db_filename)[0] + ".ttl"
    return os.path.join("data", "ttl", ttl_filename)


def build_mapping_cache_dir(db_path):
    db_name = safe_name(os.path.splitext(os.path.basename(db_path))[0])
    return os.path.join("data", "mapping_cache", db_name)
