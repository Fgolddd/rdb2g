import os
import json
import hashlib
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait
from dotenv import load_dotenv
from dataloader import SpiderDataLoader
from schema_parser import parse_schema_org, parse_private_kb
from vector_store import OntologyVectorStore
from agents import MultiAgentSystem
from graph_builder import RDFGraphBuilder
from progress_utils import ProgressBar, format_elapsed, progress_iter
from ignored_columns import is_ignored_rdf_property
from relation_rules import RelationRuleSet

# 加载环境变量
load_dotenv()

MAPPING_CACHE_VERSION = "zhongshan-domain-strict-v1"

def _build_index_dir(source_file, prefix):
    filename = os.path.splitext(os.path.basename(source_file))[0]
    safe_name = "".join(ch if ch.isalnum() or ch in ('_', '-') else '_' for ch in filename)
    return os.path.join("data", "chroma_db", f"{prefix}_{safe_name}")


def _safe_name(value):
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(value))


def _build_mapping_cache_dir(db_path):
    db_name = _safe_name(os.path.splitext(os.path.basename(db_path))[0])
    return os.path.join("data", "mapping_cache", db_name)


def _fingerprint_digest(fingerprint):
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


def _load_mapping_cache(cache_file):
    if not os.path.exists(cache_file):
        return None
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_mapping_cache(cache_file, fingerprint_digest, relations, final_mapping):
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    payload = {
        "mapping_version": MAPPING_CACHE_VERSION,
        "fingerprint_digest": fingerprint_digest,
        "relations": relations,
        "final_mapping": final_mapping,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def _env_int(name, default=None):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        print(f"⚠️ 忽略无效整数环境变量 {name}={value!r}")
        return default


def _env_float(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ 忽略无效数字环境变量 {name}={value!r}")
        return default


def _choose_mapping_workers(pending_count, retrieval_enabled=False):
    if pending_count <= 1:
        return 1
    override = _env_int("MAPPING_WORKERS")
    if override is not None:
        return min(max(1, override), pending_count)
    if retrieval_enabled:
        return min(2, pending_count)
    cpu = os.cpu_count() or 4
    return min(6, max(2, cpu // 2), pending_count)


def _init_vector_store(kb_file=None, schema_file=None):
    """初始化向量库：私域知识库优先，其次兼容 schema.org，最后可无知识库模式"""
    def _fallback_no_retrieval(reason):
        print(f"⚠️ 向量检索初始化失败，降级为无检索模式：{reason}")
        print("   提示：将 QWEN_EMBEDDING_MODEL 改为你账号可用的 DashScope Embedding 模型后可恢复 RAG。")
        store = OntologyVectorStore(enable_retrieval=False)
        store.create_or_load_index()
        return store

    if kb_file:
        if not os.path.exists(kb_file):
            raise FileNotFoundError(f"⚠️ 未找到私域知识库文件: {kb_file}")
        terms = parse_private_kb(kb_file)
        persist_dir = _build_index_dir(kb_file, "private")
        store = OntologyVectorStore(persist_dir=persist_dir, enable_retrieval=True)
        try:
            store.create_or_load_index(terms)
        except RuntimeError as e:
            if "Embedding 模型不可用" in str(e):
                return _fallback_no_retrieval(e)
            raise
        return store

    if schema_file:
        if not os.path.exists(schema_file):
            raise FileNotFoundError(f"⚠️ 未找到本体文件: {schema_file}")
        terms = parse_schema_org(schema_file)
        persist_dir = _build_index_dir(schema_file, "schema")
        store = OntologyVectorStore(persist_dir=persist_dir, enable_retrieval=True)
        try:
            store.create_or_load_index(terms)
        except RuntimeError as e:
            if "Embedding 模型不可用" in str(e):
                return _fallback_no_retrieval(e)
            raise
        return store

    # 无知识库：完全依赖模型能力
    store = OntologyVectorStore(enable_retrieval=False)
    store.create_or_load_index()
    return store


def _run_table_mapping(table, fingerprint, kg_store, allow_public_uri=False):
    agent_system = MultiAgentSystem(kg_store, allow_public_uri=allow_public_uri)
    started = time.perf_counter()
    print(f"\n>>> [{table}] Mapping Agent 开始")
    stage_started = time.perf_counter()
    raw_mapping = agent_system.run_mapping_agent(fingerprint)
    print(f">>> [{table}] Mapping Agent 完成，耗时 {format_elapsed(time.perf_counter() - stage_started)}")
    print(f">>> [{table}] Relation Agent 开始")
    stage_started = time.perf_counter()
    relations = agent_system.run_relation_agent(fingerprint)
    print(f">>> [{table}] Relation Agent 完成，耗时 {format_elapsed(time.perf_counter() - stage_started)}")
    print(f">>> [{table}] Validator Agent 开始")
    stage_started = time.perf_counter()
    final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
    print(f">>> [{table}] Validator Agent 完成，耗时 {format_elapsed(time.perf_counter() - stage_started)}")
    elapsed = time.perf_counter() - started
    return {
        "table": table,
        "fingerprint": fingerprint,
        "raw_mapping": raw_mapping,
        "relations": relations,
        "final_mapping": final_mapping,
        "elapsed": elapsed,
    }


def _build_output_path(db_path):
    db_filename = os.path.basename(db_path)
    ttl_filename = os.path.splitext(db_filename)[0] + ".ttl"
    return os.path.join("data", "ttl", ttl_filename)


def _relation_rule_columns_for_table(table, relation_rules):
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


def _select_columns_for_table(loader, table, fingerprint, relations, final_mapping, relation_rules=None):
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

    for col in _relation_rule_columns_for_table(table, relation_rules):
        add_column(col)

    if not selected:
        for col in fingerprint.get("explicit_pk", []):
            add_column(col)
    return selected


def _build_relation_entity_index(loader, tables, relation_rules):
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

        for df in loader.get_dataframe(table, columns=selected, chunksize=max(_env_int("RELATION_INDEX_CHUNK_SIZE", 10000) or 10000, 1)):
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


def main(db_path, kb_file=None, schema_file=None, allow_public_uri=False, relation_rules_file=None):
    # 配置路径现在通过函数参数传入
    DB_PATH = db_path
    
    print("=== Step 1: 初始化系统 ===")
    # 1. 准备向量库（私域知识库 > schema.org > 无知识库）
    try:
        kg_store = _init_vector_store(kb_file=kb_file, schema_file=schema_file)
    except FileNotFoundError as e:
        print(str(e))
        return

    # 2. 初始化数据加载器
    try:
        loader = SpiderDataLoader(DB_PATH)
    except FileNotFoundError:
        print(f"⚠️ 未找到数据库文件: {DB_PATH}，跳过执行。")
        return

    relation_rules = RelationRuleSet.load(relation_rules_file) if relation_rules_file else RelationRuleSet({})
    if relation_rules.enabled():
        print(f"关系规则: version={relation_rules.version} file={relation_rules_file}")

    # 获取所有表
    tables = loader.get_all_table_names()
    print(f"发现表: {tables}")

    entity_index = _build_relation_entity_index(loader, tables, relation_rules)
    graph_builder = RDFGraphBuilder(kb_file=kb_file, relation_rules=relation_rules, entity_index=entity_index)

    print("\n=== Step 2: 多智能体协同映射 ===")
    fingerprints = {}
    for table in progress_iter(tables, total=len(tables), label="表指纹", unit="表"):
        print(f"\n>>> 准备表指纹: {table}")
        fingerprints[table] = loader.generate_table_fingerprint(table)

    mapping_results = {}
    pending_tables = []
    mapping_cache_dir = _build_mapping_cache_dir(DB_PATH)
    os.makedirs(mapping_cache_dir, exist_ok=True)

    cache_progress = ProgressBar(total=len(tables), label="映射缓存检查", unit="表")
    for table in tables:
        fingerprint = fingerprints[table]
        fp_digest = _fingerprint_digest(fingerprint)
        cache_file = os.path.join(mapping_cache_dir, f"{_safe_name(table)}.json")
        cached = _load_mapping_cache(cache_file)
        if (
            isinstance(cached, dict)
            and cached.get("mapping_version") == MAPPING_CACHE_VERSION
            and cached.get("fingerprint_digest") == fp_digest
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
        cache_progress.update(detail=table)
    cache_progress.close(detail=f"缓存命中 {len(mapping_results)} 表，待映射 {len(pending_tables)} 表")

    retrieval_enabled = bool(getattr(kg_store, "vector_db", None) is not None)
    mapping_workers = _choose_mapping_workers(len(pending_tables), retrieval_enabled=retrieval_enabled)
    worker_source = "环境变量 MAPPING_WORKERS" if os.getenv("MAPPING_WORKERS") else "自动"
    print(f"\n映射加速策略：{worker_source} worker={mapping_workers}，待映射表数={len(pending_tables)}，缓存目录={mapping_cache_dir}")

    if mapping_workers == 1:
        agent_system = MultiAgentSystem(kg_store, allow_public_uri=allow_public_uri)
        mapping_progress = ProgressBar(total=len(pending_tables), label="多智能体映射", unit="表")
        for table in pending_tables:
            table_started = time.perf_counter()
            print(f"\n>>> 处理表: {table}")
            fingerprint = fingerprints[table]
            raw_mapping = agent_system.run_mapping_agent(fingerprint)
            print(f"   初次映射: {raw_mapping}")
            relations = agent_system.run_relation_agent(fingerprint)
            print(f"   识别关系: {relations}")
            final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
            print(f"   最终映射: {final_mapping}")
            fp_digest = _fingerprint_digest(fingerprint)
            cache_file = os.path.join(mapping_cache_dir, f"{_safe_name(table)}.json")
            _save_mapping_cache(cache_file, fp_digest, relations, final_mapping)
            mapping_results[table] = {
                "fingerprint": fingerprint,
                "relations": relations,
                "final_mapping": final_mapping,
            }
            mapping_progress.update(detail=f"{table} {format_elapsed(time.perf_counter() - table_started)}", force=True)
        mapping_progress.close()
    else:
        mapping_started = time.perf_counter()
        mapping_progress = ProgressBar(total=len(pending_tables), label="多智能体映射", unit="表")
        with ThreadPoolExecutor(max_workers=mapping_workers) as executor:
            future_map = {
                executor.submit(_run_table_mapping, table, fingerprints[table], kg_store, allow_public_uri): table
                for table in pending_tables
            }
            pending_futures = set(future_map)
            heartbeat_s = max(_env_float("MAPPING_HEARTBEAT_SECONDS", 15.0), 1.0)
            while pending_futures:
                done, pending_futures = wait(
                    pending_futures,
                    timeout=heartbeat_s,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    waiting_tables = [future_map[f] for f in pending_futures]
                    shown = ", ".join(waiting_tables[:8])
                    if len(waiting_tables) > 8:
                        shown += f", ...(+{len(waiting_tables) - 8})"
                    elapsed = format_elapsed(time.perf_counter() - mapping_started)
                    print(f"\n⏳ 映射仍在运行：已完成 {len(future_map) - len(pending_futures)}/{len(future_map)}，elapsed={elapsed}，未完成={shown}")
                    continue
                for future in done:
                    table = future_map[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        raise RuntimeError(f"表 '{table}' 并行映射失败: {e}") from e
                    print(f"\n>>> 完成映射: {table}，耗时 {format_elapsed(result.get('elapsed', 0))}")
                    print(f"   初次映射: {result['raw_mapping']}")
                    print(f"   识别关系: {result['relations']}")
                    print(f"   最终映射: {result['final_mapping']}")
                    fp_digest = _fingerprint_digest(result["fingerprint"])
                    cache_file = os.path.join(mapping_cache_dir, f"{_safe_name(table)}.json")
                    _save_mapping_cache(cache_file, fp_digest, result["relations"], result["final_mapping"])
                    mapping_results[table] = {
                        "fingerprint": result["fingerprint"],
                        "relations": result["relations"],
                        "final_mapping": result["final_mapping"],
                    }
                    mapping_progress.update(detail=table, force=True)
        mapping_progress.close(detail=f"总耗时 {format_elapsed(time.perf_counter() - mapping_started)}")

    print("\n=== Step 2.5: 组装三元组 ===")
    output_path = _build_output_path(DB_PATH)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)
    build_progress = ProgressBar(total=len(tables), label="组装三元组", unit="表")
    chunk_size = max(_env_int("RDF_BUILD_CHUNK_SIZE", 5000) or 5000, 1)
    wrote_output = False
    for table in tables:
        table_started = time.perf_counter()
        print(f"\n>>> 写入表数据: {table}")
        result = mapping_results[table]
        fingerprint = result["fingerprint"]
        relations = result["relations"]
        final_mapping = result["final_mapping"]

        pk = relations.get("pk")
        fks = relations.get("fks", [])
        fk_ref_map = {}
        for fk_item in fingerprint.get("explicit_fk_details", []):
            fk_col = fk_item.get("column")
            ref_table = fk_item.get("ref_table")
            if fk_col and ref_table:
                fk_ref_map[fk_col] = ref_table

        selected_columns = _select_columns_for_table(loader, table, fingerprint, relations, final_mapping, relation_rules=relation_rules)
        chunk_frames = loader.get_dataframe(table, columns=selected_columns, chunksize=chunk_size)
        chunk_count = 0
        for df in chunk_frames:
            chunk_count += 1
            graph_builder.add_table_data(
                df,
                table,
                final_mapping,
                primary_key=pk,
                foreign_keys=fks,
                foreign_key_refs=fk_ref_map,
            )
            graph_builder.save_graph(output_path, append=wrote_output, reset=True)
            wrote_output = True

        build_progress.update(detail=f"{table} chunks={chunk_count} {format_elapsed(time.perf_counter() - table_started)}", force=True)
    build_progress.close()

    loader.close()

    print("\n=== Step 3: 导出知识图谱 ===")
    export_started = time.perf_counter()
    print(f"开始导出 TTL: {output_path}")
    if not wrote_output:
        graph_builder.save_graph(output_path, append=False, reset=True)
    print(f"TTL 导出完成，耗时 {format_elapsed(time.perf_counter() - export_started)}")

if __name__ == "__main__":
    # --- 设置命令行参数解析 ---
    parser = argparse.ArgumentParser(description="Generate a Knowledge Graph from a SQLite database with optional knowledge retrieval.")
    parser.add_argument("db_path", type=str, help="Path to the input SQLite database file.")
    parser.add_argument("--kb-file", type=str, default=None, help="Path to the private knowledge base JSON file.")
    parser.add_argument("--schema-file", type=str, default=None, help="Optional path to a Schema.org JSON-LD file (backward compatibility).")
    parser.add_argument("--allow-public-uri", action="store_true", help="Allow public ontology URIs in no-knowledge mode.")
    parser.add_argument("--relation-rules", type=str, default=None, help="Optional relation rules JSON for KG path edges.")
    args = parser.parse_args()

    # 使用从命令行解析的参数调用 main 函数
    main(
        args.db_path,
        kb_file=args.kb_file,
        schema_file=args.schema_file,
        allow_public_uri=args.allow_public_uri,
        relation_rules_file=args.relation_rules,
    )
