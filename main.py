import os
import json
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from dataloader import SpiderDataLoader
from schema_parser import parse_schema_org, parse_private_kb
from vector_store import OntologyVectorStore
from agents import MultiAgentSystem
from graph_builder import RDFGraphBuilder

# 加载环境变量
load_dotenv()

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
        "fingerprint_digest": fingerprint_digest,
        "relations": relations,
        "final_mapping": final_mapping,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def _choose_mapping_workers(pending_count):
    if pending_count <= 1:
        return 1
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
    raw_mapping = agent_system.run_mapping_agent(fingerprint)
    relations = agent_system.run_relation_agent(fingerprint)
    final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
    return {
        "table": table,
        "fingerprint": fingerprint,
        "raw_mapping": raw_mapping,
        "relations": relations,
        "final_mapping": final_mapping,
    }


def main(db_path, kb_file=None, schema_file=None, allow_public_uri=False):
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

    graph_builder = RDFGraphBuilder(kb_file=kb_file)

    # 获取所有表
    tables = loader.get_all_table_names()
    print(f"发现表: {tables}")

    print("\n=== Step 2: 多智能体协同映射 ===")
    fingerprints = {}
    for table in tables:
        print(f"\n>>> 准备表指纹: {table}")
        fingerprints[table] = loader.generate_table_fingerprint(table)

    mapping_results = {}
    pending_tables = []
    mapping_cache_dir = _build_mapping_cache_dir(DB_PATH)
    os.makedirs(mapping_cache_dir, exist_ok=True)

    for table in tables:
        fingerprint = fingerprints[table]
        fp_digest = _fingerprint_digest(fingerprint)
        cache_file = os.path.join(mapping_cache_dir, f"{_safe_name(table)}.json")
        cached = _load_mapping_cache(cache_file)
        if (
            isinstance(cached, dict)
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

    mapping_workers = _choose_mapping_workers(len(pending_tables))
    print(f"\n映射加速策略：自动并行 worker={mapping_workers}，待映射表数={len(pending_tables)}，缓存目录={mapping_cache_dir}")

    if mapping_workers == 1:
        agent_system = MultiAgentSystem(kg_store, allow_public_uri=allow_public_uri)
        for table in pending_tables:
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
    else:
        with ThreadPoolExecutor(max_workers=mapping_workers) as executor:
            future_map = {
                executor.submit(_run_table_mapping, table, fingerprints[table], kg_store, allow_public_uri): table
                for table in pending_tables
            }
            for future in as_completed(future_map):
                table = future_map[future]
                try:
                    result = future.result()
                except Exception as e:
                    raise RuntimeError(f"表 '{table}' 并行映射失败: {e}") from e
                print(f"\n>>> 完成映射: {table}")
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

    print("\n=== Step 2.5: 组装三元组 ===")
    for table in tables:
        print(f"\n>>> 写入表数据: {table}")
        result = mapping_results[table]
        fingerprint = result["fingerprint"]
        relations = result["relations"]
        final_mapping = result["final_mapping"]

        df = loader.get_dataframe(table)
        pk = relations.get("pk")
        fks = relations.get("fks", [])
        fk_ref_map = {}
        for fk_item in fingerprint.get("explicit_fk_details", []):
            fk_col = fk_item.get("column")
            ref_table = fk_item.get("ref_table")
            if fk_col and ref_table:
                fk_ref_map[fk_col] = ref_table

        graph_builder.add_table_data(
            df,
            table,
            final_mapping,
            primary_key=pk,
            foreign_keys=fks,
            foreign_key_refs=fk_ref_map,
        )

    loader.close()

    print("\n=== Step 3: 导出知识图谱 ===")
    db_filename = os.path.basename(DB_PATH)
    ttl_filename = os.path.splitext(db_filename)[0] + ".ttl"
    output_path = os.path.join("data", "ttl", ttl_filename)
    graph_builder.save_graph(output_path)

if __name__ == "__main__":
    # --- 设置命令行参数解析 ---
    parser = argparse.ArgumentParser(description="Generate a Knowledge Graph from a SQLite database with optional knowledge retrieval.")
    parser.add_argument("db_path", type=str, help="Path to the input SQLite database file.")
    parser.add_argument("--kb-file", type=str, default=None, help="Path to the private knowledge base JSON file.")
    parser.add_argument("--schema-file", type=str, default=None, help="Optional path to a Schema.org JSON-LD file (backward compatibility).")
    parser.add_argument("--allow-public-uri", action="store_true", help="Allow public ontology URIs in no-knowledge mode.")
    args = parser.parse_args()

    # 使用从命令行解析的参数调用 main 函数
    main(
        args.db_path,
        kb_file=args.kb_file,
        schema_file=args.schema_file,
        allow_public_uri=args.allow_public_uri,
    )
