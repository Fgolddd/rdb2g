import os
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from dotenv import load_dotenv

from rdb2g.common.env import env_float, env_int
from rdb2g.common.paths import build_output_path
from rdb2g.common.progress import ProgressBar, format_elapsed, progress_iter
from rdb2g.data.sqlite_loader import SpiderDataLoader
from rdb2g.graph.rdf_builder import RDFGraphBuilder
from rdb2g.graph.relation_rules import RelationRuleSet
from rdb2g.mapping.agent_system import MultiAgentSystem
from rdb2g.pipeline.column_selection import select_columns_for_table
from rdb2g.pipeline.mapping_cache import (
    fingerprint_digest,
    load_cached_mapping_results,
    mapping_cache_file,
    save_mapping_cache,
)
from rdb2g.pipeline.relation_index import build_relation_entity_index
from rdb2g.retrieval.store_factory import init_vector_store


load_dotenv()


def choose_mapping_workers(pending_count, retrieval_enabled=False):
    if pending_count <= 1:
        return 1
    override = env_int("MAPPING_WORKERS")
    if override is not None:
        return min(max(1, override), pending_count)
    if retrieval_enabled:
        return min(2, pending_count)
    cpu = os.cpu_count() or 4
    return min(6, max(2, cpu // 2), pending_count)


def run_table_mapping(table, fingerprint, kg_store, allow_public_uri=False):
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


def run_pending_mappings(pending_tables, fingerprints, kg_store, cache_dir, allow_public_uri=False):
    mapping_results = {}
    retrieval_enabled = bool(getattr(kg_store, "vector_db", None) is not None)
    mapping_workers = choose_mapping_workers(len(pending_tables), retrieval_enabled=retrieval_enabled)
    worker_source = "环境变量 MAPPING_WORKERS" if os.getenv("MAPPING_WORKERS") else "自动"
    print(
        f"\n映射加速策略：{worker_source} worker={mapping_workers}，"
        f"待映射表数={len(pending_tables)}，缓存目录={cache_dir}"
    )

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
            digest = fingerprint_digest(fingerprint)
            save_mapping_cache(mapping_cache_file(cache_dir, table), digest, relations, final_mapping)
            mapping_results[table] = {
                "fingerprint": fingerprint,
                "relations": relations,
                "final_mapping": final_mapping,
            }
            mapping_progress.update(detail=f"{table} {format_elapsed(time.perf_counter() - table_started)}", force=True)
        mapping_progress.close()
        return mapping_results

    mapping_started = time.perf_counter()
    mapping_progress = ProgressBar(total=len(pending_tables), label="多智能体映射", unit="表")
    with ThreadPoolExecutor(max_workers=mapping_workers) as executor:
        future_map = {
            executor.submit(run_table_mapping, table, fingerprints[table], kg_store, allow_public_uri): table
            for table in pending_tables
        }
        pending_futures = set(future_map)
        heartbeat_s = max(env_float("MAPPING_HEARTBEAT_SECONDS", 15.0), 1.0)
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
                print(
                    f"\n⏳ 映射仍在运行：已完成 {len(future_map) - len(pending_futures)}/{len(future_map)}，"
                    f"elapsed={elapsed}，未完成={shown}"
                )
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
                digest = fingerprint_digest(result["fingerprint"])
                save_mapping_cache(mapping_cache_file(cache_dir, table), digest, result["relations"], result["final_mapping"])
                mapping_results[table] = {
                    "fingerprint": result["fingerprint"],
                    "relations": result["relations"],
                    "final_mapping": result["final_mapping"],
                }
                mapping_progress.update(detail=table, force=True)
    mapping_progress.close(detail=f"总耗时 {format_elapsed(time.perf_counter() - mapping_started)}")
    return mapping_results


def build_ttl(db_path, kb_file=None, schema_file=None, allow_public_uri=False, relation_rules_file=None):
    print("=== Step 1: 初始化系统 ===")
    try:
        kg_store = init_vector_store(kb_file=kb_file, schema_file=schema_file)
    except FileNotFoundError as e:
        print(str(e))
        return

    try:
        loader = SpiderDataLoader(db_path)
    except FileNotFoundError:
        print(f"⚠️ 未找到数据库文件: {db_path}，跳过执行。")
        return

    relation_rules = RelationRuleSet.load(relation_rules_file) if relation_rules_file else RelationRuleSet({})
    if relation_rules.enabled():
        print(f"关系规则: version={relation_rules.version} file={relation_rules_file}")

    tables = loader.get_all_table_names()
    print(f"发现表: {tables}")

    entity_index = build_relation_entity_index(loader, tables, relation_rules)
    graph_builder = RDFGraphBuilder(kb_file=kb_file, relation_rules=relation_rules, entity_index=entity_index)

    print("\n=== Step 2: 多智能体协同映射 ===")
    fingerprints = {}
    for table in progress_iter(tables, total=len(tables), label="表指纹", unit="表"):
        print(f"\n>>> 准备表指纹: {table}")
        fingerprints[table] = loader.generate_table_fingerprint(table)

    cache_progress = ProgressBar(total=len(tables), label="映射缓存检查", unit="表")
    cache_dir, mapping_results, pending_tables = load_cached_mapping_results(db_path, tables, fingerprints, cache_progress)
    cache_progress.close(detail=f"缓存命中 {len(mapping_results)} 表，待映射 {len(pending_tables)} 表")
    mapping_results.update(run_pending_mappings(pending_tables, fingerprints, kg_store, cache_dir, allow_public_uri))

    print("\n=== Step 2.5: 组装三元组 ===")
    output_path = build_output_path(db_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)
    build_progress = ProgressBar(total=len(tables), label="组装三元组", unit="表")
    chunk_size = max(env_int("RDF_BUILD_CHUNK_SIZE", 5000) or 5000, 1)
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

        selected_columns = select_columns_for_table(
            loader,
            table,
            fingerprint,
            relations,
            final_mapping,
            relation_rules=relation_rules,
        )
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
