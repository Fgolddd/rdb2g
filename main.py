import os
import argparse
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


def _init_vector_store(kb_file=None, schema_file=None):
    """初始化向量库：私域知识库优先，其次兼容 schema.org，最后可无知识库模式"""
    if kb_file:
        if not os.path.exists(kb_file):
            raise FileNotFoundError(f"⚠️ 未找到私域知识库文件: {kb_file}")
        terms = parse_private_kb(kb_file)
        persist_dir = _build_index_dir(kb_file, "private")
        store = OntologyVectorStore(persist_dir=persist_dir, enable_retrieval=True)
        store.create_or_load_index(terms)
        return store

    if schema_file:
        if not os.path.exists(schema_file):
            raise FileNotFoundError(f"⚠️ 未找到本体文件: {schema_file}")
        terms = parse_schema_org(schema_file)
        persist_dir = _build_index_dir(schema_file, "schema")
        store = OntologyVectorStore(persist_dir=persist_dir, enable_retrieval=True)
        store.create_or_load_index(terms)
        return store

    # 无知识库：完全依赖模型能力
    store = OntologyVectorStore(enable_retrieval=False)
    store.create_or_load_index()
    return store


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

    agent_system = MultiAgentSystem(kg_store, allow_public_uri=allow_public_uri)
    graph_builder = RDFGraphBuilder()

    # 获取所有表
    tables = loader.get_all_table_names()
    print(f"发现表: {tables}")

    print("\n=== Step 2: 多智能体协同映射 ===")
    for table in tables:
        print(f"\n>>> 处理表: {table}")
        
        fingerprint = loader.generate_table_fingerprint(table)
        
        raw_mapping = agent_system.run_mapping_agent(fingerprint)
        print(f"   初次映射: {raw_mapping}")
        
        relations = agent_system.run_relation_agent(fingerprint)
        print(f"   识别关系: {relations}")
        
        final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
        print(f"   最终映射: {final_mapping}")

        df = loader.get_dataframe(table)
        pk = relations.get("pk")
        fks = relations.get("fks", [])
        graph_builder.add_table_data(df, table, final_mapping, primary_key=pk, foreign_keys=fks)

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
    main(args.db_path, kb_file=args.kb_file, schema_file=args.schema_file, allow_public_uri=args.allow_public_uri)
