import os
import argparse
from dotenv import load_dotenv
from dataloader import SpiderDataLoader
from schema_parser import parse_schema_org
from vector_store import OntologyVectorStore
from agents import MultiAgentSystem
from graph_builder import RDFGraphBuilder

# 加载环境变量
load_dotenv()

def main(db_path, schema_file):
    # 配置路径现在通过函数参数传入
    DB_PATH = db_path
    SCHEMA_FILE = schema_file
    
    print("=== Step 1: 初始化系统 ===")
    # 1. 准备向量库
    kg_store = OntologyVectorStore()
    chroma_dir = "./data/chroma_db"
    need_build = not (os.path.exists(chroma_dir) and os.listdir(chroma_dir))
    if need_build:
        if not os.path.exists(SCHEMA_FILE):
            print(f"⚠️ 未找到本体文件: {SCHEMA_FILE}，无法构建向量索引。")
            return
        terms = parse_schema_org(SCHEMA_FILE)
        kg_store.create_or_load_index(terms)
    else:
        kg_store.create_or_load_index()  # 加载已有

    # 2. 初始化数据加载器
    try:
        loader = SpiderDataLoader(DB_PATH)
    except FileNotFoundError:
        print(f"⚠️ 未找到数据库文件: {DB_PATH}，跳过执行。")
        return

    agent_system = MultiAgentSystem(kg_store)
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
    parser = argparse.ArgumentParser(description="Generate a Knowledge Graph from a SQLite database and a Schema.org ontology.")
    parser.add_argument("db_path", type=str, help="Path to the input SQLite database file.")
    parser.add_argument("schema_file", type=str, help="Path to the Schema.org JSON-LD file.")
    args = parser.parse_args()

    # 使用从命令行解析的参数调用 main 函数
    main(args.db_path, args.schema_file)
