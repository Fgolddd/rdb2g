import os
from dotenv import load_dotenv
from dataloader import SpiderDataLoader
from schema_parser import parse_schema_org
from vector_store import OntologyVectorStore
from agents import MultiAgentSystem
from graph_builder import RDFGraphBuilder

# 加载环境变量
load_dotenv()

def main():
    # 配置路径
    DB_PATH = "data/spider_data/database/cinema/cinema.sqlite"  # 请替换为实际 Spider 数据库路径
    SCHEMA_FILE = "data/schemaorg.jsonld"
    
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
    # 注意：这里假设 spider 数据库存在。如果不存在，可以写一个简单的创建逻辑用于演示。
    try:
        loader = SpiderDataLoader(DB_PATH)
    except FileNotFoundError:
        print("⚠️ 未找到数据库文件，跳过执行。请修改 DB_PATH。")
        return

    agent_system = MultiAgentSystem(kg_store)
    graph_builder = RDFGraphBuilder()

    # 获取所有表
    tables = loader.get_all_table_names()
    print(f"发现表: {tables}")

    print("\n=== Step 2: 多智能体协同映射 ===")
    for table in tables:
        print(f"\n>>> 处理表: {table}")
        
        # 2.1 生成指纹
        fingerprint = loader.generate_table_fingerprint(table)
        
        # 2.2 运行智能体流水线
        # A. 映射
        raw_mapping = agent_system.run_mapping_agent(fingerprint)
        print(f"   初次映射: {raw_mapping}")
        
        # B. 关系
        relations = agent_system.run_relation_agent(fingerprint)
        print(f"   识别关系: {relations}")
        
        # C. 验证 (创新点)
        final_mapping = agent_system.run_validator_agent(fingerprint, raw_mapping, relations)
        print(f"   最终映射: {final_mapping}")

        # 2.3 添加到图谱构建器
        df = loader.get_dataframe(table)
        pk = relations.get("pk")
        graph_builder.add_table_data(df, table, final_mapping, primary_key=pk)

    loader.close()

    print("\n=== Step 3: 导出知识图谱 ===")
    graph_builder.save_graph("knowledge_graph.ttl")

if __name__ == "__main__":
    main()