from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore
from rdflib import Graph
import os
import argparse

# --- 1. 设置命令行参数解析 ---
parser = argparse.ArgumentParser(description="Import an RDF TTL file into Neo4j Aura.")
parser.add_argument("ttl_file", type=str, help="Path to the .ttl file to import.")
args = parser.parse_args()
ttl_file_path = args.ttl_file

# 推荐：从 .env 文件加载凭据
# from dotenv import load_dotenv
# load_dotenv()

# 配置 Aura 连接信息
auth_data = {
    'uri': os.getenv("NEO4J_URI", "neo4j+s://a1b9c584.databases.neo4j.io"),
    'database': os.getenv("NEO4J_DATABASE", "neo4j"),
    'user': os.getenv("NEO4J_USER", "neo4j"),
    'pwd': os.getenv("NEO4J_PWD", "SpTPDLpQmXojFcQewQdLYQr4LwoSyFbZs0H3iXR8z_I")
}

if not all(auth_data.values()):
    raise ValueError("Neo4j 连接信息不完整，请检查环境变量或代码中的硬编码值。")

config = Neo4jStoreConfig(auth_data=auth_data)
graph = Graph(store=Neo4jStore(config=config))

# --- 2. 使用从命令行获取的文件路径 ---
print(f"准备从 '{ttl_file_path}' 解析三元组...")
if not os.path.exists(ttl_file_path):
    print(f"错误：文件 '{ttl_file_path}' 未找到。请检查文件路径是否正确。")
else:
    local_g = Graph()
    local_g.parse(ttl_file_path, format="turtle")

    print(f"解析完成，共找到 {len(local_g)} 个三元组。现在开始分批导入...")

    batch_size = 100
    triples = list(local_g)
    imported_count = 0

    for i in range(0, len(triples), batch_size):
        batch = triples[i:i + batch_size]
        for triple in batch:
            graph.add(triple)
        
        try:
            if hasattr(graph.store, 'commit') and callable(graph.store.commit):
                graph.store.commit()
            imported_count += len(batch)
            print(f"成功提交批次 {(i // batch_size) + 1}，已导入 {imported_count}/{len(triples)} 个三元组。")
        except Exception as e:
            print(f"批次 {(i // batch_size) + 1} 提交失败: {e}")
            if hasattr(graph.store, 'rollback') and callable(graph.store.rollback):
                graph.store.rollback()

    print("导入完成！")
    graph.close()
