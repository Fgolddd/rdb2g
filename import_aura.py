from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore
from rdflib import Graph
import os

# 推荐：从 .env 文件加载凭据，避免硬编码
# from dotenv import load_dotenv
# load_dotenv()

# 配置 Aura 连接信息
auth_data = {
    'uri': "neo4j+s://a1b9c584.databases.neo4j.io", # 你的 Aura URI
    'database': "neo4j",
    'user': "neo4j",
    'pwd': "SpTPDLpQmXojFcQewQdLYQr4LwoSyFbZs0H3iXR8z_I"
}

# 检查凭据是否存在
if not all(auth_data.values()):
    raise ValueError("Neo4j 连接信息不完整，请检查环境变量或代码中的硬编码值。")

config = Neo4jStoreConfig(auth_data=auth_data)

# 实例化连接到 Neo4j 的 Graph 对象
graph = Graph(store=Neo4jStore(config=config))

# --- 新的、更可靠的导入逻辑 ---
# 1. 在内存中创建一个临时的 rdflib Graph 来解析本地文件
print(f"正在从 'knowledge_graph.ttl' 解析三元组...")
if not os.path.exists("knowledge_graph.ttl"):
    print("错误：'knowledge_graph.ttl' 文件未找到。请先运行 main.py 生成图谱文件。")
else:
    local_g = Graph()
    local_g.parse("knowledge_graph.ttl", format="turtle")

    print(f"解析完成，共找到 {len(local_g)} 个三元组。现在开始分批导入...")

    # 2. 分批将三元组添加到 Neo4j graph 中并手动提交
    batch_size = 100  # 每次提交 100 个三元组
    triples = list(local_g)
    imported_count = 0

    for i in range(0, len(triples), batch_size):
        batch = triples[i:i + batch_size]
        
        # 将一小批三元组添加到连接到 Neo4j 的 graph 对象中
        for triple in batch:
            graph.add(triple)
        
        # 3. 关键步骤：手动提交事务
        try:
            # 某些旧版本可能没有 commit 方法，这是一个兼容性尝试
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
