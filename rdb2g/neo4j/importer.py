from neo4j import GraphDatabase
from rdflib import Graph
from rdflib_neo4j import Neo4jStore, Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY


def import_ttl_to_neo4j(
    ttl_file,
    neo4j_uri: str,
    neo4j_database: str,
    neo4j_user: str,
    neo4j_pwd: str,
    batch_size: int = 5000,
    progress_interval: int = 5000,
    vocab_strategy: str = "IGNORE",
):
    if not ttl_file.exists():
        raise FileNotFoundError(f"TTL 文件不存在: {ttl_file}")

    auth_data = {
        "uri": neo4j_uri,
        "database": neo4j_database,
        "user": neo4j_user,
        "pwd": neo4j_pwd,
    }
    if not all(auth_data.values()):
        raise ValueError("Neo4j 连接信息不完整。")

    config = Neo4jStoreConfig(
        auth_data=auth_data,
        batching=True,
        batch_size=batch_size,
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY[vocab_strategy],
    )
    graph = Graph(store=Neo4jStore(config=config))

    local_g = Graph()
    local_g.parse(str(ttl_file), format="turtle")
    total = len(local_g)
    print(f"[Import] 解析完成，共 {total} 个三元组，开始导入 Neo4j...")

    for i, triple in enumerate(local_g, start=1):
        graph.add(triple)
        if i % progress_interval == 0:
            print(f"[Import] 已处理 {i}/{total}")

    graph.close()
    print(f"[Import] 导入完成，共处理 {total} 个三元组。")

    enhance_neo4j_display_fields(auth_data)


def enhance_neo4j_display_fields(auth_data: dict):
    """导入后补充中文显示字段，兼容 Neo4j Browser 与业务查询展示。"""
    print("[Import] 开始补充 Neo4j 中文显示字段...")
    driver = GraphDatabase.driver(
        auth_data["uri"],
        auth=(auth_data["user"], auth_data["pwd"]),
    )

    cypher_statements = [
        """
        MATCH (n)
        WITH n,
             coalesce(
                 n.display_name_zh,
                 n.name,
                 n['schema__name'],
                 n['http://schema.org/name'],
                 n['rdfs__label'],
                 n['http://www.w3.org/2000/01/rdf-schema#label']
             ) AS raw_display,
             coalesce(n.uri, n['rdf__about'], n['@id']) AS raw_uri
        SET n.display_name_zh =
            CASE
                WHEN raw_display IS NULL OR trim(toString(raw_display)) = ''
                    THEN coalesce(n.display_name_zh, split(toString(raw_uri), '/')[-1], toString(id(n)))
                ELSE toString(raw_display)
            END,
            n.display_uri_short =
            CASE
                WHEN raw_uri IS NULL OR trim(toString(raw_uri)) = ''
                    THEN coalesce(n.display_uri_short, toString(id(n)))
                ELSE split(replace(toString(raw_uri), '#', '/'), '/')[-1]
            END,
            n.name = coalesce(n.name, n.display_name_zh)
        """,
        """
        MATCH ()-[r]->()
        WITH r,
             coalesce(
                 r.display_name_zh,
                 r.name,
                 r['rdfs__label'],
                 r['http://www.w3.org/2000/01/rdf-schema#label']
             ) AS raw_display
        SET r.display_name_zh =
            CASE
                WHEN raw_display IS NULL OR trim(toString(raw_display)) = ''
                    THEN coalesce(r.display_name_zh, type(r))
                ELSE toString(raw_display)
            END
        """,
    ]

    try:
        with driver.session(database=auth_data["database"]) as session:
            for stmt in cypher_statements:
                session.run(stmt)
        print("[Import] 中文显示字段补充完成。")
    finally:
        driver.close()
