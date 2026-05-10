import argparse
import os
import subprocess
import sys
from pathlib import Path

from rdflib import Graph
from rdflib_neo4j import Neo4jStoreConfig, Neo4jStore
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
from neo4j import GraphDatabase

from main import main as build_ttl


def import_ttl_to_neo4j(
    ttl_file: Path,
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

    _enhance_neo4j_display_fields(auth_data)


def _enhance_neo4j_display_fields(auth_data: dict):
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


def run_eval(
    db_path: Path,
    question_bank: Path,
    out_dir: Path,
    engine: str,
    neo4j_uri: str,
    neo4j_database: str,
    neo4j_user: str,
    neo4j_pwd: str,
):
    cmd = [
        sys.executable,
        str(Path(__file__).with_name("run_rdb_kg_eval.py")),
        "--question-bank",
        str(question_bank),
        "--db-path",
        str(db_path),
        "--engine",
        engine,
        "--out-dir",
        str(out_dir),
        "--neo4j-uri",
        neo4j_uri,
        "--neo4j-database",
        neo4j_database,
        "--neo4j-user",
        neo4j_user,
        "--neo4j-pwd",
        neo4j_pwd,
    ]
    print("[Eval] 开始执行:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="One-shot pipeline: build TTL -> import Neo4j -> run evaluation.")
    parser.add_argument("db_path", type=Path, help="SQLite DB path")
    parser.add_argument("--schema-file", type=Path, default=Path("data/schemaorg.jsonld"), help="Schema.org JSON-LD path")
    parser.add_argument("--kb-file", type=Path, default=None, help="Optional private KB JSON path")
    parser.add_argument("--relation-rules", type=Path, default=None, help="Optional relation rules JSON for KG path edges")
    parser.add_argument("--allow-public-uri", action="store_true", help="Allow public URI in no-knowledge mode")
    parser.add_argument("--question-bank", type=Path, required=True, help="Evaluation question bank CSV path")
    parser.add_argument("--out-dir", type=Path, default=Path("data/eval/full_experiment_run"), help="Evaluation output dir")
    parser.add_argument("--engine", choices=["rdb", "kg", "both"], default="both", help="Evaluation engine")
    parser.add_argument("--skip-import", action="store_true", help="Skip Neo4j import")
    parser.add_argument("--skip-eval", action="store_true", help="Skip evaluation")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("IMPORT_BATCH_SIZE", "5000")))
    parser.add_argument("--progress-interval", type=int, default=int(os.getenv("IMPORT_PROGRESS_INTERVAL", "5000")))
    parser.add_argument("--vocab-strategy", choices=["SHORTEN", "MAP", "KEEP", "IGNORE"], default=os.getenv("NEO4J_VOCAB_STRATEGY", "IGNORE"))
    parser.add_argument("--neo4j-uri", type=str, default=os.getenv("NEO4J_URI", "neo4j+s://35dae612.databases.neo4j.io"))
    parser.add_argument("--neo4j-database", type=str, default=os.getenv("NEO4J_DATABASE", "35dae612"))
    parser.add_argument("--neo4j-user", type=str, default=os.getenv("NEO4J_USER", "35dae612"))
    parser.add_argument("--neo4j-pwd", type=str, default=os.getenv("NEO4J_PWD", "snbSppE_EKE6RDjFMuWbHuQgQ9p8go_70IoW3BaPD0A"))
    args = parser.parse_args()

    db_path = args.db_path.resolve()
    schema_file = args.schema_file.resolve() if args.schema_file else None
    kb_file = args.kb_file.resolve() if args.kb_file else None
    relation_rules = args.relation_rules.resolve() if args.relation_rules else None

    print("[Pipeline] Step 1/3 生成 TTL")
    build_ttl(
        str(db_path),
        kb_file=str(kb_file) if kb_file else None,
        schema_file=str(schema_file) if schema_file else None,
        allow_public_uri=args.allow_public_uri,
        relation_rules_file=str(relation_rules) if relation_rules else None,
    )

    ttl_file = Path("data/ttl") / f"{db_path.stem}.ttl"
    if not ttl_file.exists():
        raise FileNotFoundError(f"未找到生成的 TTL 文件: {ttl_file}")

    if not args.skip_import:
        print("[Pipeline] Step 2/3 导入 Neo4j")
        import_ttl_to_neo4j(
            ttl_file=ttl_file,
            neo4j_uri=args.neo4j_uri,
            neo4j_database=args.neo4j_database,
            neo4j_user=args.neo4j_user,
            neo4j_pwd=args.neo4j_pwd,
            batch_size=args.batch_size,
            progress_interval=args.progress_interval,
            vocab_strategy=args.vocab_strategy,
        )

    if not args.skip_eval:
        print("[Pipeline] Step 3/3 运行测评")
        args.out_dir.mkdir(parents=True, exist_ok=True)
        run_eval(
            db_path=db_path,
            question_bank=args.question_bank.resolve(),
            out_dir=args.out_dir.resolve(),
            engine=args.engine,
            neo4j_uri=args.neo4j_uri,
            neo4j_database=args.neo4j_database,
            neo4j_user=args.neo4j_user,
            neo4j_pwd=args.neo4j_pwd,
        )

    print("[Pipeline] 完成。")


if __name__ == "__main__":
    main()
