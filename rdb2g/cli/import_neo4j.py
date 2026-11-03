import argparse
import os
from pathlib import Path

from rdb2g.neo4j.importer import import_ttl_to_neo4j


def main():
    parser = argparse.ArgumentParser(description="Import an RDF TTL file into Neo4j.")
    parser.add_argument("ttl_file", type=Path, help="Path to the TTL file to import.")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("IMPORT_BATCH_SIZE", "5000")))
    parser.add_argument("--progress-interval", type=int, default=int(os.getenv("IMPORT_PROGRESS_INTERVAL", "5000")))
    parser.add_argument("--vocab-strategy", choices=["SHORTEN", "MAP", "KEEP", "IGNORE"], default=os.getenv("NEO4J_VOCAB_STRATEGY", "IGNORE"))
    parser.add_argument("--neo4j-uri", type=str, default=os.getenv("NEO4J_URI"))
    parser.add_argument("--neo4j-database", type=str, default=os.getenv("NEO4J_DATABASE"))
    parser.add_argument("--neo4j-user", type=str, default=os.getenv("NEO4J_USER"))
    parser.add_argument("--neo4j-pwd", type=str, default=os.getenv("NEO4J_PWD"))
    args = parser.parse_args()

    import_ttl_to_neo4j(
        ttl_file=args.ttl_file,
        neo4j_uri=args.neo4j_uri,
        neo4j_database=args.neo4j_database,
        neo4j_user=args.neo4j_user,
        neo4j_pwd=args.neo4j_pwd,
        batch_size=args.batch_size,
        progress_interval=args.progress_interval,
        vocab_strategy=args.vocab_strategy,
    )


if __name__ == "__main__":
    main()
