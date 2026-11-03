import argparse

from rdb2g.pipeline.ttl_builder import build_ttl


def main():
    parser = argparse.ArgumentParser(description="Generate a Knowledge Graph from a SQLite database with optional knowledge retrieval.")
    parser.add_argument("db_path", type=str, help="Path to the input SQLite database file.")
    parser.add_argument("--kb-file", type=str, default=None, help="Path to the private knowledge base JSON file.")
    parser.add_argument("--schema-file", type=str, default=None, help="Optional path to a Schema.org JSON-LD file.")
    parser.add_argument("--allow-public-uri", action="store_true", help="Allow public ontology URIs in no-knowledge mode.")
    parser.add_argument("--relation-rules", type=str, default=None, help="Optional relation rules JSON for KG path edges.")
    args = parser.parse_args()

    build_ttl(
        args.db_path,
        kb_file=args.kb_file,
        schema_file=args.schema_file,
        allow_public_uri=args.allow_public_uri,
        relation_rules_file=args.relation_rules,
    )


if __name__ == "__main__":
    main()
