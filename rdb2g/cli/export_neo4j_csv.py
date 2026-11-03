import argparse
import json
from pathlib import Path

from rdb2g.neo4j.csv_exporter import export_neo4j_csv


def main():
    parser = argparse.ArgumentParser(description="Export a TTL file as Cypher-friendly Neo4j CSV files.")
    parser.add_argument("ttl_file", type=Path, help="Path to the input TTL file.")
    parser.add_argument("--out-dir", type=Path, required=True, help="Directory for nodes.csv, relationships.csv and Cypher helper files.")
    args = parser.parse_args()

    summary = export_neo4j_csv(args.ttl_file, args.out_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
