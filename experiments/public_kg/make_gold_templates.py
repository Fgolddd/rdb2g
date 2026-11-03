"""Create unlabelled field-gold review sheets without reading model predictions."""

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data/public/benchmark_v1"
FIELDS = ["table", "column", "accepted_uris_json", "no_match", "review_status", "reviewer", "evidence"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=BASE)
    args = parser.parse_args()
    for dataset in ("foshan_osm", "geonames_cn"):
        folder = args.base / dataset / "annotation"
        with (folder / "mapping_tasks.csv").open(encoding="utf-8-sig", newline="") as stream:
            tasks = list(csv.DictReader(stream))
        destination = folder / "mapping_gold_template.csv"
        with destination.open("x", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows({"table": row["table"], "column": row["column"], "review_status": "pending"}
                             for row in tasks)
        print(f"{destination}: {len(tasks)} pending")


if __name__ == "__main__":
    main()
