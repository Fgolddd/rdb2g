import argparse

from rdb2g.pipeline.relation_rule_builder import build_relation_rules


def main():
    parser = argparse.ArgumentParser(description="Generate relation_rules JSON from a SQLite database using profiling and LLM proposals.")
    parser.add_argument("db_path", type=str, help="Path to the input SQLite database file.")
    parser.add_argument("--kb-file", type=str, default=None, help="Optional private KB JSON path used as semantic hints.")
    parser.add_argument("--out", type=str, required=True, help="Output generated relation rules JSON path.")
    parser.add_argument("--report", type=str, required=True, help="Output validation report JSON path.")
    parser.add_argument("--auto-accept-strong", action="store_true", help="Enable deterministic strong rules that pass validation thresholds.")
    parser.add_argument("--min-hit-rate", type=float, default=0.8, help="Minimum hit rate to auto-enable a strong rule.")
    parser.add_argument("--sample-size", type=int, default=10000, help="Maximum sampled rows per table for profiling/probing.")
    args = parser.parse_args()

    build_relation_rules(
        args.db_path,
        kb_file=args.kb_file,
        out_path=args.out,
        report_path=args.report,
        auto_accept_strong=args.auto_accept_strong,
        min_hit_rate=args.min_hit_rate,
        sample_size=args.sample_size,
    )


if __name__ == "__main__":
    main()
