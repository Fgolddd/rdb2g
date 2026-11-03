"""Run bounded public-data pilots without presenting unaudited metrics as results."""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from experiments.public_kg.artifacts import export_review, file_hash, materialize, score_mapping, write_json
from experiments.public_kg.methods import run_method


ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data/public/benchmark_v1"


def git_version():
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def run(dataset, method, output, retrieval, budget, calls, timeout, max_output, benchmark_dir=BASE,
        ontology_path=BASE / "ontology/target_terms.json"):
    benchmark_dir = Path(benchmark_dir)
    db = benchmark_dir / dataset / "sample.sqlite"
    ontology = Path(ontology_path)
    output = Path(output)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"run output must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    load_dotenv(ROOT / ".env")
    config = {"dataset": dataset, "method": method, "scope": "public-adapted pilot, not formal evaluation",
              "dataset_sha256": file_hash(db), "ontology_sha256": file_hash(ontology),
              "retrieval_sha256": file_hash(retrieval) if retrieval else None,
              "code_git_head": git_version(), "code_git_worktree": "dirty; see individual source hashes",
              "source_sha256": {name: file_hash(ROOT / "experiments/public_kg" / name)
                                for name in ("methods.py", "artifacts.py", "run_pilot.py")},
              "model": os.getenv("QWEN_CHAT_MODEL", "qwen-plus"), "seed": 0,
              "token_budget": budget, "max_calls": calls, "timeout": timeout,
              "max_output_tokens": max_output, "gold_status": "not available, human review pending",
              "python": platform.python_version()}
    write_json(output / "manifest.json", {**config, "status": "started"})
    started = time.monotonic()
    try:
        result = run_method(db, ontology, method, output, token_budget=budget, max_calls=calls,
                            timeout=timeout, max_output_tokens=max_output, retrieval_path=retrieval)
        status = result["run_status"]
        result_path = output / "predictions.json"
        review = output / "review"
        export_review(db, result_path, ontology, review)
        metrics = materialize(db, result_path, ontology, output / "graph") if status == "success" else {
            "status": "not_materialized", "reason": "inference_not_successful"}
        scoring = score_mapping(result, benchmark_dir / dataset / "gold/mapping_gold.csv")
        write_json(output / "evaluation.json", scoring)
        manifest = {**config, "status": status, "elapsed_seconds": time.monotonic() - started,
                    "usage": result["usage"], "tool_usage": result.get("tool_usage"),
                    "graph": metrics, "quality_evaluation": scoring,
                    "prediction_sha256": file_hash(result_path), "review_status": "pending"}
        write_json(output / "manifest.json", manifest)
        return manifest
    except Exception as exc:
        manifest = {**config, "status": "runner_error", "exception_type": type(exc).__name__,
                    "error": str(exc)[:400], "elapsed_seconds": time.monotonic() - started}
        write_json(output / "manifest.json", manifest)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["foshan_osm", "geonames_cn"], required=True)
    parser.add_argument("--method", choices=["full", "single_agent", "w/oValidator", "w/oProbeGate", "without_probe"], required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--retrieval")
    parser.add_argument("--benchmark-dir", type=Path, default=BASE)
    parser.add_argument("--ontology", type=Path, default=BASE / "ontology/target_terms.json")
    parser.add_argument("--token-budget", type=int, default=500000)
    parser.add_argument("--max-calls", type=int, default=12)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--max-output-tokens", type=int, default=24000)
    args = parser.parse_args()
    manifest = run(args.dataset, args.method, args.out, args.retrieval, args.token_budget,
                   args.max_calls, args.timeout, args.max_output_tokens, args.benchmark_dir, args.ontology)
    print(json.dumps({key: manifest[key] for key in ("dataset", "method", "status", "elapsed_seconds", "usage", "graph", "quality_evaluation")}, ensure_ascii=False, indent=2))
    sys.exit(0 if manifest["status"] == "success" else 1)
