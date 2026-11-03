#!/usr/bin/env python3
"""Run independent repetitions of LLMatch and base Magneto on benchmark v2."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_KG = ROOT / "experiments/public_kg"
BENCHMARK = ROOT / "data/public/benchmark_v2"
DEFAULT_OUTPUT = ROOT / "data/eval/public_kg/semantic_repeats_20261030/external"
MAX_REQUESTS, MAX_TOTAL_TOKENS, REQUEST_TIMEOUT_SECONDS = 60, 300_000, 60
RUN_TIMEOUT_SECONDS, MODEL = 1800, "qwen3.5-flash"
DATASETS = ("foshan_osm", "geonames_cn")
METHODS = ("llmatch", "magneto-base")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def tree_hash(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted((p for p in root.rglob("*.py") if p.is_file()), key=str):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(bytes.fromhex(sha256(path)))
    return digest.hexdigest()


def load_terms(path: Path) -> tuple[dict, list[dict]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    terms = raw.get("terms", raw.get("target_terms", [])) if isinstance(raw, dict) else raw
    if not isinstance(terms, list) or len(terms) != 47:
        raise ValueError("Expected the frozen 47-item benchmark ontology")
    fields = [t for t in terms if t.get("type") == "Property" and t.get("property_kind") == "DatatypeProperty"]
    if len(fields) != 32:
        raise ValueError("Expected 32 DatatypeProperty field targets")
    return raw, fields


def prepare_terms(terms: list[dict], method: str, path: Path) -> Path:
    adapted = []
    for term in terms:
        copy = dict(term)
        if method == "llmatch":
            copy["type"] = "DatatypeProperty"
            annotation = f"OWL property kind: DatatypeProperty; range: {copy.get('range', '')}."
            copy["comment"] = (copy.get("comment", "") + " " + annotation).strip()
        adapted.append(copy)
    write_json(path, {"terms": adapted})
    return path


class AuditedRequestBudget:
    """RequestBudget subclass recording full request/response payloads durably."""
    def __new__(cls, *args, audit_path: Path, **kwargs):
        from external_methods import RequestBudget

        class Budget(RequestBudget):
            def __init__(self, *base_args, audit_path: Path, **base_kwargs):
                super().__init__(*base_args, **base_kwargs)
                self.audit_path = audit_path

            def chat(self, client, scope, model, messages, max_tokens, **call_kwargs):
                before = len(self.requests)
                req = {"model": model, "messages": messages, "max_tokens": max_tokens,
                       "kwargs": call_kwargs}

                def audited_create(**actual_kwargs):
                    # RequestBudget appends its in-flight record before invoking this
                    # wrapper, so a hard process timeout still leaves the sent prompt.
                    self.requests[-1]["request"] = req
                    self.requests[-1]["actual_request_kwargs"] = actual_kwargs
                    write_json(self.audit_path, self.as_dict())
                    return client.chat.completions.create(**actual_kwargs)

                audited_client = type("AuditedClient", (), {})()
                audited_client.chat = type("AuditedChat", (), {})()
                audited_client.chat.completions = type("AuditedCompletions", (), {})()
                audited_client.chat.completions.create = audited_create
                try:
                    response, record = super().chat(audited_client, scope, model, messages, max_tokens, **call_kwargs)
                except Exception as exc:
                    if len(self.requests) > before:
                        self.requests[-1].update({"request": req, "response": None})
                        if "error" in self.requests[-1]:
                            self.requests[-1]["error"] = _secret_safe(self.requests[-1]["error"])
                        write_json(self.audit_path, self.as_dict())
                    raise
                record["request"] = req
                record["response"] = {
                    "model": getattr(response, "model", model),
                    "content": response.choices[0].message.content or "",
                    "usage": (response.usage.model_dump() if getattr(response, "usage", None)
                              and hasattr(response.usage, "model_dump") else None)}
                self.requests[-1] = redact(self.requests[-1])
                write_json(self.audit_path, self.as_dict())
                return response, record

        return Budget(*args, audit_path=audit_path, **kwargs)


def source_hashes(db: Path, ontology: Path, effective_terms: Path, method: str) -> dict[str, str]:
    baseline = ROOT / ("data/public/baselines/LLMatch" if method == "llmatch"
                       else "data/public/baselines/Magneto/algorithms/magneto")
    return {
        "database_sha256": sha256(db), "ontology_47_sha256": sha256(ontology),
        "effective_target_terms_sha256": sha256(effective_terms),
        "runner_sha256": sha256(Path(__file__)),
        "external_methods_sha256": sha256(PUBLIC_KG / "external_methods.py"),
        "normalizer_sha256": sha256(PUBLIC_KG / "normalize_external_predictions.py"),
        "diagnostics_sha256": sha256(PUBLIC_KG / "machine_diagnostics.py"),
        "upstream_python_source_sha256": tree_hash(baseline)}


def _secret_safe(message: str) -> str:
    secret = os.environ.get("DASHSCOPE_API_KEY", "")
    return message.replace(secret, "[REDACTED]") if secret else message


def redact(value: Any) -> Any:
    if isinstance(value, str):
        return _secret_safe(value)
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, dict):
        return {key: redact(item) for key, item in value.items()}
    return value


def run_one(method: str, dataset: str, replicate: int, out: Path,
            request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS, max_output_tokens: int = 16000) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    import external_methods as adapters

    db = BENCHMARK / dataset / "sample.sqlite"
    ontology = BENCHMARK / "ontology/target_terms.json"
    raw_ontology, datatype_terms = load_terms(ontology)
    out.mkdir(parents=True, exist_ok=True)
    terms_path = prepare_terms(datatype_terms, method, out / "effective_target_terms.json")
    hashes = source_hashes(db, ontology, terms_path, method)
    raw_path, audit_path = out / "raw_result.json", out / "request_audit.json"
    cache = {"prediction_cache_hit": False,
             "prediction_cache_policy": "none; independent child process and fresh inference"}
    payload, error = {}, None
    started = time.monotonic()
    if method == "llmatch":
        budget = AuditedRequestBudget(max_calls=MAX_REQUESTS, max_total_tokens=MAX_TOTAL_TOKENS,
                                      timeout_seconds=request_timeout_seconds, audit_path=audit_path)
        write_json(audit_path, budget.as_dict())
        cache.update({"upstream_cache": "process-local SimpleCache", "upstream_cache_hit": False})
        try:
            payload = adapters.llmatch_predict(
                db, terms_path, raw_path, model=MODEL, request_budget=budget,
                request_scope=f"LLMatch:{dataset}:r{replicate}", max_llm_calls=MAX_REQUESTS,
                max_total_tokens=MAX_TOTAL_TOKENS, timeout_seconds=request_timeout_seconds,
                max_output_tokens=max_output_tokens)
        except Exception as exc:
            error = _secret_safe(f"{type(exc).__name__}: {exc}")
            payload = {"method": "LLMatch", "status": "runner_failed", "predictions": [],
                       "error": error, "request_budget": budget.as_dict()}
        payload.setdefault("request_budget", budget.as_dict())
        payload["request_audit"] = audit_path.name
        payload["frozen_ontology_term_count"], payload["field_target_term_count"] = len(raw_ontology["terms"]), 32
        cache["cache_hit"] = False
    else:
        cache_path = Path(os.environ.get("HF_HUB_CACHE", Path.home() / ".cache/huggingface/hub")) / "models--sentence-transformers--all-MiniLM-L6-v2"
        cache.update({"embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
                      "embedding_model_cache_hit": cache_path.exists(), "model_cache_path": str(cache_path)})
        try:
            predictions = adapters.magneto_predict(db, terms_path, raw_path)
            payload = json.loads(raw_path.read_text(encoding="utf-8"))
            payload["predictions"] = predictions
            payload["frozen_ontology_term_count"], payload["field_target_term_count"] = len(raw_ontology["terms"]), 32
            cache["cache_hit"] = cache["embedding_model_cache_hit"]
        except Exception as exc:
            error = _secret_safe(f"{type(exc).__name__}: {exc}")
            payload = {"method": "Magneto", "status": "runner_failed", "predictions": [], "error": error}
    if not raw_path.exists():
        write_json(raw_path, payload)
    else:
        raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        write_json(raw_path, redact(raw_payload))

    elapsed = time.monotonic() - started
    status = payload.get("status", "unknown")
    norm_dir = out / "normalized"
    completed = ((method == "llmatch" and status == "prediction_component_completed") or
                 (method == "magneto-base" and status == "base_method_completed"))
    if completed:
        from normalize_external_predictions import normalize
        normalized = normalize(raw_path, db, ontology, norm_dir,
                               "LLMatch" if method == "llmatch" else "Magneto", "datatype_only")
    else:
        # A failed prediction is explicitly non-success; it is not a normalized output or a zero score.
        norm_dir.mkdir(parents=True, exist_ok=True)
        failed_prediction = {"run_status": "inference_failed", "table_mappings": [], "mappings": [], "relation_rules": []}
        write_json(norm_dir / "predictions.json", failed_prediction)
        write_json(norm_dir / "manifest.json", {"method": method, "dataset": dataset,
            "dataset_sha256": hashes["database_sha256"], "ontology_sha256": hashes["ontology_47_sha256"],
            "prediction_sha256": sha256(norm_dir / "predictions.json"), "target_scope": "datatype_only",
            "target_term_count": 32, "field_count": 0, "full_pipeline_reproduction": False,
            "quality_scored": False, "human_gold": False})
        normalized = {"status": "not_normalized_upstream_failure", "reason": status}

    command = [sys.executable, "-m", "experiments.public_kg.machine_diagnostics", "--run", str(norm_dir),
               "--benchmark-dir", str(BENCHMARK), "--reference-version", "second"]
    try:
        checked = subprocess.run(command, cwd=ROOT, text=True, capture_output=True,
                                 timeout=RUN_TIMEOUT_SECONDS, check=False)
        if checked.returncode == 0 and (norm_dir / "machine_diagnostics.json").is_file():
            diagnostic = json.loads((norm_dir / "machine_diagnostics.json").read_text(encoding="utf-8"))
        else:
            diagnostic = {"status": "diagnostics_failed", "exit_code": checked.returncode,
                          "stdout": checked.stdout[-4000:], "stderr": _secret_safe(checked.stderr[-4000:])}
    except subprocess.TimeoutExpired as exc:
        diagnostic = {"status": "diagnostics_timeout", "timeout_seconds": RUN_TIMEOUT_SECONDS,
                      "stdout": str(exc.stdout or "")[-4000:], "stderr": str(exc.stderr or "")[-4000:]}

    record = {
        "method": method, "dataset": dataset, "replicate": replicate, "status": status,
        "error": error or payload.get("error"), "elapsed_seconds": round(elapsed, 3),
        "model_configuration": ({"chat_model": MODEL, "calls_max": MAX_REQUESTS,
            "total_tokens_max": MAX_TOTAL_TOKENS, "request_timeout_seconds": request_timeout_seconds,
            "run_timeout_seconds": RUN_TIMEOUT_SECONDS} if method == "llmatch" else
            {"embedding_model": "sentence-transformers/all-MiniLM-L6-v2", "embedding_name": "minilm",
             "use_gpt_reranker": False, "topk": 20, "sampling_size": 10, "bipartite_reranker": True}),
        "source_hashes": hashes, "cache": cache, "request_budget": payload.get("request_budget"),
        "request_audit_path": str(audit_path) if audit_path.exists() else None,
        "raw_result_path": str(raw_path), "raw_prediction_sha256": sha256(raw_path),
        "normalized_path": str(norm_dir / "predictions.json"),
        "normalized_prediction_sha256": sha256(norm_dir / "predictions.json"),
        "normalization": normalized, "machine_diagnostics": diagnostic,
        "full_pipeline_reproduction": False, "human_gold": False,
        "inference_read_machine_reference_or_gold": False}
    write_json(out / "run_manifest.json", record)
    return record


def summarize_runs(runs: list[dict]) -> dict:
    by_key: dict[tuple[str, str], list[dict]] = {}
    for run in runs:
        by_key.setdefault((run["method"], run["dataset"]), []).append(run)
    groups = []
    for (method, dataset), items in sorted(by_key.items()):
        metrics = {}
        for name in ("accuracy", "precision", "recall", "f1"):
            vals = [float((r.get("machine_diagnostics") or {}).get("mapping", {}).get(name))
                    for r in items if (r.get("machine_diagnostics") or {}).get("status") == "machine_reference_diagnostic_only"
                    and isinstance((r.get("machine_diagnostics") or {}).get("mapping", {}).get(name), (int, float))]
            mean = sum(vals) / len(vals) if vals else None
            std = math.sqrt(sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)) if len(vals) > 1 else None
            metrics[name] = {"n_scored": len(vals), "mean": mean, "std_sample": std}
        hashes = [r.get("normalized_prediction_sha256") for r in sorted(items, key=lambda x: x["replicate"])]
        groups.append({"method": method, "dataset": dataset, "replicate_count": len(items),
                       "statuses": [r.get("status") for r in sorted(items, key=lambda x: x["replicate"])],
                       "successful_diagnostics": sum((r.get("machine_diagnostics") or {}).get("status") == "machine_reference_diagnostic_only" and (r.get("machine_diagnostics") or {}).get("mapping", {}).get("status") == "machine_reference_diagnostic_only" for r in items),
                       "metrics_over_scored_runs_only": metrics,
                       "normalized_prediction_sha256": hashes,
                       "predictions_deterministic": len(set(hashes)) == 1 if method == "magneto-base" and all(hashes) else None})
    return {"aggregation": "mean and sample standard deviation (ddof=1), scored repetitions only",
            "failed_runs_count_as_zero": False, "groups": groups}


def run_all(output_root: Path, replicates: int = 3, methods: tuple[str, ...] = METHODS,
            request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS, max_output_tokens: int = 16000) -> dict:
    runs = []
    for method in methods:
        for dataset in DATASETS:
            for replicate in range(1, replicates + 1):
                dest = output_root / method / dataset / f"r{replicate}"
                dest.mkdir(parents=True, exist_ok=True)
                cmd = [sys.executable, str(Path(__file__).resolve()), "--single", "--method", method,
                       "--dataset", dataset, "--replicate", str(replicate), "--out-dir", str(dest),
                       "--request-timeout", str(request_timeout_seconds),
                       "--max-output-tokens", str(max_output_tokens)]
                try:
                    child = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True,
                                           timeout=RUN_TIMEOUT_SECONDS, check=False)
                    manifest = dest / "run_manifest.json"
                    if manifest.is_file():
                        run = json.loads(manifest.read_text(encoding="utf-8"))
                    else:
                        detail = _secret_safe((child.stderr or child.stdout)[-5000:])
                        run = {"method": method, "dataset": dataset, "replicate": replicate,
                               "status": "child_process_failed", "error": f"exit {child.returncode}: {detail}",
                               "source_hashes": {}, "cache": {"prediction_cache_hit": False},
                               "machine_diagnostics": {"status": "not_scored_upstream_failure", "reason": "child process failed"}}
                except subprocess.TimeoutExpired as exc:
                    stderr = exc.stderr or ""
                    if isinstance(stderr, bytes): stderr = stderr.decode(errors="replace")
                    run = {"method": method, "dataset": dataset, "replicate": replicate,
                           "status": "run_timeout", "error": _secret_safe(f"exceeded {RUN_TIMEOUT_SECONDS}s: {stderr[-3000:]}"),
                           "source_hashes": {}, "cache": {"prediction_cache_hit": False},
                           "machine_diagnostics": {"status": "not_scored_upstream_failure", "reason": "run timeout"}}
                    write_json(dest / "run_manifest.json", run)
                runs.append(run)
                print(json.dumps({"method": method, "dataset": dataset, "replicate": replicate,
                                  "status": run.get("status"), "elapsed_seconds": run.get("elapsed_seconds")}), flush=True)
    result = {"status": "completed_attempts", "frozen_ontology_term_count": 47,
              "field_target_count": 32, "replicates_per_method_dataset": replicates, "runs": runs,
              "summary": summarize_runs(runs),
              "upstream_scope": {"LLMatch": "upstream prediction component; per-process SimpleCache; Qwen-compatible transport; no table-selection or expert-feedback branches",
                                 "Magneto Base": "upstream MiniLM embedding and bipartite reranker; existing base parameters; no GPT reranker"},
              "limits": {"LLMatch_max_requests_per_run": MAX_REQUESTS, "LLMatch_max_total_tokens_per_run": MAX_TOTAL_TOKENS,
                         "request_timeout_seconds": request_timeout_seconds, "wall_timeout_seconds_per_run": RUN_TIMEOUT_SECONDS},
              "diagnostics_reference": "second machine-reviewed reference; diagnostic only, not human gold"}
    write_json(output_root / "repeat_summary.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--method", choices=METHODS)
    parser.add_argument("--dataset", choices=DATASETS)
    parser.add_argument("--replicate", type=int, default=1)
    parser.add_argument("--replicates", type=int, default=3)
    parser.add_argument("--methods", nargs="+", choices=METHODS, default=list(METHODS))
    parser.add_argument("--request-timeout", type=int, default=REQUEST_TIMEOUT_SECONDS)
    parser.add_argument("--max-output-tokens", type=int, default=16000)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.single:
        if not args.method or not args.dataset:
            parser.error("--single needs --method and --dataset")
        result = run_one(args.method, args.dataset, args.replicate, args.out_dir.resolve(), args.request_timeout, args.max_output_tokens)
    else:
        result = run_all(args.out_dir.resolve(), args.replicates, tuple(args.methods), args.request_timeout, args.max_output_tokens)
    print(json.dumps({"status": result.get("status"), "method": result.get("method"),
                      "dataset": result.get("dataset"), "replicate": result.get("replicate")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
