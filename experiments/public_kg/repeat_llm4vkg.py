#!/usr/bin/env python3
"""Run independent, bounded LLM4VKG OC/MG benchmark repetitions.

Each attempt is a fresh Python process which invokes the pinned v2 runner and
official LLM4VKG adapter. Prompts, provider responses, usage, errors and
provenance are kept per attempt. No prior predictions or result cache are read.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import traceback
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK = ROOT / "data/public/benchmark_v2"
DEFAULT_OUT = ROOT / "data/eval/public_kg/semantic_repeats_20261030/llm4vkg"
DATASETS = ("foshan_osm", "geonames_cn")
METHOD = "llm4vkg-oc-mg"
MODEL = "qwen3.5-flash"
EMBEDDING_MODEL = "BAAI/bge-m3"
# Allow a separately budgeted supplement run without changing the historical
# 60-request attempts. Defaults preserve the original experiment settings.
MAX_REQUESTS = int(os.environ.get("LLM4VKG_MAX_REQUESTS", "60"))
MAX_TOTAL_TOKENS = int(os.environ.get("LLM4VKG_MAX_TOTAL_TOKENS", "300000"))
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("LLM4VKG_REQUEST_TIMEOUT_SECONDS", "60"))
RUN_TIMEOUT_SECONDS = int(os.environ.get("LLM4VKG_RUN_TIMEOUT_SECONDS", "1800"))


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _secret_values() -> tuple[str, ...]:
    return tuple(value for key, value in os.environ.items()
                 if ("KEY" in key.upper() or "TOKEN" in key.upper() or "SECRET" in key.upper())
                 and value and len(value) >= 8)


def sanitize(value: Any, secrets: tuple[str, ...] | None = None) -> Any:
    """Redact credential values from captured exception/output strings."""
    secrets = _secret_values() if secrets is None else secrets
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, list):
        return [sanitize(item, secrets) for item in value]
    if isinstance(value, tuple):
        return [sanitize(item, secrets) for item in value]
    if isinstance(value, dict):
        return {key: ("[REDACTED]" if any(word in str(key).casefold()
                for word in ("api_key", "authorization", "access_token", "secret"))
                else sanitize(item, secrets)) for key, item in value.items()}
    return value


def metric_summary(values: list[float | int | None]) -> dict[str, float | int | None]:
    """Return mean and sample standard deviation (ddof=1) for available scores."""
    valid = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not valid:
        return {"n": 0, "mean": None, "sample_stddev": None}
    return {"n": len(valid), "mean": statistics.fmean(valid),
            "sample_stddev": statistics.stdev(valid) if len(valid) > 1 else None}


def classify_failure(error: str) -> str:
    text = error.casefold()
    if any(part in text for part in ("request cap reached", "total token cap", "may exceed the total token cap",
                                     "total usage is unknown", "request was not sent")):
        return "request_or_token_budget_limited"
    if "dashscope_api_key is required" in text or "das hscope_api_key" in text:
        return "credential_configuration_error"
    if "modulenotfounderror" in text or "importerror" in text:
        return "dependency_error"
    if any(part in text for part in ("timeout", "timed out", "timeouterror")):
        return "timeout_or_provider_latency"
    if any(part in text for part in ("http", "apierror", "connectionerror", "rate limit")):
        return "provider_or_generation_error"
    return "generation_or_adaptation_error"


def hf_model_revision() -> dict[str, Any]:
    """Describe the cached immutable BGE-M3 revision without hashing multi-GB weights."""
    hub = Path.home() / ".cache/huggingface/hub/models--BAAI--bge-m3"
    ref = hub / "refs/main"
    revision = ref.read_text(encoding="utf-8").strip() if ref.is_file() else None
    snapshot = hub / "snapshots" / revision if revision else None
    files = []
    if snapshot and snapshot.is_dir():
        for path in sorted(snapshot.rglob("*")):
            if path.is_file():
                try:
                    files.append({"path": str(path.relative_to(snapshot)), "size_bytes": path.stat().st_size})
                except OSError:
                    continue
    return {"repository": EMBEDDING_MODEL, "revision": revision,
            "snapshot_path": str(snapshot) if snapshot and snapshot.is_dir() else None,
            "cached_file_count": len(files), "cached_file_inventory": files,
            "local_snapshot_available": bool(snapshot and snapshot.is_dir())}


def source_hashes(dataset: str) -> dict[str, Any]:
    """Hash frozen inputs, adapters, scoring code and the upstream prompt/source."""
    db = BENCHMARK / dataset / "sample.sqlite"
    terms = BENCHMARK / "ontology/target_terms.json"
    upstream = ROOT / "data/public/baselines/LLM4VKG"
    files = {
        "source_database": db,
        "target_terms_ontology": terms,
        "external_methods_adapter": ROOT / "experiments/public_kg/external_methods.py",
        "v2_runner": ROOT / "experiments/public_kg/run_external_v2.py",
        "sqlite_r2rml_adapter": ROOT / "experiments/public_kg/adapt_llm4vkg_sqlite_r2rml.py",
        "strict_mapping_normalizer": ROOT / "experiments/public_kg/normalize_llm4vkg_mapping.py",
        "machine_diagnostic_scorer": ROOT / "experiments/public_kg/machine_diagnostics.py",
        "upstream_oc_mg_source": upstream / "src/vkg_generation/ontology_completion_mapping_generation.py",
        "upstream_datasets_source": upstream / "src/vkg_generation/VKGDatasets.py",
        "upstream_llm_transport_source": upstream / "src/llm/utils/langchain_utils.py",
        "upstream_oc_prompt": upstream / "prompts/OntologyCompletion/prompt.json",
        "upstream_oc_strategy": upstream / "setting/strategy/OntologyCompletion/LLM4VKG.json",
    }
    hashes = {}
    for name, path in files.items():
        hashes[name] = {"path": str(path.resolve()), "sha256": file_hash(path)} if path.is_file() else {
            "path": str(path.resolve()), "sha256": None, "missing": True}
    try:
        commit = subprocess.run(["git", "-C", str(upstream), "rev-parse", "HEAD"],
                                check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception as exc:
        commit = None
        git_error = f"{type(exc).__name__}: {exc}"
    else:
        git_error = None
    return {"files": hashes, "llm4vkg_upstream_git_commit": commit,
            "llm4vkg_upstream_git_error": git_error}


def _metadata(dataset: str, repeat: int, run_dir: Path) -> dict[str, Any]:
    database = BENCHMARK / dataset / "sample.sqlite"
    ontology = BENCHMARK / "ontology/target_terms.json"
    refs = BENCHMARK / dataset / "machine_review_v2"
    base = {
        "method": "LLM4VKG OC/MG",
        "method_entrypoint": "run_external_v2.run('llm4vkg-oc-mg', ...) -> external_methods.llm4vkg_oc_mg",
        "dataset": dataset,
        "repeat": repeat,
        "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_directory": str(run_dir.resolve()),
        "independent_process": True,
        "result_cache_policy": "No prediction/request cache is read; each repeat is a fresh process and unique output directory.",
        "budget": {"max_requests": MAX_REQUESTS, "max_total_tokens": MAX_TOTAL_TOKENS,
                   "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
                   "whole_attempt_timeout_seconds": RUN_TIMEOUT_SECONDS},
        "model": {"chat_model_requested": MODEL, "embedding_model": hf_model_revision(),
                  "embedding_device": "cpu", "provider_endpoint_host": _safe_endpoint_host()},
        "inputs": {
            "source_database": {"path": str(database.resolve()), "sha256": file_hash(database) if database.is_file() else None},
            "target_ontology": {"path": str(ontology.resolve()), "sha256": file_hash(ontology) if ontology.is_file() else None},
            "second_machine_reference": {
                "directory": str(refs.resolve()),
                "not_read_during_generation_or_normalization": True,
                "used_only_for_final_diagnostic_score": True,
            },
        },
        "source_hashes": source_hashes(dataset),
        "requests_path": str((run_dir / "requests.json").resolve()),
    }
    for file_name in ("manifest.json",):
        ref_manifest = refs / file_name
        base["inputs"]["second_machine_reference"]["manifest_sha256"] = (
            file_hash(ref_manifest) if ref_manifest.is_file() else None)
    return base


def _safe_endpoint_host() -> str | None:
    # Keep useful provider provenance without storing URL userinfo or query parameters.
    from urllib.parse import urlsplit
    value = os.environ.get("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    try:
        return urlsplit(value).hostname
    except Exception:
        return None


def _capture_provider_requests(output_file: Path) -> list[dict[str, Any]]:
    """Wrap the existing budget method to preserve exact prompts and raw SDK responses."""
    import external_methods

    records: list[dict[str, Any]] = []
    write_json(output_file, {"requests": records})
    original = external_methods.RequestBudget.chat

    def captured(self, client, scope, model, messages, max_tokens, **kwargs):
        index = len(records) + 1
        request = {
            "request_index": index,
            "scope": scope,
            "model_requested": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "provider_parameters": kwargs,
            "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "status": "in_flight",
            "request_sent": False,
        }
        records.append(request)
        write_json(output_file, {"requests": records})
        before_count = self.request_count
        started = time.perf_counter()
        try:
            response, budget_record = original(self, client, scope, model, messages, max_tokens, **kwargs)
        except Exception as exc:
            request.update({"status": "failed", "request_sent": self.request_count > before_count,
                            "elapsed_seconds": round(time.perf_counter() - started, 3),
                            "error": sanitize(f"{type(exc).__name__}: {exc}"),
                            "traceback": sanitize(traceback.format_exc())})
            write_json(output_file, {"requests": records})
            raise

        usage = getattr(response, "usage", None)
        raw_response = response.model_dump() if hasattr(response, "model_dump") else None
        request.update({
            "status": "response_received",
            "request_sent": True,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "request_budget_record": budget_record,
            "response_model": getattr(response, "model", None),
            "usage": usage.model_dump() if usage and hasattr(usage, "model_dump") else None,
            "raw_response": raw_response,
        })
        request["raw_response"] = sanitize(request["raw_response"])
        request["usage"] = sanitize(request["usage"])
        write_json(output_file, {"requests": records})
        return response, budget_record

    external_methods.RequestBudget.chat = captured
    return records


def _install_langchain_legacy_import_shim() -> None:
    """Expose the legacy langchain.llms import path used by this pinned code."""
    try:
        import langchain
        import langchain_classic.llms.base
    except ImportError:
        return
    legacy_llms = sys.modules.get("langchain.llms")
    if legacy_llms is None:
        import types
        legacy_llms = types.ModuleType("langchain.llms")
        legacy_llms.__path__ = []
        sys.modules["langchain.llms"] = legacy_llms
    legacy_llms.base = langchain_classic.llms.base
    sys.modules["langchain.llms.base"] = langchain_classic.llms.base
    langchain.llms = legacy_llms


def _worker(dataset: str, repeat: int, out_root: Path) -> int:
    run_dir = out_root / dataset / f"r{repeat}"
    run_dir.mkdir(parents=True, exist_ok=True)
    record_path = run_dir / "attempt.json"
    request_path = run_dir / "requests.json"
    record = _metadata(dataset, repeat, run_dir)
    record.update({"status": "starting", "elapsed_seconds": None, "error": None, "score": None})
    write_json(record_path, record)
    write_json(request_path, {"requests": []})
    started = time.perf_counter()
    try:
        if not record["model"]["embedding_model"]["local_snapshot_available"]:
            raise FileNotFoundError(
                "Required local BAAI/bge-m3 snapshot is unavailable; refusing an unbounded download.")

        # The official upstream encoder is used as implemented, with CPU selected by
        # the existing SQLite adapter. Fail closed if its pinned weights are absent.
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
        os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

        sys.path.insert(0, str((ROOT / "experiments/public_kg").resolve()))
        _install_langchain_legacy_import_shim()
        import run_external_v2
        _capture_provider_requests(request_path)
        run_external_v2.MAX_REQUESTS = MAX_REQUESTS
        run_external_v2.MAX_TOTAL_TOKENS = MAX_TOTAL_TOKENS
        run_external_v2.REQUEST_TIMEOUT_SECONDS = REQUEST_TIMEOUT_SECONDS

        database = BENCHMARK / dataset / "sample.sqlite"
        ontology = BENCHMARK / "ontology/target_terms.json"
        result = run_external_v2.run(METHOD, database, ontology, run_dir)
        record["generation"] = result
        record["elapsed_generation_seconds"] = result.get("elapsed_seconds")
        if not result.get("outputs", {}).get("mapping_ttl"):
            raise RuntimeError("official adapter returned without an R2RML mapping output")

        from adapt_llm4vkg_sqlite_r2rml import adapt_and_validate
        from normalize_llm4vkg_mapping import normalize
        raw_mapping = run_dir / result["outputs"]["mapping_ttl"]
        adapted_mapping = run_dir / f"llm4vkg_{dataset}_generated_mapping_sqlite.ttl"
        adaptation_report = run_dir / f"llm4vkg_{dataset}_sqlite_validation.json"
        adaptation = adapt_and_validate(database, raw_mapping, adapted_mapping, adaptation_report)
        record["sqlite_r2rml_adaptation"] = adaptation

        input_ontology = Path(result["ontology_input"])
        generated_ontology = run_dir / result["outputs"]["ontology_ttl"]
        generation_record = run_dir / f"{METHOD}_{dataset}.json"
        normalized_dir = run_dir / "field_mapping_v2"
        normalized = normalize(raw_mapping.resolve(), adapted_mapping.resolve(),
            input_ontology.resolve(), generated_ontology.resolve(), generation_record.resolve(),
            database.resolve(), ontology.resolve(), normalized_dir.resolve(), None)
        # The existing normalizer historically defaulted to GeoNames. For this
        # two-dataset repeat runner, correct only that provenance field.
        normalized["dataset"] = dataset
        normalized["dataset_provenance_corrected_by_repeat_runner"] = True
        write_json(normalized_dir / "manifest.json", normalized)
        record["normalization"] = normalized

        from machine_diagnostics import _validate_reference_provenance, mapping_diagnostics
        reference_dir = BENCHMARK / dataset / "machine_review_v2"
        mapping_reference = reference_dir / "mapping_reference.csv"
        relation_reference = reference_dir / "relation_reference.json"
        provenance_path = reference_dir / "manifest.json"
        _validate_reference_provenance(provenance_path, "second", dataset, database, ontology,
                                       mapping_reference, relation_reference)
        predictions_path = normalized_dir / "predictions.json"
        predictions = json.loads(predictions_path.read_text(encoding="utf-8"))
        mapping_score = mapping_diagnostics(predictions, mapping_reference)
        diagnostic = {
            "status": "machine_reference_diagnostic_only",
            "human_gold": False,
            "dataset": dataset,
            "method": "LLM4VKG",
            "reference_version": "second",
            "reference_manifest_sha256": file_hash(provenance_path),
            "mapping_reference_sha256": file_hash(mapping_reference),
            "prediction_sha256": file_hash(predictions_path),
            "mapping": mapping_score,
        }
        write_json(normalized_dir / "machine_diagnostics.json", diagnostic)
        record["score"] = mapping_score
        record["status"] = "generated_normalized_and_scored"
    except Exception as exc:
        error = sanitize(f"{type(exc).__name__}: {exc}")
        record.update({"status": classify_failure(error), "error": error,
                       "traceback": sanitize(traceback.format_exc())})
    finally:
        record["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        record["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        if request_path.is_file():
            try:
                requests = json.loads(request_path.read_text(encoding="utf-8"))
                record["request_summary"] = _request_summary(requests.get("requests", []))
            except Exception as exc:
                record["request_log_error"] = f"{type(exc).__name__}: {exc}"
        write_json(record_path, record)
    return 0 if record["status"] == "generated_normalized_and_scored" else 1


def _request_summary(requests: list[dict[str, Any]]) -> dict[str, Any]:
    sent = [request for request in requests if request.get("request_sent")]
    known = [request.get("usage", {}).get("total_tokens") for request in sent
             if isinstance(request.get("usage"), dict)
             and request["usage"].get("total_tokens") is not None]
    unknown = sum(1 for request in sent if not isinstance(request.get("usage"), dict)
                  or request["usage"].get("total_tokens") is None)
    return {"logged_request_attempts": len(requests), "provider_requests_sent": len(sent),
            "successful_total_tokens": sum(known), "known_usage_response_count": len(known),
            "unknown_usage_response_count": unknown,
            "elapsed_request_seconds": round(sum(float(r.get("elapsed_seconds", 0)) for r in requests), 3)}


def _attempt_summary(run_dir: Path, returncode: int | None,
                     stdout: str = "", stderr: str = "") -> dict[str, Any]:
    path = run_dir / "attempt.json"
    if path.is_file():
        record = json.loads(path.read_text(encoding="utf-8"))
    else:
        record = {"status": "worker_failed_before_record", "error": None,
                  "score": None, "elapsed_seconds": None}
    record["worker_returncode"] = returncode
    record["worker_stdout"] = sanitize(stdout[-12000:])
    record["worker_stderr"] = sanitize(stderr[-12000:])
    if record.get("status") == "starting" and returncode is None:
        record["status"] = "whole_attempt_timeout"
        record["error"] = f"Attempt exceeded the {RUN_TIMEOUT_SECONDS}s whole-run limit; inspect requests.json for in-flight requests."
        record["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        record["elapsed_seconds"] = RUN_TIMEOUT_SECONDS
    elif record.get("status") == "starting" and returncode not in (0, None):
        record["status"] = "worker_terminated_before_finish"
        record["error"] = sanitize(stderr[-4000:] or f"worker exited with code {returncode}")
    write_json(path, record)
    return record


def _dataset_summary(dataset: str, attempts: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [(item.get("score") or {}) if item.get("status") == "generated_normalized_and_scored" else {}
              for item in attempts]
    metrics = {}
    for name in ("accuracy", "precision", "recall", "f1"):
        metrics[name] = metric_summary([score.get(name) for score in scores])
    return {"dataset": dataset, "attempt_count": len(attempts),
            "successful_scored_attempt_count": sum(item.get("status") == "generated_normalized_and_scored"
                                                     for item in attempts),
            "metrics_mean_and_sample_stddev": metrics,
            "attempts": [{"repeat": item.get("repeat"), "status": item.get("status"),
                "elapsed_seconds": item.get("elapsed_seconds"),
                "request_summary": item.get("request_summary"),
                "score": item.get("score"), "error": item.get("error"),
                "error_type": classify_failure(item["error"]) if item.get("error") else None,
                "attempt_file": str((Path(item.get("run_directory", "")) / "attempt.json").resolve())}
                for item in attempts]}


def run_all(out_root: Path = DEFAULT_OUT, datasets: tuple[str, ...] = DATASETS,
            repeats: int = 3) -> dict[str, Any]:
    out_root = Path(out_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    all_attempts: dict[str, list[dict[str, Any]]] = {}
    for dataset in datasets:
        all_attempts[dataset] = []
        for repeat in range(1, repeats + 1):
            run_dir = out_root / dataset / f"r{repeat}"
            run_dir.mkdir(parents=True, exist_ok=True)
            print(f"Starting LLM4VKG {dataset} r{repeat}", flush=True)
            started = time.perf_counter()
            try:
                completed = subprocess.run(
                    [sys.executable, str(Path(__file__).resolve()), "--worker", dataset,
                     "--repeat", str(repeat), "--out-root", str(out_root)],
                    cwd=ROOT, capture_output=True, text=True,
                    timeout=RUN_TIMEOUT_SECONDS, check=False)
                attempt = _attempt_summary(run_dir, completed.returncode,
                                           completed.stdout, completed.stderr)
            except subprocess.TimeoutExpired as exc:
                out = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
                err = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
                attempt = _attempt_summary(run_dir, None, out, err)
                attempt["elapsed_seconds"] = round(time.perf_counter() - started, 3)
                write_json(run_dir / "attempt.json", attempt)
            all_attempts[dataset].append(attempt)
            print(json.dumps({"dataset": dataset, "repeat": repeat,
                              "status": attempt.get("status"),
                              "elapsed_seconds": attempt.get("elapsed_seconds"),
                              "error": attempt.get("error")}, ensure_ascii=False), flush=True)

    datasets_summary = {dataset: _dataset_summary(dataset, attempts)
                        for dataset, attempts in all_attempts.items()}
    report = {
        "experiment": "Three independent LLM4VKG OC/MG generation attempts per benchmark_v2 dataset",
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "output_root": str(out_root),
        "model": {"chat": MODEL, "embedding": hf_model_revision(), "embedding_device": "cpu"},
        "budget_per_attempt": {"max_requests": MAX_REQUESTS, "max_total_tokens": MAX_TOTAL_TOKENS,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "whole_attempt_timeout_seconds": RUN_TIMEOUT_SECONDS},
        "score_reference": "second machine-reviewed mapping reference; not human gold",
        "score_aggregation": "Mean and sample standard deviation (ddof=1) over completed, scored independent attempts; standard deviation is null for n<2. Failed attempts are listed and excluded from metric summaries.",
        "cache_policy": "No prediction cache or prior run outputs are read; unique rN directories and isolated subprocesses.",
        "datasets": datasets_summary,
        "attempts": all_attempts,
        "limitations": [
            "The external LLM4VKG model is invoked through the Qwen OpenAI-compatible endpoint; the service-side model revision is not pinned by the adapter.",
            "Only exact datatype-property URI projection is scored; no RDF graph materialization is performed.",
            "The second machine reference is diagnostic and is not human-reviewed gold.",
        ],
    }
    write_json(out_root / "summary.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, default=list(DATASETS))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--worker", choices=DATASETS)
    parser.add_argument("--repeat", type=int)
    args = parser.parse_args()
    if args.worker:
        if args.repeat is None or args.repeat < 1:
            parser.error("--worker requires a positive --repeat")
        return _worker(args.worker, args.repeat, args.out_root.resolve())
    if args.repeats != 3:
        parser.error("This repetition experiment is defined as exactly 3 attempts per dataset")
    report = run_all(args.out_root, tuple(args.datasets), args.repeats)
    print(json.dumps({"output_root": report["output_root"], "datasets": {
        dataset: {"statuses": [attempt["status"] for attempt in summary["attempts"]],
                  "metrics": summary["metrics_mean_and_sample_stddev"]}
        for dataset, summary in report["datasets"].items()}}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
