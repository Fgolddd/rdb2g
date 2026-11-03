#!/usr/bin/env python3
"""Bounded v2 runner for pinned public-KG baselines.

Applies request, timeout, and total-token budgets; keeps v2 OWL property kinds
and ranges in LLM4VKG's input ontology; writes results into a distinct output
directory. It does not read review data or claim full pipeline reproduction.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import OWL, XSD

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TERMS = ROOT / "data/public/benchmark_v2/ontology/target_terms.json"
MAX_REQUESTS = 60
MAX_TOTAL_TOKENS = 300_000
MAX_RESPONSE_TOKENS = 16_000
REQUEST_TIMEOUT_SECONDS = 60


def _term_iri(value: str, terms: list[dict[str, Any]]) -> URIRef:
    if value == "Literal":
        return URIRef(str(RDFS.Literal))
    if value.startswith(("http://", "https://", "urn:")):
        return URIRef(value)
    by_label = {str(term.get("label", "")): URIRef(term["uri"]) for term in terms}
    if value in by_label:
        return by_label[value]
    if value.startswith("xsd:"):
        return URIRef(str(XSD) + value.split(":", 1)[1])
    raise ValueError(f"Cannot resolve v2 ontology term reference {value!r}")


def _patch_owl_term_insertion(terms: list[dict[str, Any]]):
    """Reflect v2 property_kind and range in the input graph built by adapter."""
    lookup = {str(term["uri"]): term for term in terms}
    original_add = Graph.add
    applied = {"object_property_types": 0, "ranges": 0, "domains": 0}

    def add_with_v2_semantics(graph, triple):
        subject, predicate, obj = triple
        term = lookup.get(str(subject))
        if term and term.get("type") == "Property":
            kind = term.get("property_kind")
            if kind not in {"DatatypeProperty", "ObjectProperty"}:
                raise ValueError(f"Invalid v2 property_kind for {subject}: {kind!r}")
            if predicate == RDF.type and kind == "ObjectProperty" and obj == OWL.DatatypeProperty:
                original_add(graph, (subject, predicate, OWL.ObjectProperty))
                applied["object_property_types"] += 1
                obj = None
            if predicate == RDF.type and kind == "DatatypeProperty" and obj != OWL.DatatypeProperty:
                raise ValueError(f"Unexpected OWL type for DatatypeProperty {subject}: {obj}")
        if obj is not None:
            original_add(graph, triple)
        if term and predicate == RDF.type and term.get("type") == "Property":
            range_value = term.get("range", "")
            if range_value:
                original_add(graph, (subject, RDFS.range, _term_iri(range_value, terms)))
                applied["ranges"] += 1
            domain_value = term.get("domain", "")
            if domain_value:
                original_add(graph, (subject, RDFS.domain, _term_iri(domain_value, terms)))
                applied["domains"] += 1
        return graph

    Graph.add = add_with_v2_semantics
    return original_add, applied


def _message_bytes(args, kwargs) -> int:
    messages = kwargs.get("messages")
    if messages is None and args:
        messages = args[0]
    total = 0
    for message in messages or []:
        content = message.get("content", "") if isinstance(message, dict) else ""
        if isinstance(content, str):
            total += len(content.encode("utf-8"))
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and isinstance(block.get("text"), str):
                    total += len(block["text"].encode("utf-8"))
    return total


def _usage_tokens(response) -> int | None:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    if isinstance(usage, dict):
        value = usage.get("total_tokens")
    else:
        value = getattr(usage, "total_tokens", None)
        if value is None and hasattr(usage, "model_dump"):
            value = usage.model_dump().get("total_tokens")
    return int(value) if value is not None else None


def _prepare_llmatch_terms(terms: list[dict[str, Any]], destination: Path) -> Path:
    adapted = []
    for term in terms:
        if term.get("type") != "Property" or term.get("property_kind") != "DatatypeProperty":
            continue
        copy = dict(term)
        copy["type"] = "DatatypeProperty"
        annotation = f"OWL property kind: DatatypeProperty; range: {copy.get('range', '')}."
        copy["comment"] = (copy.get("comment", "") + " " + annotation).strip()
        adapted.append(copy)
    if len(adapted) != 32:
        raise ValueError(f"Expected the v2 32 DatatypeProperty field targets, got {len(adapted)}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps({"terms": adapted}, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    return destination


def run(method: str, database: Path, terms_path: Path, output_dir: Path, request_budget=None) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    raw_terms = json.loads(terms_path.read_text(encoding="utf-8"))
    terms = raw_terms.get("terms", raw_terms.get("target_terms", [])) if isinstance(raw_terms, dict) else raw_terms
    if not isinstance(terms, list) or not terms:
        raise ValueError("v2 target vocabulary is empty or invalid")
    if len({str(term["uri"]) for term in terms}) != len(terms):
        raise ValueError("Target term URIs must be unique")

    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{method}_{database.parent.name}.json"
    sys.path.insert(0, str((ROOT / "experiments/public_kg").resolve()))
    import external_methods as adapters

    from external_methods import RequestBudget
    request_budget = request_budget or RequestBudget(max_calls=MAX_REQUESTS,
        max_total_tokens=MAX_TOTAL_TOKENS, timeout_seconds=REQUEST_TIMEOUT_SECONDS)
    scope = f"{method}:{database.parent.name}"
    if method == "llmatch-predict":
        adapted_terms = _prepare_llmatch_terms(terms, output_dir / f"llmatch_{database.parent.name}_v2_target_terms.json")
        result = adapters.llmatch_predict(database, adapted_terms, output,
            request_budget=request_budget, request_scope=scope, max_llm_calls=MAX_REQUESTS,
            max_output_tokens=16_000, timeout_seconds=REQUEST_TIMEOUT_SECONDS)
        v2_record = {"field_matching_target_scope": "DatatypeProperty only", "target_term_count": 32}
    elif method == "llm4vkg-oc-mg":
        result = adapters.llm4vkg_oc_mg(database, terms_path, output, max_llm_calls=MAX_REQUESTS,
            request_budget=request_budget, request_scope=scope,
            max_total_tokens=MAX_TOTAL_TOKENS, timeout_seconds=REQUEST_TIMEOUT_SECONDS)
        v2_record = {"input_ontology_semantics": {
            "owl_type_counts": {
                "Class": sum(term.get("type") == "Class" for term in terms),
                "DatatypeProperty": sum(term.get("property_kind") == "DatatypeProperty" for term in terms),
                "ObjectProperty": sum(term.get("property_kind") == "ObjectProperty" for term in terms)},
            "declared_range_count": sum(bool(term.get("range")) for term in terms),
            "declared_domain_count": sum(bool(term.get("domain")) for term in terms)}}
    else:
        raise ValueError(f"Unsupported method: {method}")

    payload = result if isinstance(result, dict) else json.loads(output.read_text(encoding="utf-8"))
    raw_version = raw_terms.get("version") if isinstance(raw_terms, dict) else None
    payload["v2_vocabulary"] = {"path": str(terms_path), "term_count": len(terms),
                                "version": raw_version, **v2_record}
    payload["request_budget"] = request_budget.as_dict()
    payload["request_budget"]["scope"] = scope
    payload["request_budget"]["scope_request_count"] = request_budget.calls_by_scope.get(scope, 0)
    payload["full_pipeline_reproduction"] = False
    payload["quality_scored"] = False
    payload["quality_scoring_reason"] = "No independent human-reviewed gold mappings are available."
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def run_suite(terms_path: Path, output_dir: Path) -> dict:
    from external_methods import RequestBudget
    raw_terms = json.loads(terms_path.read_text(encoding="utf-8"))
    if raw_terms.get("version") != "public-spatial-v2-machine-reviewed":
        raise ValueError("v2-suite expects benchmark_v2 target terms")
    budget = RequestBudget(max_calls=MAX_REQUESTS, max_total_tokens=MAX_TOTAL_TOKENS,
                           timeout_seconds=REQUEST_TIMEOUT_SECONDS)
    run_specs = [("llmatch-predict", "foshan_osm"),
                 ("llmatch-predict", "geonames_cn"),
                 ("llm4vkg-oc-mg", "geonames_cn")]
    results = []
    db_root = ROOT / "data/public/benchmark_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    for method, dataset in run_specs:
        result = run(method, db_root / dataset / "sample.sqlite", terms_path, output_dir, budget)
        normalized_result = None
        if method == "llmatch-predict" and result.get("status") == "prediction_component_completed":
            from normalize_external_predictions import normalize
            normalized_result = normalize(output_dir / f"{method}_{dataset}.json",
                db_root / dataset / "sample.sqlite", terms_path,
                output_dir / "normalized" / f"llmatch_{dataset}", "LLMatch", "datatype_only")
        elif method == "llm4vkg-oc-mg" and result.get("outputs", {}).get("mapping_ttl"):
            from adapt_llm4vkg_sqlite_r2rml import adapt_and_validate
            mapping = output_dir / result["outputs"]["mapping_ttl"]
            adapt_and_validate(db_root / dataset / "sample.sqlite", mapping,
                output_dir / f"llm4vkg_{dataset}_generated_mapping_sqlite.ttl",
                output_dir / f"llm4vkg_{dataset}_sqlite_validation.json")
        results.append({"method": method, "dataset": dataset, "status": result.get("status"),
                        "scope_request_count": budget.calls_by_scope.get(f"{method}:{dataset}", 0),
                        "normalized": normalized_result is not None,
                        "output": f"{method}_{dataset}.json"})
        write_path = output_dir / "run_budget.json"
        write_path.write_text(json.dumps(budget.as_dict(), ensure_ascii=False, indent=2) + "\n",
                              encoding="utf-8")
        if budget.total_tokens_status != "known":
            break

    manifest = {"scope": "Public v2 external baseline pilot; not independent-gold or full reproduction",
        "vocabulary": {"path": str(terms_path), "version": raw_terms["version"],
                       "term_count": len(raw_terms["terms"])},
        "field_matching_target_scope": {"property_kind": "DatatypeProperty", "term_count": 32},
        "budget": budget.as_dict(), "runs": results,
        "limits": {"max_calls_per_method_database": MAX_REQUESTS,
                   "max_total_tokens": MAX_TOTAL_TOKENS, "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS},
        "full_pipeline_reproduction": False, "quality_scored": False,
        "human_gold": False,
        "limitations": ["LLMatch uses prediction component only; no table selection or expert branch",
                        "LLM4VKG OC/MG only on GeoNames; no mapping engine or graph materialization",
                        "No review/reference data were read"]}
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                                                encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("method", choices=["llmatch-predict", "llm4vkg-oc-mg", "v2-suite"])
    parser.add_argument("--db", type=Path)
    parser.add_argument("--terms", type=Path, default=DEFAULT_TERMS)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data/eval/public_kg/external_setup_v2")
    args = parser.parse_args()
    if args.method == "v2-suite":
        if args.db:
            parser.error("v2-suite chooses its benchmark databases; omit --db")
        payload = run_suite(args.terms.resolve(), args.out_dir.resolve())
        print(json.dumps({"runs": payload["runs"], "request_count": payload["budget"]["request_count"],
                          "total_tokens": payload["budget"]["total_tokens"],
                          "usage_status": payload["budget"]["total_tokens_status"]}, ensure_ascii=False))
        return
    if not args.db:
        parser.error("single method run requires --db")
    payload = run(args.method, args.db.resolve(), args.terms.resolve(), args.out_dir.resolve())
    print(json.dumps({"method": payload["method"], "status": payload["status"],
                      "request_budget": payload["request_budget"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
