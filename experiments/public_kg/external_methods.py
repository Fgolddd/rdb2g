"""Thin, explicitly labelled adapters to pinned upstream baseline implementations.

No adapter reads annotations. Prediction-only LLMatch invokes the upstream
column matcher, not its evaluation or expert-feedback (ground-truth) branch.
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BASELINES = ROOT / "data/public/baselines"


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


class RequestBudget:
    """Track real provider usage and stop before an unknown or over-budget call."""

    def __init__(self, max_calls=60, max_total_tokens=300_000, timeout_seconds=60):
        self.max_calls = max_calls
        self.max_total_tokens = max_total_tokens
        self.timeout_seconds = timeout_seconds
        self.request_count = 0
        self.total_tokens = 0
        self.total_tokens_status = "known"
        self.unknown_usage_requests = 0
        self.calls_by_scope = {}
        self.requests = []

    @staticmethod
    def _prompt_upper_bound(messages):
        total_bytes = sum(len(str(message.get("content", "")).encode("utf-8"))
                          for message in messages)
        return total_bytes + 128 * len(messages)

    @staticmethod
    def _usage(response):
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

    def chat(self, client, scope, model, messages, max_tokens, **kwargs):
        scope_count = self.calls_by_scope.get(scope, 0)
        if scope_count >= self.max_calls:
            raise RuntimeError(f"request cap reached for {scope}: {self.max_calls}")
        if self.total_tokens_status != "known":
            raise RuntimeError("total usage is unknown after an earlier failed request; refusing further calls")
        prompt_upper = self._prompt_upper_bound(messages)
        if self.total_tokens + prompt_upper + max_tokens > self.max_total_tokens:
            raise RuntimeError("next request may exceed the total token cap; request was not sent")

        self.request_count += 1
        self.calls_by_scope[scope] = scope_count + 1
        started = time.time()
        record = {"scope": scope, "attempt": self.calls_by_scope[scope],
                  "prompt_utf8_bytes_upper_bound": prompt_upper,
                  "max_output_tokens": max_tokens, "status": "in_flight"}
        self.requests.append(record)
        try:
            response = client.chat.completions.create(
                model=model, messages=messages, max_tokens=max_tokens,
                timeout=self.timeout_seconds, **kwargs)
        except Exception as exc:
            record.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}",
                           "usage_status": "unknown", "total_tokens": None,
                           "elapsed_seconds": round(time.time() - started, 3)})
            self.total_tokens_status = "unknown_after_failed_request"
            self.unknown_usage_requests += 1
            raise

        usage = self._usage(response)
        content = response.choices[0].message.content or ""
        record.update({"status": "response_received", "usage_status": "known" if usage is not None else "unknown",
                       "total_tokens": usage, "response_length_chars": len(content),
                       "elapsed_seconds": round(time.time() - started, 3),
                       "model": getattr(response, "model", model)})
        if usage is None:
            self.total_tokens_status = "unknown_after_missing_usage"
            self.unknown_usage_requests += 1
        else:
            self.total_tokens += usage
        return response, record

    def as_dict(self):
        scope_summaries = {}
        for request in self.requests:
            scope = request["scope"]
            summary = scope_summaries.setdefault(scope, {
                "request_count": 0, "successful_usage_tokens": 0,
                "unknown_usage_requests": 0, "status": "known"})
            summary["request_count"] += 1
            if request.get("total_tokens") is None:
                summary["unknown_usage_requests"] += 1
                summary["status"] = "unknown"
            else:
                summary["successful_usage_tokens"] += request["total_tokens"]
        return {"request_count": self.request_count, "total_tokens": self.total_tokens,
                "total_tokens_status": self.total_tokens_status,
                "unknown_usage_requests": self.unknown_usage_requests,
                "scope_summaries": scope_summaries,
                "max_requests_per_method_database": self.max_calls,
                "max_total_tokens": self.max_total_tokens,
                "timeout_seconds": self.timeout_seconds,
                "requests": self.requests}


def source_schema(db_path):
    result = {}
    with sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True) as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        for (table,) in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
            examples = conn.execute(f"SELECT * FROM {quoted} LIMIT 3").fetchall()
            result[table] = {
                "table_description": "Relational source table",
                "columns": {col[1]: {"name": col[1], "data_type": col[2],
                    "sample_values": [row[i] for row in examples]} for i, col in enumerate(columns)},
            }
    return result


def rel2graph_schema(db_paths, output):
    """Execute the author's real schema and FK/type normalization stage."""
    sys.path.insert(0, str(BASELINES / "Rel2Graph/rel_db2kg"))
    module = importlib.import_module("schema2graph")
    result = {}
    # Upstream keys databases by file stem; both benchmark files are sample.sqlite.
    for db_path in db_paths:
        path = Path(db_path).resolve()
        dataset = module.RelDBDataset([str(path)], logging.getLogger("rel2graph"))
        for tables in dataset.rel_dbs.values():
            result[str(path)] = {table: {"headers": obj.headers, "primary_keys": obj.pks,
                "foreign_keys": obj.fks, "rows": len(obj.rows), "compound_pk": obj.is_compound_pk}
                for table, obj in tables.items()}
    write_json(output, {"method": "Rel2Graph", "status": "schema_stage_only",
        "upstream_api": "schema2graph.RelDBDataset", "graph_materialized": False,
        "ontology_alignment": False, "databases": result})
    return result


def llmatch_predict(db_path, terms_path, output, model="qwen3.5-flash", *,
                    request_budget=None, request_scope=None, max_llm_calls=60,
                    max_total_tokens=300_000, timeout_seconds=60, max_output_tokens=16_000):
    """Invoke upstream prediction code; adapt only transport and cache storage."""
    from dotenv import load_dotenv
    from openai import OpenAI
    from cachelib import SimpleCache

    load_dotenv(ROOT / ".env")
    sys.path.insert(0, str(BASELINES / "LLMatch"))
    os.environ.setdefault("MONGODB_HOST", "mongodb://127.0.0.1:27017/unused_prediction_only")
    utils = importlib.import_module("schema_match.utils")
    utils.get_cache = lambda: SimpleCache(default_timeout=3600)
    matcher = importlib.import_module("schema_match.column_match.schema_understanding")
    terms = json.loads(Path(terms_path).read_text(encoding="utf-8"))
    if isinstance(terms, dict):
        terms = terms.get("terms", terms.get("target_terms", []))
    if not isinstance(terms, list) or not terms:
        raise ValueError("Target terms must be a nonempty list, or {'terms': [...]}.")
    target, id_to_uri = {}, {}
    for i, term in enumerate(terms):
        key = term["uri"].rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1]
        if not key or "." in key or key in target:
            raise ValueError("Target URI local names must be unique and contain no dots")
        id_to_uri[f"ontology.{key}"] = term["uri"]
        target[key] = {"name": term["label"], "description": term.get("comment", ""),
                       "domain": term.get("domain", ""), "data_type": term.get("type", "unknown")}
    target_schema = {"ontology": {"table_description": "Independent target vocabulary", "columns": target}}
    client = OpenAI(api_key=os.environ["DASHSCOPE_API_KEY"],
                    base_url=os.environ.get("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
                    timeout=timeout_seconds, max_retries=0)
    request_budget = request_budget or RequestBudget(max_calls=max_llm_calls,
        max_total_tokens=max_total_tokens, timeout_seconds=timeout_seconds)
    request_scope = request_scope or f"LLMatch:{Path(db_path).parent.name}"
    calls = []

    class Response:
        def __init__(self, value):
            self.value = value

        def json(self):
            return self.value

    def complete(prompt, model, run_specs, **kwargs):
        messages = [{"role": "user", "content": prompt}]
        response, request_record = request_budget.chat(client, request_scope, model, messages,
            max_tokens=max_output_tokens, extra_body={"enable_thinking": False},
            response_format={"type": "json_object"})
        content = response.choices[0].message.content or ""
        request_record["parse_status"] = "pending"
        try:
            parsed = json.loads(content)
        except Exception as exc:
            request_record["parse_status"] = "failed"
            request_record["parse_error"] = f"{type(exc).__name__}: {exc}"
            calls.append(request_record)
            raise
        request_record["parse_status"] = "success"
        calls.append(request_record)
        usage_payload = response.usage.model_dump() if response.usage and hasattr(response.usage, "model_dump") else None
        return Response({"extra": {"extracted_json": parsed}, "usage": usage_payload})

    matcher.complete = complete
    try:
        result = matcher.prompt_schema_matching({"column_matching_strategy": "llm",
            "column_matching_llm": model}, source_schema(db_path), target_schema)
    except Exception as exc:
        payload = {"method": "LLMatch", "status": "prediction_component_failed_or_partial",
            "upstream_api": "schema_match.column_match.schema_understanding.prompt_schema_matching",
            "source_db": str(db_path), "model": model,
            "adaptation": ["Mongo cache replaced by SimpleCache", "completion service replaced by OpenAI-compatible transport",
                           "target vocabulary represented as one schema table"],
            "full_pipeline_reproduction": False, "expert_feedback_used": False,
            "table_selection_used": False, "calls": calls,
            "error": f"{type(exc).__name__}: {exc}",
            "request_budget": request_budget.as_dict(), "predictions": [],
            "target_uri_lookup": id_to_uri}
        write_json(output, payload)
        return payload
    cleaned = result["extra"]["cleaned_json"]
    predictions = [{"source": source, "target_uri": id_to_uri[item["mapping"]],
                    "reasoning": item.get("reasoning", "")}
        for source, targets in cleaned.items() for item in targets if item["mapping"] in id_to_uri]
    payload = {"method": "LLMatch", "status": "prediction_component_completed",
        "upstream_api": "schema_match.column_match.schema_understanding.prompt_schema_matching",
        "adaptation": ["Mongo cache replaced by SimpleCache", "completion service replaced by OpenAI-compatible transport",
                       "target vocabulary represented as one schema table"],
        "full_pipeline_reproduction": False, "expert_feedback_used": False,
        "table_selection_used": False, "calls": calls, "raw_result": result,
        "predictions": predictions, "target_uri_lookup": id_to_uri,
        "request_budget": request_budget.as_dict()}
    write_json(output, payload)
    return payload


def llm4vkg_patterns(db_path, output):
    """Adapt SQLite PRAGMA schema to the upstream MPR constructor contract."""
    sys.path.insert(0, str(BASELINES / "LLM4VKG"))
    from src.vkg_generation.mapping_pattern_recognition import DBSchemaGraph

    schema = {}
    with sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True) as conn:
        for table in source_schema(db_path):
            quoted = '"' + table.replace('"', '""') + '"'
            columns = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
            fks = conn.execute(f"PRAGMA foreign_key_list({quoted})").fetchall()
            schema[table] = {"columns": [{"column_name": c[1], "data_type": c[2]} for c in columns],
                "primary_keys": [c[1] for c in columns if c[5]],
                "foreign_keys": [{"column_name": fk[3], "foreign_table": fk[2], "foreign_column": fk[4]}
                                 for fk in fks]}
    graph = DBSchemaGraph(table_info=schema)
    patterns = {kind: graph.recognize_mapping_pattern(kind) for kind in graph.mapping_pattern_types}
    write_json(output, {"method": "LLM4VKG", "status": "mapping_pattern_stage_completed",
        "upstream_api": "src.vkg_generation.mapping_pattern_recognition.DBSchemaGraph",
        "adaptation": "SQLite PRAGMA metadata replaces PostgreSQL introspection",
        "full_pipeline_reproduction": False, "ontology_completion_run": False,
        "mapping_generation_run": False, "patterns": patterns})
    return patterns


def llm4vkg_oc_mg(db_path, terms_path, output, model="qwen3.5-flash", max_llm_calls=60,
                  *, request_budget=None, request_scope=None, max_total_tokens=300_000,
                  timeout_seconds=60):
    """Run the upstream LLM4VKG OC and MG classes against one SQLite schema.

    This adapts source metadata input (PostgreSQL introspection to SQLite
    PRAGMA), model transport, and hard-coded CUDA device to CPU. Declared v2
    OWL property kinds, domains, and ranges are retained in the input ontology.
    """
    from dotenv import load_dotenv
    from openai import OpenAI
    from types import SimpleNamespace
    from typing import get_args

    load_dotenv(ROOT / ".env")
    if not os.environ.get("DASHSCOPE_API_KEY"):
        raise RuntimeError("DASHSCOPE_API_KEY is required for LLM4VKG OC/MG")
    if max_llm_calls <= 0:
        raise ValueError("max_llm_calls must be positive")

    upstream = BASELINES / "LLM4VKG"
    sys.path.insert(0, str(upstream))
    os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
    api_key = os.environ["DASHSCOPE_API_KEY"]
    client = OpenAI(api_key=api_key,
                    base_url=os.environ.get("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
                    timeout=timeout_seconds, max_retries=0)
    calls = []
    request_budget = request_budget or RequestBudget(max_calls=max_llm_calls,
        max_total_tokens=max_total_tokens, timeout_seconds=timeout_seconds)
    request_scope = request_scope or f"LLM4VKG:{Path(db_path).parent.name}"

    # The upstream API manager normally loads a tracked file containing remote
    # provider credentials. Replace only its lookup with a synthetic Qwen entry;
    # no stored provider credentials are read or used.
    from src.llm.utils.gpt_utils import API_Manager
    from src.llm.utils.langchain_utils import Modules, CustomLLM
    from pydantic import BaseModel

    def safe_api_load(manager):
        manager.apis = {model: SimpleNamespace(url=client.base_url, api_key=api_key)}
        return manager

    original_api_manager_load = API_Manager.load
    original_custom_llm_call = CustomLLM._call
    API_Manager.load = safe_api_load

    def qwen_call(llm, prompt, stop=None, **kwargs):
        # DashScope JSON mode requires the literal word "json" in a message;
        # the author pair-prediction template asks for structured output but
        # does not include that token.
        if "json" not in prompt.lower():
            prompt = prompt + "\nReturn only a valid JSON object matching the requested fields."
        response, request_record = request_budget.chat(client, request_scope, model,
            [{"role": "user", "content": prompt}], max_tokens=512, temperature=0,
            response_format={"type": "json_object"},
            extra_body={"enable_thinking": False})
        content = response.choices[0].message.content or "{}"
        try:
            parsed = json.loads(content)
            schema = getattr(llm, "response_format", None)
            if isinstance(schema, type) and issubclass(schema, BaseModel):
                # Some JSON-mode responses encode the null choice in a
                # Literal[...] field as the string "None". Normalize only
                # fields whose declared type explicitly permits None.
                model_fields = getattr(schema, "model_fields", {})
                for field_name, field in model_fields.items():
                    allowed_values = get_args(field.annotation)
                    if (parsed.get(field_name) == "None"
                            and (None in allowed_values or type(None) in allowed_values)):
                        parsed[field_name] = None
                parsed = schema.model_validate(parsed).model_dump()
        except Exception as exc:
            request_record["parse_status"] = "failed"
            request_record["parse_error"] = f"{type(exc).__name__}: {exc}"
            calls.append(request_record)
            raise
        request_record["parse_status"] = "success"
        calls.append({
            **request_record, "seconds": request_record["elapsed_seconds"],
            "usage": response.usage.model_dump() if response.usage and hasattr(response.usage, "model_dump") else None})
        return json.dumps(parsed, ensure_ascii=False)

    CustomLLM._call = qwen_call

    # LLM4VKG hardcodes device="cuda" in OntologyCompletion.__init__. Keep its
    # selected BAAI/bge-m3 encoder and computation, but direct it to CPU.
    from sentence_transformers import SentenceTransformer
    original_init = SentenceTransformer.__init__

    def cpu_sentence_transformer_init(instance, *args, **kwargs):
        kwargs["device"] = "cpu"
        return original_init(instance, *args, **kwargs)

    SentenceTransformer.__init__ = cpu_sentence_transformer_init

    old_cwd = Path.cwd()
    db_path = Path(db_path).resolve()
    terms_path = Path(terms_path).resolve()
    output_path = Path(output).resolve()
    try:
        os.chdir(upstream)
        terms = json.loads(Path(terms_path).read_text(encoding="utf-8"))
        terms = terms.get("terms", terms.get("target_terms", [])) if isinstance(terms, dict) else terms
        if not isinstance(terms, list) or not terms:
            raise ValueError("Target terms must be a nonempty list")

        # Construct an OWL input using the declared term IRIs and descriptions.
        # Generic Property is represented as DatatypeProperty because the source
        # vocabulary does not encode the OWL property subtype needed by LLM4VKG.
        from rdflib import Graph, URIRef, Literal, RDF, RDFS, OWL
        # config.py is deliberately gitignored in the upstream repository and
        # only supplies PostgreSQL credentials for its CLI; the adapted path
        # builds TableDataset from SQLite below.
        import types
        config_module = types.ModuleType("config")
        config_module.db_config = {}
        sys.modules.setdefault("config", config_module)
        from src.vkg_generation.VKGDatasets import OntologyDataset, TableDataset
        from src.vkg_generation.ontology_completion_mapping_generation import OntologyCompletion, MappingGeneration
        from src.vkg_generation.mapping_pattern_recognition import DBSchemaGraph
        from src.utils.tools import load_json_file

        input_ontology = output_path.with_name(f"llm4vkg_{db_path.parent.name}_input_ontology.ttl")
        graph = Graph()
        input_ontology_type_counts = {"Class": 0, "DatatypeProperty": 0, "ObjectProperty": 0}
        input_range_count = 0
        input_domain_count = 0
        for term in terms:
            iri = URIRef(term["uri"])
            term_type = term.get("type", "Property")
            if term_type == "Class":
                graph.add((iri, RDF.type, OWL.Class))
                input_ontology_type_counts["Class"] += 1
            elif term_type == "Property":
                property_kind = term.get("property_kind", "DatatypeProperty")
                if property_kind == "DatatypeProperty":
                    graph.add((iri, RDF.type, OWL.DatatypeProperty))
                    input_ontology_type_counts["DatatypeProperty"] += 1
                elif property_kind == "ObjectProperty":
                    graph.add((iri, RDF.type, OWL.ObjectProperty))
                    input_ontology_type_counts["ObjectProperty"] += 1
                else:
                    raise ValueError(f"Unsupported property_kind {property_kind!r} for {iri}")
                range_value = term.get("range", "")
                if range_value:
                    if range_value == "Literal":
                        range_iri = RDFS.Literal
                    elif range_value.startswith(("http://", "https://", "urn:")):
                        range_iri = URIRef(range_value)
                    else:
                        matches = [URIRef(item["uri"]) for item in terms if item.get("label") == range_value]
                        if len(matches) != 1:
                            raise ValueError(f"Cannot uniquely resolve declared range {range_value!r}")
                        range_iri = matches[0]
                    graph.add((iri, RDFS.range, range_iri))
                    input_range_count += 1
                domain_value = term.get("domain", "")
                if domain_value:
                    if domain_value.startswith(("http://", "https://", "urn:")):
                        domain_iri = URIRef(domain_value)
                    else:
                        matches = [URIRef(item["uri"]) for item in terms if item.get("label") == domain_value]
                        if len(matches) != 1:
                            raise ValueError(f"Cannot uniquely resolve declared domain {domain_value!r}")
                        domain_iri = matches[0]
                    graph.add((iri, RDFS.domain, domain_iri))
                    input_domain_count += 1
            else:
                raise ValueError(f"Unsupported v2 term type {term_type!r} for {iri}")
            if term.get("label"):
                graph.add((iri, RDFS.label, Literal(term["label"], lang="en")))
            if term.get("comment"):
                graph.add((iri, RDFS.comment, Literal(term["comment"], lang="en")))
        input_ontology.parent.mkdir(parents=True, exist_ok=True)
        graph.serialize(destination=str(input_ontology), format="turtle")

        table_info = {}
        schema = {}
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            table_names = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            for table_name in table_names:
                quoted = '"' + table_name.replace('"', '""') + '"'
                columns = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
                fks = conn.execute(f"PRAGMA foreign_key_list({quoted})").fetchall()
                pks = [col[1] for col in columns if col[5]]
                table_info[table_name] = {
                    "table_name": table_name,
                    "columns": [{"column_name": col[1], "data_type": col[2] or "string",
                                 "is_primary_key": bool(col[5])} for col in columns],
                    "primary_keys": pks,
                    "foreign_keys": [{"column_name": fk[3], "foreign_table": fk[2],
                                      "foreign_column": fk[4], "constraint_name": f"fk_{fk[0]}"}
                                     for fk in fks],
                }
                schema[table_name] = {
                    "columns": [{"column_name": col[1], "data_type": col[2] or "string"} for col in columns],
                    "primary_keys": pks,
                    "foreign_keys": [{"column_name": fk[3], "foreign_table": fk[2],
                                      "foreign_column": fk[4]} for fk in fks],
                }

        pattern_graph = DBSchemaGraph(table_info=schema)
        patterns = {kind: pattern_graph.recognize_mapping_pattern(kind)
                    for kind in pattern_graph.mapping_pattern_types}
        ontology = OntologyDataset(str(input_ontology))
        table = TableDataset(table_info)
        strategy = load_json_file("setting/strategy/OntologyCompletion/LLM4VKG.json")
        prompts = load_json_file("prompts/OntologyCompletion/prompt.json")
        modules = Modules(strategy, prompts, set_all_api_id=model)

        started = time.time()
        oc = OntologyCompletion(modules=modules, db_schema=db_path.stem)
        completed_ontology, db2o = oc.run_ontology_completion(ontology, table)
        original_ontology = OntologyDataset(str(input_ontology))
        mg = MappingGeneration(modules=modules, db_schema=db_path.stem)
        mg.run_mapping_generation(table=table, db2o=db2o, ontology=completed_ontology,
                                  mapping_patterns=patterns, original_ontology=original_ontology)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        ontology_path = output_path.with_name(f"llm4vkg_{db_path.parent.name}_generated_ontology.ttl")
        mapping_path = output_path.with_name(f"llm4vkg_{db_path.parent.name}_generated_mapping.ttl")
        original_ontology.graph.serialize(destination=str(ontology_path), format="turtle")
        mg.mapping.serialize(destination=str(mapping_path), format="turtle")
        prediction = {
            source: {"target_uri": str(value[0]), "top_ancestor_uri": str(value[1])}
            for source, value in db2o.items()
        }
        payload = {
            "method": "LLM4VKG", "status": "ontology_completion_and_mapping_generation_completed",
            "upstream_api": ["OntologyCompletion.run_ontology_completion", "MappingGeneration.run_mapping_generation"],
            "model": model, "embedding_model": "BAAI/bge-m3", "embedding_device": "cpu",
            "source_db": str(db_path), "source_schema": "SQLite PRAGMA adapter",
            "ontology_input": str(input_ontology),
            "property_type_adaptation": "v2 property_kind was used to declare owl:DatatypeProperty or owl:ObjectProperty; declared ranges and domains were copied to RDFS range/domain.",
            "input_ontology_semantics": {"owl_type_counts": input_ontology_type_counts,
                                         "declared_range_count": input_range_count,
                                         "declared_domain_count": input_domain_count},
            "mapping_patterns": {key: len(value) for key, value in patterns.items()},
            "api_calls": calls, "api_call_count": len(calls), "api_call_limit": max_llm_calls,
            "request_budget": request_budget.as_dict(),
            "elapsed_seconds": round(time.time() - started, 3),
            "predicted_source_concepts": prediction,
            "outputs": {"ontology_ttl": ontology_path.name, "mapping_ttl": mapping_path.name},
            "mapping_sql_dialect": "Upstream R2RML templates use PostgreSQL schema-qualified SELECTs; not directly executable against the SQLite source.",
            "full_pipeline_reproduction": False, "quality_scored": False,
            "quality_scoring_reason": "No independent gold mappings are available.",
        }
        write_json(output_path, payload)
        return payload
    finally:
        os.chdir(old_cwd)
        SentenceTransformer.__init__ = original_init
        API_Manager.load = original_api_manager_load
        CustomLLM._call = original_custom_llm_call


def magneto_predict(db_path, terms_path, output):
    """Run upstream base Magneto (MiniLM retrieval and bipartite reranking)."""
    import pandas as pd
    os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    sys.path.insert(0, str(BASELINES / "Magneto/algorithms/magneto"))
    from magneto import Magneto

    frames = []
    with sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True) as conn:
        for table in source_schema(db_path):
            quoted = '"' + table.replace('"', '""') + '"'
            frame = pd.read_sql_query(f"SELECT * FROM {quoted}", conn)
            frame.columns = [f"{table}.{col}" for col in frame.columns]
            frames.append(frame)
    source = pd.concat(frames, axis=1)
    terms = json.loads(Path(terms_path).read_text(encoding="utf-8"))
    if isinstance(terms, dict):
        terms = terms["terms"]
    lookup = {term["uri"].rsplit("/", 1)[-1]: term["uri"] for term in terms}
    target = pd.DataFrame({term["uri"].rsplit("/", 1)[-1]:
        [term.get("comment", ""), term.get("domain", ""), term["label"]] for term in terms})
    started = time.time()
    matcher = Magneto(embedding_model="minilm", use_gpt_reranker=False,
                      topk=20, sampling_size=10)
    matches = matcher.get_matches(source, target)
    predictions = [{"source": key[0][1], "target_uri": lookup[key[1][1]], "score": float(score)}
                   for key, score in matches.items()]
    write_json(output, {"method": "Magneto", "status": "base_method_completed",
        "upstream_api": "magneto.Magneto.get_matches", "model": "sentence-transformers/all-MiniLM-L6-v2",
        "llm_reranking_used": False, "bipartite_reranking_used": True,
        "adaptation": "Source tables concatenated as qualified columns; target descriptions serve as values",
        "seconds": time.time() - started, "source_columns": len(source.columns),
        "target_columns": len(target.columns), "predictions": predictions})
    return predictions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("method", choices=["rel2graph-schema", "llmatch-predict", "llm4vkg-patterns", "llm4vkg-oc-mg", "magneto-predict"])
    parser.add_argument("--db", action="append", required=True)
    parser.add_argument("--target-terms")
    parser.add_argument("--max-llm-calls", type=int, default=100)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.method == "rel2graph-schema":
        rel2graph_schema(args.db, args.out)
    elif args.method == "llm4vkg-patterns":
        if len(args.db) != 1:
            parser.error("llm4vkg-patterns requires one --db")
        llm4vkg_patterns(args.db[0], args.out)
    elif args.method == "llm4vkg-oc-mg":
        if len(args.db) != 1 or not args.target_terms:
            parser.error("llm4vkg-oc-mg requires one --db and --target-terms")
        llm4vkg_oc_mg(args.db[0], args.target_terms, args.out, max_llm_calls=args.max_llm_calls)
    else:
        if len(args.db) != 1 or not args.target_terms:
            parser.error("llmatch-predict requires one --db and --target-terms")
        if args.method == "magneto-predict":
            magneto_predict(args.db[0], args.target_terms, args.out)
        else:
            llmatch_predict(args.db[0], args.target_terms, args.out)


if __name__ == "__main__":
    main()
