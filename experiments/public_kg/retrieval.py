"""Build source-neutral Qwen/Chroma retrieval candidates for public KG experiments.

This module deliberately consumes only the frozen SQLite schema/data samples and
the public target vocabulary. It never loads annotation files or gold labels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_chroma import Chroma

from experiments.public_kg.methods import schema_profile
from rdb2g.retrieval.vector_store import (
    APIConnectionError, APITimeoutError, NotFoundError, QwenEmbeddings, RateLimitError,
)


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "data/public/benchmark_v1"
DEFAULT_OUT = ROOT / "data/eval/public_kg/retrieval_v1"
MODEL = os.getenv("QWEN_EMBEDDING_MODEL", "text-embedding-v4")
BATCH_SIZE = 10
TOP_K = 5


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ontology_documents(ontology_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    payload = json.loads(ontology_path.read_text(encoding="utf-8"))
    if payload.get("status") not in {"draft_for_review", "machine_reviewed_reference"}:
        raise ValueError("target ontology requires an explicit review status")
    terms = payload.get("terms")
    if not isinstance(terms, list) or not terms:
        raise ValueError("target ontology requires a non-empty term list")
    docs, ids = [], []
    seen = set()
    for index, term in enumerate(terms):
        uri = term.get("uri")
        if not isinstance(uri, str) or not uri or uri in seen:
            raise ValueError("ontology terms require unique non-empty URIs")
        seen.add(uri)
        fields = [
            f"Ontology {term.get('type', 'term')}: {term.get('label', '')}.",
            str(term.get("comment", "")),
            f"Domain: {term.get('domain', '')}.",
            f"Range: {term.get('range', '')}.",
        ]
        docs.append({"uri": uri, "text": " ".join(piece for piece in fields if piece).strip(),
                     "type": str(term.get("type", "")), "label": str(term.get("label", ""))})
        ids.append(f"term-{index:03d}-{hashlib.sha256(uri.encode()).hexdigest()[:12]}")
    return docs, ids


def build_schema_queries(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Create one table-class and one column-property retrieval query per item."""
    tasks = []
    for table in sorted(profile):
        table_data = profile[table]
        columns = table_data["columns"]
        names = [column["name"] for column in columns]
        sample_context = []
        for column in columns[:20]:
            vals = [str(value)[:80] for value in column.get("samples", [])[:2]]
            if vals:
                sample_context.append(f"{column['name']} sample values {', '.join(vals)}")
        table_text = (
            f"Database table named {table}. It contains {table_data['rows']} rows and "
            f"fields {', '.join(names)}. Example field values: {'; '.join(sample_context)}. "
            "Choose a semantic class for this geographic database entity."
        )
        tasks.append({"table": table, "column": None, "text": table_text})
        for column in columns:
            sample_values = [str(value)[:120] for value in column.get("samples", [])[:3]]
            query = (
                f"In database table {table}, field {column['name']} has declared type "
                f"{column.get('type') or 'unspecified'} and non-null rate "
                f"{column.get('nonnull_rate', 0.0):.4f}. Example values: "
                f"{'; '.join(sample_values) if sample_values else '(none)'}. "
                "Select a semantic property or geographic feature class describing this field."
            )
            tasks.append({"table": table, "column": column["name"], "text": query})
    return tasks


class MeteredQwenEmbeddings(QwenEmbeddings):
    """Project adapter with actual response usage and request/error accounting."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.requests = 0
        self.errors = 0
        self.tokens = {"input": 0, "output": 0, "total": 0, "reported_responses": 0}
        self.started = time.perf_counter()

    def _create_embedding(self, input_data):
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            self.requests += 1
            try:
                response = self.client.with_options(max_retries=0).embeddings.create(
                    model=self.model, input=input_data, timeout=self.timeout)
                break
            except NotFoundError as exc:
                self.errors += 1
                raise RuntimeError(
                    "Embedding model unavailable; check QWEN_EMBEDDING_MODEL configuration.") from exc
            except (APIConnectionError, APITimeoutError, RateLimitError) as exc:
                self.errors += 1
                last_exc = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 20))
            except Exception:
                self.errors += 1
                raise
        else:
            raise last_exc
        usage = getattr(response, "usage", None)
        if usage is not None:
            prompt_tokens = getattr(usage, "prompt_tokens", getattr(usage, "input_tokens", None))
            completion_tokens = getattr(usage, "completion_tokens", getattr(usage, "output_tokens", 0))
            total_tokens = getattr(usage, "total_tokens", None)
            if prompt_tokens is not None:
                self.tokens["input"] += int(prompt_tokens)
                self.tokens["reported_responses"] += 1
            if completion_tokens is not None:
                self.tokens["output"] += int(completion_tokens)
            if total_tokens is not None:
                self.tokens["total"] += int(total_tokens)
        return response

    def telemetry(self):
        return {"requests": self.requests, "errors": self.errors,
                "token_usage": dict(self.tokens),
                "elapsed_seconds": round(time.perf_counter() - self.started, 3)}


def _index_digest(model: str, docs: list[dict[str, Any]], vectors: list[list[float]]) -> str:
    digest = hashlib.sha256()
    digest.update(model.encode("utf-8"))
    for doc, vector in zip(docs, vectors):
        digest.update(doc["uri"].encode("utf-8"))
        digest.update(doc["text"].encode("utf-8"))
        digest.update(struct.pack(f"!{len(vector)}f", *vector))
    return digest.hexdigest()


def embed_in_batches(embedder, texts: list[str]) -> list[list[float]]:
    vectors = []
    for offset in range(0, len(texts), BATCH_SIZE):
        vectors.extend(embedder.embed_documents(texts[offset:offset + BATCH_SIZE]))
    return vectors


def _make_store(persist_dir: Path, docs: list[dict[str, Any]], ids: list[str],
                vectors: list[list[float]]) -> Chroma:
    persist_dir.mkdir(parents=True, exist_ok=True)
    store = Chroma(collection_name="public_spatial_targets_v1", persist_directory=str(persist_dir))
    store._collection.upsert(
        ids=ids,
        embeddings=vectors,
        documents=[doc["text"] for doc in docs],
        metadatas=[{"uri": doc["uri"], "type": doc["type"], "label": doc["label"]} for doc in docs],
    )
    return store


def _artifact_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    key = os.getenv("DASHSCOPE_API_KEY")
    return message.replace(key, "[redacted]") if key else message


def _write_candidates(path: Path, metadata: dict[str, Any], candidates: list[dict[str, Any]],
                      status: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"metadata": {**metadata, "status": status,
                            "candidate_count": len(candidates),
                            "candidate_sha256": hashlib.sha256(_json_bytes(candidates)).hexdigest()},
               "candidates": candidates}
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def prepare_index(ontology_path: Path, out_dir: Path, embedder: MeteredQwenEmbeddings):
    """Embed the target vocabulary once and persist its dedicated Chroma index."""
    ontology_path, out_dir = Path(ontology_path), Path(out_dir)
    ontology_hash = file_sha256(ontology_path)
    ontology, ids = ontology_documents(ontology_path)
    index_dir = out_dir / "chroma_index" / f"{embedder.model.replace('/', '_')}_{ontology_hash[:12]}"
    store = None
    if index_dir.exists():
        store = Chroma(collection_name="public_spatial_targets_v1", persist_directory=str(index_dir))
        indexed = store._collection.get(ids=ids, include=["embeddings", "documents", "metadatas"])
        reusable = len(indexed.get("ids", [])) == len(ids)
        if reusable:
            stored_docs = dict(zip(indexed["ids"], indexed["documents"]))
            stored_meta = dict(zip(indexed["ids"], indexed["metadatas"]))
            reusable = all(stored_docs.get(term_id) == doc["text"] and
                           stored_meta.get(term_id) == {"uri": doc["uri"], "type": doc["type"],
                                                        "label": doc["label"]}
                           for term_id, doc in zip(ids, ontology))
        if reusable:
            vectors_by_id = dict(zip(indexed["ids"], indexed["embeddings"]))
            vectors = [vectors_by_id[term_id].tolist() for term_id in ids]
        else:
            vectors = embed_in_batches(embedder, [item["text"] for item in ontology])
            if len(vectors) != len(ontology):
                raise RuntimeError("embedding service returned an unexpected ontology vector count")
            store = _make_store(index_dir, ontology, ids, vectors)
    else:
        vectors = embed_in_batches(embedder, [item["text"] for item in ontology])
        if len(vectors) != len(ontology):
            raise RuntimeError("embedding service returned an unexpected ontology vector count")
        store = _make_store(index_dir, ontology, ids, vectors)
    index_hash = _index_digest(embedder.model, ontology, vectors)
    return {"ontology": ontology, "ontology_hash": ontology_hash, "index_hash": index_hash,
            "index_dir": index_dir, "store": store, "reused_index": embedder.requests == 0}


def build_candidates(db_path: Path, ontology_path: Path, out_dir: Path, *,
                     embedder: MeteredQwenEmbeddings | None = None,
                     store_factory=None, index_context=None, progress=print) -> dict[str, Any]:
    """Build a per-source artifact, writing a valid partial checkpoint after batches."""
    started = time.perf_counter()
    db_path, ontology_path, out_dir = Path(db_path), Path(ontology_path), Path(out_dir)
    ontology_hash = file_sha256(ontology_path)
    profile = schema_profile(db_path)
    tasks = build_schema_queries(profile)
    if embedder is None:
        embedder = MeteredQwenEmbeddings()
    if store_factory is None:
        store_factory = _make_store

    # The project's QwenEmbeddings adapter reads this per-call batching setting.
    os.environ["QWEN_EMBEDDING_BATCH_SIZE"] = str(BATCH_SIZE)
    if index_context is None:
        if store_factory is None:
            store_factory = _make_store
        ontology, ids = ontology_documents(ontology_path)
        ontology_vectors = embed_in_batches(embedder, [item["text"] for item in ontology])
        if len(ontology_vectors) != len(ontology):
            raise RuntimeError("embedding service returned an unexpected ontology vector count")
        index_hash = _index_digest(embedder.model, ontology, ontology_vectors)
        index_dir = out_dir / "chroma_index" / f"{embedder.model.replace('/', '_')}_{ontology_hash[:12]}"
        store = store_factory(index_dir, ontology, ids, ontology_vectors)
    else:
        ontology = index_context["ontology"]
        index_hash = index_context["index_hash"]
        index_dir = index_context["index_dir"]
        store = index_context["store"]

    candidate_path = out_dir / "candidates.json"
    source_name = db_path.parent.name
    base_metadata = {
        "embedding_model": embedder.model,
        "ontology_sha256": ontology_hash,
        "index_artifact": _artifact_path(index_dir) + f"#sha256={index_hash}",
        "index_sha256": index_hash,
        "index_reused": bool(index_context.get("reused_index")) if index_context else False,
        "index_term_count": len(ontology),
        "source_database": source_name,
        "source_database_sha256": file_sha256(db_path),
        "query_count": len(tasks),
        "batch_size": BATCH_SIZE,
        "top_k": TOP_K,
        "result_type_filter": "Class for table tasks, Property for column tasks",
        "gold_accessed": False,
    }
    results: list[dict[str, Any]] = []
    # Persist a valid zero-candidate checkpoint before the long query phase.
    _write_candidates(candidate_path, {**base_metadata, "api": embedder.telemetry()}, results, "partial")
    try:
        for offset in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[offset:offset + BATCH_SIZE]
            vectors = embedder.embed_documents([task["text"] for task in batch])
            if len(vectors) != len(batch):
                raise RuntimeError("embedding service returned an unexpected query vector count")
            for task, vector in zip(batch, vectors):
                expected_type = "Class" if task["column"] is None else "Property"
                matches = store.similarity_search_by_vector(
                    vector, k=TOP_K, filter={"type": expected_type})
                uris = []
                for doc in matches:
                    uri = (doc.metadata or {}).get("uri")
                    if uri and uri not in uris:
                        uris.append(uri)
                results.append({"table": task["table"], "column": task["column"],
                                "candidate_uris": uris})
            state = "partial" if offset + len(batch) < len(tasks) else "success"
            metadata = {**base_metadata, "api": embedder.telemetry(),
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "completed_queries": len(results)}
            _write_candidates(candidate_path, metadata, results, state)
            progress(f"{source_name}: retrieval {len(results)}/{len(tasks)}; "
                     f"API requests={embedder.requests}, tokens={embedder.tokens['total']}")
    except Exception as exc:
        _write_candidates(candidate_path, {**base_metadata, "api": embedder.telemetry(),
                                           "elapsed_seconds": round(time.perf_counter() - started, 3),
                                           "completed_queries": len(results),
                                           "error_type": type(exc).__name__,
                                           "error": _safe_error(exc)}, results, "error")
        raise
    return json.loads(candidate_path.read_text(encoding="utf-8"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("foshan_osm", "geonames_cn", "all"), default="all")
    parser.add_argument("--benchmark-dir", type=Path, default=DATA_ROOT)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--ontology", type=Path, default=DATA_ROOT / "ontology/target_terms.json")
    args = parser.parse_args(argv)
    load_dotenv(ROOT / ".env")
    if not os.getenv("DASHSCOPE_API_KEY"):
        parser.error("DASHSCOPE_API_KEY is required; no embedding requests were sent")
    os.environ["QWEN_EMBEDDING_BATCH_SIZE"] = str(BATCH_SIZE)
    sources = ("foshan_osm", "geonames_cn") if args.source == "all" else (args.source,)
    embedder = MeteredQwenEmbeddings()
    run_started = time.perf_counter()
    try:
        index_context = prepare_index(args.ontology, args.out_dir, embedder)
    except Exception as exc:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        (args.out_dir / "retrieval_error.json").write_text(json.dumps(
            {"status": "error", "phase": "ontology_embedding_or_index",
             "error_type": type(exc).__name__, "error": _safe_error(exc),
             "api": embedder.telemetry()}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"target vocabulary embedding/index failed ({type(exc).__name__}); "
              "see retrieval_error.json", file=sys.stderr)
        return 1
    candidate_hashes = {}
    for source in sources:
        db_path = args.benchmark_dir / source / "sample.sqlite"
        out_dir = args.out_dir / source
        try:
            result = build_candidates(db_path, args.ontology, out_dir, embedder=embedder,
                                      index_context=index_context)
        except Exception as exc:
            # Keep failures explicit and local; never convert an API error to a result.
            report_path = out_dir / "retrieval_error.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps({"status": "error", "source": source,
                                               "error_type": type(exc).__name__,
                                               "error": _safe_error(exc), "api": embedder.telemetry()},
                                              ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(f"{source}: retrieval failed ({type(exc).__name__}); details: {report_path}",
                  file=sys.stderr)
            return 1
        candidate_hashes[source] = result["metadata"]["candidate_sha256"]
        print(f"{source}: {len(result['candidates'])} candidates written to "
              f"{out_dir / 'candidates.json'}")
    summary = {"status": "success", "embedding_model": embedder.model,
               "ontology_sha256": file_sha256(args.ontology),
               "index_sha256": index_context["index_hash"],
               "index_reused": index_context["reused_index"],
               "candidate_sha256": candidate_hashes,
               "api": embedder.telemetry(),
               "elapsed_seconds": round(time.perf_counter() - run_started, 3),
               "batch_size": BATCH_SIZE, "gold_accessed": False}
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "run_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
