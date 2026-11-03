import os
import json
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

from rdb2g.common.ignored_columns import is_ignored_rag_column
from rdb2g.mapping.chat_client import QwenChatClient
from rdb2g.mapping.json_utils import parse_json_for_debug, parse_json_output, summarize_parsed_json


class MultiAgentSystem:
    def __init__(self, vector_store=None, allow_public_uri=False, local_uri_base="http://example.org/auto/"):
        self.chat_client = QwenChatClient()
        self.chat_model = self.chat_client.model
        self.chat_timeout = self.chat_client.timeout
        self.chat_max_retries = self.chat_client.max_retries
        self.enable_thinking = self.chat_client.enable_thinking
        self.vector_store = vector_store
        self.debug_rag = os.getenv("DEBUG_RAG_RESULTS", "0") == "1"
        self.debug_chat = os.getenv("DEBUG_CHAT_RESULTS", "0") == "1"
        self.debug_chat_log_request = os.getenv("DEBUG_CHAT_LOG_REQUEST", "0") == "1"
        self.debug_chat_log_dir = Path(os.getenv("DEBUG_CHAT_LOG_DIR", "data/chat_logs"))
        self.allow_public_uri = allow_public_uri
        self.local_uri_base = local_uri_base.rstrip("/") + "/"
        self.public_uri_prefixes = (
            "http://schema.org",
            "https://schema.org",
            "http://www.w3.org",
            "https://www.w3.org",
            "http://www.opengis.net",
            "https://www.opengis.net",
        )
        self.rag_candidates_cache = {}

    def _safe_log_name(self, value):
        return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(value or "unknown"))

    def _chat_log_path(self, table_name, stage):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{self._safe_log_name(table_name)}_{self._safe_log_name(stage)}_{timestamp}.json"
        return self.debug_chat_log_dir / filename

    def _summarize_parsed_json(self, parsed):
        return summarize_parsed_json(parsed)

    def _parse_json_for_debug(self, content):
        return parse_json_for_debug(content)

    def _write_chat_debug_log(self, table_name, stage, messages, content=None, elapsed=None, error=None, parse_info=None):
        if not self.debug_chat:
            return

        payload = {
            "stage": stage,
            "table": table_name,
            "model": self.chat_model,
            "timeout": self.chat_timeout,
            "max_retries": self.chat_max_retries,
            "enable_thinking": self.enable_thinking,
            "elapsed_seconds": round(float(elapsed), 3) if elapsed is not None else None,
            "request": {
                "message_count": len(messages or []),
                "messages": messages if self.debug_chat_log_request else None,
            },
            "response": {
                "content": content,
                "content_length": len(content) if isinstance(content, str) else None,
            },
            "parse": parse_info or (self._parse_json_for_debug(content) if content is not None else None),
            "error": None,
        }
        if error is not None:
            payload["error"] = {
                "type": type(error).__name__,
                "message": str(error),
            }

        self.debug_chat_log_dir.mkdir(parents=True, exist_ok=True)
        path = self._chat_log_path(table_name, stage)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[{table_name}] {stage} Chat 调试日志: {path}")

    def _is_retrieval_enabled(self):
        return bool(self.vector_store is not None and getattr(self.vector_store, "vector_db", None) is not None)

    def _build_local_uri(self, column_name):
        safe = urllib.parse.quote(str(column_name).strip())
        return f"{self.local_uri_base}{safe}"

    def _parse_json_output(self, content, fallback):
        return parse_json_output(content, fallback)

    def _normalize_term_candidate(self, item):
        if not isinstance(item, dict):
            return None
        uri = str(item.get("uri", "")).strip()
        if not uri:
            return None
        return {
            "uri": uri,
            "type": str(item.get("type", "")).strip(),
            "label": str(item.get("label", "")).strip(),
            "comment": str(item.get("comment", "")).strip(),
            "domain": str(item.get("domain", "")).strip(),
            "range": str(item.get("range", "")).strip(),
            "role": str(item.get("role", "")).strip(),
            "priority": str(item.get("priority", "")).strip(),
            "canonical_concept_id": str(item.get("canonical_concept_id", "")).strip(),
        }

    def _normalize_candidate_list(self, candidates):
        if not isinstance(candidates, list):
            return []
        normalized = []
        seen = set()
        for item in candidates:
            term = self._normalize_term_candidate(item)
            if not term:
                continue
            uri = term["uri"]
            if uri in seen:
                continue
            seen.add(uri)
            normalized.append(term)
        return normalized

    def _uri_tail(self, uri):
        text = str(uri or "").strip()
        if not text:
            return ""
        text = text.rsplit("#", 1)[-1]
        return text.rsplit("/", 1)[-1]

    def _candidate_matches_table_column(self, term, table_name, column_name):
        table = str(table_name or "").strip()
        column = str(column_name or "").strip()
        if not isinstance(term, dict) or not table or not column:
            return False

        domain = str(term.get("domain", "")).strip()
        if domain and domain != table:
            return False

        column_code = str(term.get("column_code", "")).strip()
        if column_code and column_code != column:
            return False

        tail = self._uri_tail(term.get("uri"))
        if "." not in tail:
            return False
        uri_table, uri_column = tail.rsplit(".", 1)
        return uri_table == table and uri_column == column

    def _strict_candidates_for_column(self, candidates, table_name, column_name):
        return [
            term
            for term in self._normalize_candidate_list(candidates)
            if self._candidate_matches_table_column(term, table_name, column_name)
        ]

    def _strict_rag_candidates(self, table_fingerprint, rag_candidates):
        table_name = table_fingerprint.get("table_name", "")
        strict = {}
        for col in table_fingerprint.get("columns", []):
            if not isinstance(col, dict):
                continue
            col_name = col.get("name")
            strict[col_name] = self._strict_candidates_for_column(
                (rag_candidates or {}).get(col_name, []),
                table_name,
                col_name,
            )
        return strict

    def _mapping_value_matches_table_column(self, value, table_name, column_name):
        if value is None:
            return True
        if not isinstance(value, dict):
            return False
        return self._candidate_matches_table_column(value, table_name, column_name)

    def _sanitize_mapping_to_table(self, table_fingerprint, mapping):
        table_name = table_fingerprint.get("table_name", "")
        sanitized = {}
        for col in table_fingerprint.get("columns", []):
            if not isinstance(col, dict):
                continue
            col_name = col.get("name")
            value = (mapping or {}).get(col_name)
            sanitized[col_name] = value if self._mapping_value_matches_table_column(value, table_name, col_name) else None
        return sanitized

    def _to_float_or_none(self, value):
        try:
            if value is None or value == "":
                return None
            num = float(value)
            if num < 0:
                return 0.0
            if num > 1:
                return 1.0
            return num
        except Exception:
            return None

    def _pick_term_from_candidates(self, value, candidates):
        if not candidates:
            return None

        selected_uri = None
        reason = ""
        confidence = None

        if isinstance(value, dict):
            selected_uri = str(value.get("uri", "")).strip()
            reason = str(value.get("reason", "")).strip()
            confidence = self._to_float_or_none(value.get("confidence"))
        elif isinstance(value, str):
            selected_uri = value.strip()

        picked = None
        if selected_uri:
            for c in candidates:
                if c["uri"] == selected_uri:
                    picked = c
                    break
        if picked is None:
            picked = candidates[0]
            if not reason:
                reason = "fallback_to_top_candidate"

        payload = dict(picked)
        if reason:
            payload["reason"] = reason
        if confidence is not None:
            payload["confidence"] = confidence
        return payload

    def _compact_columns_for_mapping(self, table_fingerprint, sample_limit=2, sample_chars=50):
        compact = []
        for col in table_fingerprint.get("columns", []):
            if not isinstance(col, dict):
                continue
            samples = []
            for sample in col.get("samples", [])[:sample_limit]:
                text = str(sample).strip()
                if len(text) > sample_chars:
                    text = text[:sample_chars] + "..."
                samples.append(text)
            compact.append({
                "name": col.get("name"),
                "dtype": col.get("dtype"),
                "unique_count": col.get("unique_count"),
                "null_ratio": col.get("null_ratio"),
                "samples": samples,
            })
        return {
            "table": table_fingerprint.get("table_name"),
            "row_count": table_fingerprint.get("row_count"),
            "columns": compact,
        }

    def _compact_columns_for_relation(self, table_fingerprint):
        compact = []
        for col in table_fingerprint.get("columns", []):
            if not isinstance(col, dict):
                continue
            compact.append({
                "name": col.get("name"),
                "unique_count": col.get("unique_count"),
                "null_ratio": col.get("null_ratio"),
            })
        return {
            "table": table_fingerprint.get("table_name"),
            "row_count": table_fingerprint.get("row_count"),
            "columns": compact,
            "all_columns": table_fingerprint.get("all_columns", []),
            "explicit_pk": table_fingerprint.get("explicit_pk", []),
            "explicit_fks": table_fingerprint.get("explicit_fks", []),
            "table_count": table_fingerprint.get("table_count", 0),
        }

    def _compact_rag_candidates(self, rag_candidates, top_k=3):
        compact = {}
        for col, candidates in (rag_candidates or {}).items():
            compact[col] = []
            for idx, term in enumerate(self._normalize_candidate_list(candidates)[:top_k], start=1):
                compact[col].append({
                    "id": idx,
                    "uri": term.get("uri"),
                    "label": term.get("label"),
                    "role": term.get("role"),
                    "priority": term.get("priority"),
                    "domain": term.get("domain"),
                })
        return compact

    def _pick_term_by_candidate_id(self, value, candidates):
        normalized = self._normalize_candidate_list(candidates)
        if not normalized:
            return None
        if value is None:
            return None

        reason = "selected_by_candidate_id"
        candidate_id = None
        if isinstance(value, dict):
            if value.get("uri"):
                return self._pick_term_from_candidates(value, normalized)
            reason = str(value.get("reason") or reason).strip()
            value = value.get("id", value.get("candidate_id", value.get("choice")))
        if isinstance(value, int):
            candidate_id = value
        elif isinstance(value, str):
            text = value.strip()
            if not text or text.lower() == "null":
                return None
            if text.isdigit():
                candidate_id = int(text)

        if candidate_id is None or candidate_id < 1 or candidate_id > len(normalized):
            return None

        payload = dict(normalized[candidate_id - 1])
        payload["reason"] = reason
        return payload

    def _normalize_candidate_id_mapping(self, mapping, table_fingerprint, allowed_terms_by_column):
        if not isinstance(mapping, dict):
            return {}
        normalized = {}
        table_name = table_fingerprint.get("table_name", "")
        columns = [c.get("name") for c in table_fingerprint.get("columns", []) if isinstance(c, dict)]
        for col in columns:
            normalized[col] = self._pick_term_by_candidate_id(
                mapping.get(col),
                self._strict_candidates_for_column((allowed_terms_by_column or {}).get(col, []), table_name, col),
            )
        return normalized

    def _find_suspicious_mapping_fields(self, table_name, mapping, rag_candidates, max_fields=8):
        suspicious = {}
        table_name = str(table_name or "").strip()
        for col, value in (mapping or {}).items():
            if len(suspicious) >= max_fields:
                break
            reasons = []
            if value is None:
                reasons.append("null_mapping")
            elif isinstance(value, dict):
                uri = str(value.get("uri", "")).strip()
                domain = str(value.get("domain", "")).strip()
                if table_name and uri and f"/zhongshan/{table_name}." not in uri:
                    reasons.append("uri_table_mismatch")
                if table_name and domain and domain != table_name:
                    reasons.append("domain_mismatch")
                if str(value.get("reason", "")).strip() == "fallback_to_top_candidate":
                    reasons.append("fallback_mapping")
            else:
                reasons.append("invalid_mapping_type")
            if reasons:
                strict_candidates = self._strict_candidates_for_column((rag_candidates or {}).get(col, []), table_name, col)
                suspicious[col] = {
                    "reasons": reasons,
                    "selected": value,
                    "candidates": self._compact_rag_candidates({col: strict_candidates}).get(col, []),
                }
        return suspicious

    def _normalize_mapping_output(self, mapping, table_fingerprint, allowed_terms_by_column=None):
        """无知识库场景下，按策略过滤公共 URI，避免误用公共本体"""
        if not isinstance(mapping, dict):
            return {}

        columns = [c.get("name") for c in table_fingerprint.get("columns", []) if isinstance(c, dict)]

        if self._is_retrieval_enabled() and allowed_terms_by_column:
            normalized = {}
            for col in columns:
                table_name = table_fingerprint.get("table_name", "")
                allowed = self._strict_candidates_for_column(allowed_terms_by_column.get(col, []), table_name, col)
                value = mapping.get(col)

                if not allowed:
                    normalized[col] = None
                    continue

                normalized[col] = self._pick_term_from_candidates(value, allowed)
            return normalized

        if self._is_retrieval_enabled() or self.allow_public_uri:
            return mapping

        normalized = {}
        for col in columns:
            value = mapping.get(col)
            if value is None:
                normalized[col] = None
                continue

            if isinstance(value, dict):
                value = value.get("uri")

            if not isinstance(value, str):
                normalized[col] = self._build_local_uri(col)
                continue

            value = value.strip()
            if not value or value.lower() == "null":
                normalized[col] = None
                continue

            is_public = any(value.startswith(prefix) for prefix in self.public_uri_prefixes)
            is_http_uri = value.startswith("http://") or value.startswith("https://")
            if is_public or not is_http_uri:
                normalized[col] = self._build_local_uri(col)
            else:
                normalized[col] = value

        return normalized

    def _doc_meta(self, doc):
        meta = {}
        if hasattr(doc, "metadata") and isinstance(doc.metadata, dict):
            meta.update(doc.metadata)
        payload = {}
        raw_content = str(getattr(doc, "page_content", "")).strip()
        if raw_content:
            try:
                parsed = json.loads(raw_content)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}

        uri = str(meta.get("uri") or payload.get("uri", "")).strip()
        domain = str(meta.get("domain") or payload.get("domain", "")).strip()
        column_code = str(meta.get("column_code", "")).strip()
        label = str(meta.get("label") or payload.get("label", "")).strip()
        term_type = str(meta.get("type") or payload.get("type", "")).strip()
        range_val = str(meta.get("range") or payload.get("range", "")).strip()
        comment = str(meta.get("comment") or payload.get("comment", "")).strip()
        role = str(meta.get("role") or payload.get("role", "")).strip()
        priority = str(meta.get("priority") or payload.get("priority", "")).strip()
        canonical_concept_id = str(
            meta.get("canonical_concept_id") or payload.get("canonical_concept_id", "")
        ).strip()
        snippet = raw_content.replace("\n", " ")[:180]
        return {
            "uri": uri,
            "domain": domain,
            "column_code": column_code,
            "label": label,
            "type": term_type,
            "range": range_val,
            "comment": comment,
            "role": role,
            "priority": priority,
            "canonical_concept_id": canonical_concept_id,
            "snippet": snippet,
        }

    def _priority_weight(self, priority):
        value = str(priority or "").strip().upper()
        if value == "P0":
            return 30
        if value == "P1":
            return 10
        if value == "P2":
            return 0
        return 0

    def _role_weight(self, role):
        value = str(role or "").strip().lower()
        if value == "semantic_fk":
            return 25
        if value == "business_key":
            return 20
        if value == "entity_name":
            return 15
        return 0

    def _rerank_rag_candidates(self, docs, column_name, top_k=5):
        if not docs:
            return []

        col_name = str(column_name or "").strip().lower()
        scored = []
        for idx, doc in enumerate(docs):
            m = self._doc_meta(doc)
            if not m.get("uri"):
                continue
            score = self._priority_weight(m.get("priority")) + self._role_weight(m.get("role"))
            if str(m.get("column_code", "")).strip().lower() == col_name:
                score += 8
            scored.append((score, idx, m))

        scored.sort(key=lambda x: (-x[0], x[1]))

        selected = []
        seen_uri = set()
        seen_concepts = set()
        for _, _, m in scored:
            uri = m["uri"]
            if uri in seen_uri:
                continue

            concept = str(m.get("canonical_concept_id", "")).strip().lower()
            if concept and concept in seen_concepts:
                continue

            seen_uri.add(uri)
            if concept:
                seen_concepts.add(concept)
            selected.append(m)
            if len(selected) >= max(int(top_k), 1):
                break
        return selected

    def _has_cross_table_evidence(self, column_name, table_fingerprint):
        table_name = str(table_fingerprint.get("table_name", "")).lower()
        all_tables = [str(t).lower() for t in table_fingerprint.get("all_tables", [])]
        other_tables = [t for t in all_tables if t != table_name]

        col = str(column_name).lower()
        for suffix in ("_id", "_fk", "id", "fk"):
            if col.endswith(suffix):
                col = col[:-len(suffix)]
                break
        col = col.strip("_")
        if not col:
            return False

        def singular(s):
            return s[:-1] if s.endswith("s") and len(s) > 1 else s

        col_s = singular(col)
        for t in other_tables:
            t_s = singular(t)
            if col == t or col == t_s or col_s == t or col_s == t_s:
                return True
        return False

    def _normalize_relation_output(self, table_fingerprint, relations):
        """约束优先 + 单表防误判 + 多表受控推断"""
        all_columns = table_fingerprint.get("all_columns") or []
        columns = {str(c).strip() for c in all_columns if str(c).strip()}
        if not columns:
            columns = {c.get("name") for c in table_fingerprint.get("columns", []) if isinstance(c, dict)}
        explicit_pk = [c for c in table_fingerprint.get("explicit_pk", []) if c in columns]
        explicit_fks = [c for c in table_fingerprint.get("explicit_fks", []) if c in columns]
        table_count = int(table_fingerprint.get("table_count", 0) or 0)

        pk = None
        fks = []

        if isinstance(relations, dict):
            raw_pk = relations.get("pk")
            raw_fks = relations.get("fks", [])

            if isinstance(raw_pk, str) and raw_pk in columns:
                pk = raw_pk
            elif isinstance(raw_pk, list):
                candidate = [c for c in raw_pk if isinstance(c, str) and c in columns]
                if candidate:
                    pk = candidate

            if isinstance(raw_fks, list):
                fks = [c for c in raw_fks if isinstance(c, str) and c in columns]
            elif isinstance(raw_fks, str) and raw_fks in columns:
                fks = [raw_fks]

        if explicit_pk:
            pk = explicit_pk[0] if len(explicit_pk) == 1 else explicit_pk

        if explicit_fks:
            fks = explicit_fks
        elif table_count <= 1:
            fks = []
        else:
            fks = [c for c in fks if self._has_cross_table_evidence(c, table_fingerprint)]

        dedup_fks = []
        for c in fks:
            if c not in dedup_fks:
                dedup_fks.append(c)

        return {"pk": pk, "fks": dedup_fks}

    def _chat(self, messages, max_retries=None):
        return self.chat_client.complete(messages, max_retries=max_retries)

    def _fallback_mapping_from_rag(self, table_fingerprint, rag_candidates):
        fallback = {}
        table_name = table_fingerprint.get("table_name", "")
        for col in table_fingerprint.get("columns", []):
            if not isinstance(col, dict):
                continue
            col_name = col.get("name")
            candidates = self._strict_candidates_for_column(rag_candidates.get(col_name, []), table_name, col_name)
            fallback[col_name] = candidates[0] if candidates else None
        return self._normalize_mapping_output(
            fallback,
            table_fingerprint,
            allowed_terms_by_column=rag_candidates,
        )

    def _fallback_relations(self, table_fingerprint):
        all_columns = table_fingerprint.get("all_columns") or []
        raw_relations = {
            "pk": None,
            "fks": table_fingerprint.get("explicit_fks", []),
        }
        explicit_pk = table_fingerprint.get("explicit_pk", [])
        if explicit_pk:
            raw_relations["pk"] = explicit_pk[0] if len(explicit_pk) == 1 else explicit_pk
        else:
            gid_col = next((c for c in all_columns if str(c).strip().lower() == "gid"), None)
            if gid_col:
                raw_relations["pk"] = gid_col
        return self._normalize_relation_output(table_fingerprint, raw_relations)

    def _get_rag_context(self, table_fingerprint):
        """为表中的每一列检索 RAG 上下文"""
        if self.vector_store is None or getattr(self.vector_store, "vector_db", None) is None:
            return "", {}

        started = time.perf_counter()
        context = ""
        rag_candidates = {}
        table_data = table_fingerprint
        table_name = str(table_data.get("table_name", ""))
        columns = table_data.get('columns', [])
        print(f"[{table_name}] RAG 检索开始，列数={len(columns)}")
        for col_index, col in enumerate(columns, start=1):
            # 检索与 列名+样本 相关的术语
            col_name = col["name"]
            if is_ignored_rag_column(col_name):
                rag_candidates[col_name] = []
                continue
            raw_samples = col.get('samples', [])[:3]
            clipped_samples = []
            for s in raw_samples:
                s = str(s).strip()
                if len(s) > 120:
                    s = s[:120] + "..."
                clipped_samples.append(s)
            samples = ", ".join(clipped_samples)
            query = f"Table: {table_name}; Column: {col_name}; Samples: {samples if samples else '[empty]'}"[:500]
            try:
                results = self.vector_store.search(
                    query,
                    k=5,
                    domain=table_name,
                    column_code=col_name,
                )
            except Exception as e:
                print(f"⚠️ [{table_name}] RAG 检索失败 column={col_name}: {type(e).__name__}: {e}")
                results = []

            # --- Debug: 打印检索结果（默认关闭） ---
            if self.debug_rag:
                print(f"\n--- RAG Search Results for query: '{query}' ---")
                if not results:
                    print("No results found.")
                else:
                    ranked_preview = self._rerank_rag_candidates(results, col_name, top_k=5)
                    for i, m in enumerate(ranked_preview):
                        print(f"Result {i+1}:")
                        print(f"  - URI: {m['uri']}")
                        print(f"  - Domain: {m['domain']}, ColumnCode: {m['column_code']}")
                        print(
                            f"  - Label: {m['label']}, Type: {m['type']}, Range: {m['range']}, "
                            f"Role: {m['role']}, Priority: {m['priority']}, Concept: {m['canonical_concept_id']}"
                        )
                        print(f"  - Snippet: {m['snippet']}...")
                print("-------------------------------------------------\n")
            # --- End Debug ---

            context += f"\nColumn '{col_name}' candidate terms:\n"
            col_candidates = []
            for m in self._rerank_rag_candidates(results, col_name, top_k=5):
                term_obj = {
                    "uri": m["uri"],
                    "type": m["type"],
                    "label": m["label"],
                    "comment": m["comment"],
                    "domain": m["domain"],
                    "range": m["range"],
                    "role": m["role"],
                    "priority": m["priority"],
                    "canonical_concept_id": m["canonical_concept_id"],
                }
                col_candidates.append(term_obj)
                context += f"  - {json.dumps(term_obj, ensure_ascii=False)}\n"
            # 保留顺序去重
            rag_candidates[col_name] = col_candidates
            if col_index % 5 == 0 or col_index == len(columns):
                elapsed = time.perf_counter() - started
                print(f"[{table_name}] RAG 进度 {col_index}/{len(columns)}，elapsed={elapsed:.1f}s")
        print(f"[{table_name}] RAG 检索完成，耗时 {time.perf_counter() - started:.1f}s")
        return context, rag_candidates

    def run_mapping_agent(self, table_fingerprint):
        """Mapping Agent: 映射列到术语标准"""
        print("🤖 Mapping Agent 正在工作...")
        table_name = table_fingerprint.get("table_name", "")
        rag_started = time.perf_counter()
        _, rag_candidates = self._get_rag_context(table_fingerprint)
        print(f"[{table_name}] RAG 阶段完成，耗时 {time.perf_counter() - rag_started:.1f}s")
        rag_candidates = self._strict_rag_candidates(table_fingerprint, rag_candidates)
        self.rag_candidates_cache[table_name] = rag_candidates
        retrieval_enabled = self._is_retrieval_enabled()
        compact_fingerprint = self._compact_columns_for_mapping(table_fingerprint)
        compact_candidates = self._compact_rag_candidates(rag_candidates, top_k=3)
        
        system_prompt = (
            "你是一名资深语义映射智能体。"
            "请为每个列名选择最合适的候选术语编号，并仅返回 JSON。"
        )
        user_content = f"""
        表字段:
        {json.dumps(compact_fingerprint, ensure_ascii=False)}

        候选术语（按列，id 为候选编号）:
        {json.dumps(compact_candidates, ensure_ascii=False)}

        当前模式:
        - 已启用检索增强: {retrieval_enabled}
        - 允许输出公共本体 URI: {self.allow_public_uri}

        规则:
        1. 只从该列候选中选择；没有候选或无法判断则返回 null。
        2. 只能选择 domain 等于当前表且 URI 尾部为 当前表.当前列 的候选；其他表候选必须视为不可选。
        3. 优先 priority 高（P0>P1>P2）、role 与字段语义匹配的候选。
        4. 输出必须是最小 JSON：{{"列名": 候选id或null}}。
        5. 不要输出 uri、label、reason、Markdown 代码块或解释文本。
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        print(f"[{table_name}] Mapping Chat 开始，timeout={self.chat_timeout:.0f}s，max_retries={self.chat_max_retries}")
        chat_started = time.perf_counter()
        try:
            content = self._chat(messages)
        except Exception as e:
            self._write_chat_debug_log(
                table_name,
                "mapping",
                messages,
                elapsed=time.perf_counter() - chat_started,
                error=e,
            )
            print(f"⚠️ [{table_name}] Mapping Chat 失败，使用 RAG 候选兜底: {type(e).__name__}: {e}")
            return self._fallback_mapping_from_rag(table_fingerprint, rag_candidates)
        chat_elapsed = time.perf_counter() - chat_started
        print(f"[{table_name}] Mapping Chat 完成，耗时 {chat_elapsed:.1f}s")
        raw_mapping = self._parse_json_output(content, fallback={})
        self._write_chat_debug_log(
            table_name,
            "mapping",
            messages,
            content=content,
            elapsed=chat_elapsed,
            parse_info=self._summarize_parsed_json(raw_mapping),
        )
        return self._normalize_candidate_id_mapping(
            raw_mapping,
            table_fingerprint,
            rag_candidates,
        )

    def run_relation_agent(self, table_fingerprint):
        """Relation Agent: 识别主外键"""
        print("🤖 Relation Agent 正在工作...")
        explicit_pk = table_fingerprint.get("explicit_pk", [])
        explicit_fks = table_fingerprint.get("explicit_fks", [])
        table_count = table_fingerprint.get("table_count", 0)
        compact_relation_data = self._compact_columns_for_relation(table_fingerprint)
        system_prompt = "请根据表结构识别 PK/FK，只返回最小 JSON。"
        user_content = f"""
        表结构统计:
        {json.dumps(compact_relation_data, ensure_ascii=False)}

        显式约束: pk={json.dumps(explicit_pk, ensure_ascii=False)}, fks={json.dumps(explicit_fks, ensure_ascii=False)}, table_count={table_count}

        规则:
        1. 显式约束优先。
        2. PK 必须最小且能唯一标识行；描述、分类、时间字段不能作为 PK。
        3. 无法确定 PK 返回 null。
        4. FK 只返回可确定的关系列；无法识别返回空列表。
        5. 只返回 JSON：{{"pk": "列名"或["列名"]或null, "fks": ["列名"]}}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        table_name = table_fingerprint.get("table_name", "")
        print(f"[{table_name}] Relation Chat 开始，timeout={self.chat_timeout:.0f}s，max_retries={self.chat_max_retries}")
        chat_started = time.perf_counter()
        try:
            content = self._chat(messages)
        except Exception as e:
            self._write_chat_debug_log(
                table_name,
                "relation",
                messages,
                elapsed=time.perf_counter() - chat_started,
                error=e,
            )
            print(f"⚠️ [{table_name}] Relation Chat 失败，使用显式约束/gid兜底: {type(e).__name__}: {e}")
            return self._fallback_relations(table_fingerprint)
        chat_elapsed = time.perf_counter() - chat_started
        print(f"[{table_name}] Relation Chat 完成，耗时 {chat_elapsed:.1f}s")
        raw_relations = self._parse_json_output(content, fallback={})
        self._write_chat_debug_log(
            table_name,
            "relation",
            messages,
            content=content,
            elapsed=chat_elapsed,
            parse_info=self._summarize_parsed_json(raw_relations),
        )
        return self._normalize_relation_output(table_fingerprint, raw_relations)

    def run_validator_agent(self, table_fingerprint, mapping, relations):
        """Validator Agent: 审查并修正 [创新点]"""
        print("🕵️ Validator Agent 正在审查...")
        table_name = table_fingerprint.get("table_name", "")
        rag_candidates = self.rag_candidates_cache.get(table_name, {})
        suspicious = self._find_suspicious_mapping_fields(table_name, mapping, rag_candidates)
        if not suspicious:
            print(f"[{table_name}] Validator 跳过：未发现可疑映射")
            return self._sanitize_mapping_to_table(table_fingerprint, mapping)

        normalized = self._sanitize_mapping_to_table(table_fingerprint, mapping)
        llm_suspicious = {}
        for col, payload in suspicious.items():
            reasons = set(payload.get("reasons", []))
            candidates = payload.get("candidates", [])
            if ("domain_mismatch" in reasons or "uri_table_mismatch" in reasons) and not candidates:
                normalized[col] = None
                continue
            llm_suspicious[col] = payload

        if not llm_suspicious:
            print(f"[{table_name}] Validator 跳过：跨表可疑字段已按规则置空")
            return normalized

        system_prompt = (
            "你是一名知识图谱质量审查专家。"
            "只审查给出的可疑字段，并返回候选编号 JSON。"
        )
        user_content = f"""
        表名: {table_fingerprint['table_name']}
        候选关系: {json.dumps(relations, ensure_ascii=False)}
        可疑字段:
        {json.dumps(llm_suspicious, ensure_ascii=False)}

        规则:
        1. 只从可疑字段的 candidates 中选择。
        2. candidates 均已过滤为同表同列候选；无 candidates 时必须返回 null。
        3. 优先 role/priority 与字段语义匹配。
        4. 无合适候选返回 null。
        5. 只返回 JSON：{{"列名": 候选id或null}}，不要输出 Markdown 代码块。
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        print(f"[{table_name}] Validator Chat 开始，timeout={self.chat_timeout:.0f}s，max_retries={self.chat_max_retries}")
        chat_started = time.perf_counter()
        try:
            content = self._chat(messages)
        except Exception as e:
            self._write_chat_debug_log(
                table_name,
                "validator",
                messages,
                elapsed=time.perf_counter() - chat_started,
                error=e,
            )
            print(f"⚠️ [{table_name}] Validator Chat 失败，沿用候选映射: {type(e).__name__}: {e}")
            return normalized
        chat_elapsed = time.perf_counter() - chat_started
        print(f"[{table_name}] Validator Chat 完成，耗时 {chat_elapsed:.1f}s")
        raw_mapping = self._parse_json_output(content, fallback={})
        self._write_chat_debug_log(
            table_name,
            "validator",
            messages,
            content=content,
            elapsed=chat_elapsed,
            parse_info=self._summarize_parsed_json(raw_mapping),
        )
        revised = self._normalize_candidate_id_mapping(raw_mapping, table_fingerprint, rag_candidates)
        for col in llm_suspicious:
            if col in revised:
                normalized[col] = revised[col]
        # 若审查结果未返回 reason，则尽量沿用上一轮映射的 reason
        for col, value in normalized.items():
            if not isinstance(value, dict):
                continue
            if str(value.get("reason", "")).strip():
                continue
            prev = mapping.get(col)
            if not isinstance(prev, dict):
                continue
            prev_reason = str(prev.get("reason", "")).strip()
            prev_uri = str(prev.get("uri", "")).strip()
            if prev_reason and prev_uri and prev_uri == str(value.get("uri", "")).strip():
                value["reason"] = prev_reason
        return normalized
