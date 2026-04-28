import os
import json
import time
import urllib.parse
from openai import OpenAI, APIConnectionError, APITimeoutError, RateLimitError

class MultiAgentSystem:
    def __init__(self, vector_store=None, allow_public_uri=False, local_uri_base="http://example.org/auto/"):
        # 使用 Qwen(DashScope) 的 OpenAI 兼容接口
        self.client = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url=os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        )
        self.chat_model = os.getenv("QWEN_CHAT_MODEL", "qwen3.5-flash")
        self.enable_thinking = os.getenv("QWEN_ENABLE_THINKING", "1") == "1"
        self.vector_store = vector_store
        self.debug_rag = os.getenv("DEBUG_RAG_RESULTS", "0") == "1"
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

    def _is_retrieval_enabled(self):
        return bool(self.vector_store is not None and getattr(self.vector_store, "vector_db", None) is not None)

    def _build_local_uri(self, column_name):
        safe = urllib.parse.quote(str(column_name).strip())
        return f"{self.local_uri_base}{safe}"

    def _parse_json_output(self, content, fallback):
        if isinstance(content, (dict, list)):
            return content
        if not isinstance(content, str):
            return fallback

        candidates = [content.strip()]
        if "```" in content:
            start = content.find("```json")
            if start != -1:
                start = content.find("\n", start)
                end = content.find("```", start + 1) if start != -1 else -1
                if start != -1 and end != -1:
                    candidates.append(content[start + 1:end].strip())

        l_brace, r_brace = content.find("{"), content.rfind("}")
        if l_brace != -1 and r_brace != -1 and r_brace > l_brace:
            candidates.append(content[l_brace:r_brace + 1])

        for text in candidates:
            try:
                return json.loads(text)
            except Exception:
                continue
        return fallback

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

    def _normalize_mapping_output(self, mapping, table_fingerprint, allowed_terms_by_column=None):
        """无知识库场景下，按策略过滤公共 URI，避免误用公共本体"""
        if not isinstance(mapping, dict):
            return {}

        columns = [c.get("name") for c in table_fingerprint.get("columns", []) if isinstance(c, dict)]

        if self._is_retrieval_enabled() and allowed_terms_by_column:
            normalized = {}
            for col in columns:
                allowed = self._normalize_candidate_list(allowed_terms_by_column.get(col, []))
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

    def _chat(self, messages, max_retries=6):
        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                extra_body = {"enable_thinking": True} if self.enable_thinking else None
                completion = self.client.chat.completions.create(
                    model=self.chat_model,
                    messages=messages,
                    extra_body=extra_body,
                )
                try:
                    return completion.choices[0].message.content
                except Exception:
                    # 回退：直接返回完整 JSON 字符串，便于排错
                    return json.dumps(completion.model_dump(), ensure_ascii=False)
            except (APIConnectionError, APITimeoutError, RateLimitError) as e:
                last_exc = e
                if attempt >= max_retries:
                    break
                wait_s = min(2 ** attempt, 30)
                print(f"⚠️ Chat 请求失败({type(e).__name__})，{wait_s}s 后重试（{attempt}/{max_retries}）...")
                time.sleep(wait_s)
        raise last_exc

    def _get_rag_context(self, table_fingerprint):
        """为表中的每一列检索 RAG 上下文"""
        if self.vector_store is None or getattr(self.vector_store, "vector_db", None) is None:
            return "", {}

        context = ""
        rag_candidates = {}
        table_data = table_fingerprint
        table_name = str(table_data.get("table_name", ""))
        for col in table_data.get('columns', []):
            # 检索与 列名+样本 相关的术语
            raw_samples = col.get('samples', [])[:3]
            clipped_samples = []
            for s in raw_samples:
                s = str(s).strip()
                if len(s) > 120:
                    s = s[:120] + "..."
                clipped_samples.append(s)
            samples = ", ".join(clipped_samples)
            col_name = col["name"]
            query = f"Table: {table_name}; Column: {col_name}; Samples: {samples if samples else '[empty]'}"[:500]
            results = self.vector_store.search(
                query,
                k=5,
                domain=table_name,
                column_code=col_name,
            )

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
        return context, rag_candidates

    def run_mapping_agent(self, table_fingerprint):
        """Mapping Agent: 映射列到术语标准"""
        print("🤖 Mapping Agent 正在工作...")
        rag_context, rag_candidates = self._get_rag_context(table_fingerprint)
        table_name = table_fingerprint.get("table_name", "")
        self.rag_candidates_cache[table_name] = rag_candidates
        retrieval_enabled = self._is_retrieval_enabled()
        
        system_prompt = (
            "你是一名资深语义映射智能体。"
            "请将每个列名映射到最合适的标准术语，并仅返回 JSON，不要输出任何额外说明。"
        )
        user_content = f"""
        输入数据（表指纹）:
        {json.dumps(table_fingerprint, ensure_ascii=False)}

        参考知识（RAG 检索上下文，可能为空）:
        {rag_context}

        RAG候选术语（按列）:
        {json.dumps(rag_candidates, ensure_ascii=False)}

        当前模式:
        - 已启用检索增强: {retrieval_enabled}
        - 允许输出公共本体 URI: {self.allow_public_uri}

        规则:
        1. 综合分析列名、样本值与 RAG 上下文。
        2. 若某列存在候选术语，必须从该列候选集合中选择；候选为空时返回 null。
        2.1 候选中若包含 priority/role 字段，优先选择 priority 更高（P0>P1>P2）且 role 更匹配业务语义的术语。
        3. 若没有 RAG 上下文（即未提供知识库），必须完全基于列名、样本值和表语义进行推断。
        4. 若未启用检索增强且不允许公共本体 URI，请避免使用 schema.org / w3.org / opengis 等公共词表 URI。
        5. 如果扫描到的数据库缺少明确主键/外键信息，请基于列名语义进行实体抽取（识别核心实体相关列）与关系推断（识别疑似关联列）。
        6. 若无法确定合适映射，使用 null。
        7. 输出对象格式:
           {{
             "列名": {{
               "uri": "...",
               "type": "...",
               "label": "...",
               "comment": "...",
               "domain": "...",
               "range": "...",
               "reason": "...",
               "confidence": 0.0-1.0
             }} 或 null
           }}
        
        仅返回 JSON 对象。
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        raw_mapping = self._parse_json_output(content, fallback={})
        return self._normalize_mapping_output(
            raw_mapping,
            table_fingerprint,
            allowed_terms_by_column=rag_candidates,
        )

    def run_relation_agent(self, table_fingerprint):
        """Relation Agent: 识别主外键"""
        print("🤖 Relation Agent 正在工作...")
        explicit_pk = table_fingerprint.get("explicit_pk", [])
        explicit_fks = table_fingerprint.get("explicit_fks", [])
        table_count = table_fingerprint.get("table_count", 0)
        system_prompt = (
            "请分析表结构以识别主键（PK）与外键（FK）。"
            "PK 可能是单列，也可能是复合主键。"
            "若数据库中缺少明确主外键约束，请基于列名进行实体抽取和关系推断。"
            "只返回最小化 JSON 对象。"
        )
        user_content = f"""
        表数据:
        {json.dumps(table_fingerprint, ensure_ascii=False)}

        已知显式约束:
        - explicit_pk: {json.dumps(explicit_pk, ensure_ascii=False)}
        - explicit_fks: {json.dumps(explicit_fks, ensure_ascii=False)}
        - table_count: {table_count}

        规则:
        0. 如果 explicit_pk / explicit_fks 已给出，优先沿用这些显式约束。
        1. 主键（PK）必须是唯一标识一行数据的最小列集合，不得包含冗余列。
        2. 以 "_id" 结尾或包含 "id" 语义的列，是 PK/FK 的强候选。
        3. 关键限制：描述性字段（如名称、标题）、度量字段（如价格、数量）以及日期时间字段（如 Date）不能作为 PK。
        4. 若 PK 为单列，"pk" 返回字符串；若为复合主键，"pk" 返回列名列表。
        5. 若没有明确 PK，"pk" 返回 null。
        6. 若 table_count=1 且 explicit_fks 为空，"fks" 必须返回空列表。
        7. "fks" 需包含可确定或可推断的关系列；仅在多表且存在跨表证据时才能推断外键。
        8. 若无法识别任何关系列，"fks" 返回空列表。

        只返回最小化 JSON 对象。
        - 单列主键示例: {{ \"pk\": \"some_id\", \"fks\": [\"col_a\", \"col_b\"] }}
        - 复合主键示例: {{ \"pk\": [\"part1_id\", \"part2_id\"], \"fks\": [\"col_c\"] }}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        raw_relations = self._parse_json_output(content, fallback={})
        return self._normalize_relation_output(table_fingerprint, raw_relations)

    def run_validator_agent(self, table_fingerprint, mapping, relations):
        """Validator Agent: 审查并修正 [创新点]"""
        print("🕵️ Validator Agent 正在审查...")
        table_name = table_fingerprint.get("table_name", "")
        rag_candidates = self.rag_candidates_cache.get(table_name, {})
        system_prompt = (
            "你是一名知识图谱质量审查专家。"
            "请审查并修正映射结果，仅返回最小化 JSON 映射对象。"
        )
        has_rag = self._is_retrieval_enabled()
        user_content = f"""
        表名: {table_fingerprint['table_name']}
        候选映射: {json.dumps(mapping, ensure_ascii=False)}
        候选关系: {json.dumps(relations, ensure_ascii=False)}
        RAG候选术语（按列）: {json.dumps(rag_candidates, ensure_ascii=False)}
        已启用检索增强: {has_rag}
        显式外键: {json.dumps(table_fingerprint.get('explicit_fks', []), ensure_ascii=False)}
        表数量: {table_fingerprint.get('table_count', 0)}
        
        规则:
        1. 确保每个映射对象都语义一致，且包含正确的 uri/type/label/comment/domain/range。
        1.1 若候选提供 priority/role 信息，优先保留高优先级且角色匹配的候选（P0>P1>P2）。
        2. 若某列是外键或被推断为关系列，应优先映射为对象属性（关系），而不是数据属性。
        3. 当数据库缺少显式主外键时，结合列名语义与上下文，校正实体抽取与关系推断结果。
        4. 若未启用检索增强，不要依赖外部本体，只基于输入数据进行修正。
        4.1 若某列存在 RAG 候选术语，最终值必须取自该列候选集合；否则返回 null。
        5. 若 table_count=1 且显式外键为空，不要把普通属性列修正成关系列。
        6. 若某列映射语义未改变，尽量保留已有 reason 字段；若发生修正，给出新的 reason。
        7. 输出格式与输入映射一致：每列值为术语对象或 null。
        
        仅输出最终修正后的 JSON 映射。
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        raw_mapping = self._parse_json_output(content, fallback={})
        normalized = self._normalize_mapping_output(
            raw_mapping,
            table_fingerprint,
            allowed_terms_by_column=rag_candidates,
        )
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
