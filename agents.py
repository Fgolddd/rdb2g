import os
import json
import urllib.parse
from openai import OpenAI

class MultiAgentSystem:
    def __init__(self, vector_store=None, allow_public_uri=False, local_uri_base="http://example.org/auto/"):
        # 使用 DashScope 的 OpenAI 兼容接口（通义千问）
        self.client = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        # 聊天模型可通过环境变量覆盖，默认使用 qwen-plus
        self.chat_model = os.getenv("QWEN_CHAT_MODEL", "qwen-plus")
        self.vector_store = vector_store
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

    def _is_retrieval_enabled(self):
        return bool(self.vector_store is not None and getattr(self.vector_store, "vector_db", None) is not None)

    def _build_local_uri(self, column_name):
        safe = urllib.parse.quote(str(column_name).strip())
        return f"{self.local_uri_base}{safe}"

    def _normalize_mapping_output(self, mapping, table_fingerprint):
        """无知识库场景下，按策略过滤公共 URI，避免误用公共本体"""
        if not isinstance(mapping, dict):
            return {}

        if self._is_retrieval_enabled() or self.allow_public_uri:
            return mapping

        normalized = {}
        for col in [c.get("name") for c in table_fingerprint.get("columns", []) if isinstance(c, dict)]:
            value = mapping.get(col)
            if value is None:
                normalized[col] = None
                continue

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

    def _chat(self, messages):
        completion = self.client.chat.completions.create(
            model=self.chat_model,
            messages=messages,
        )
        try:
            return completion.choices[0].message.content
        except Exception:
            # 回退：直接返回完整 JSON 字符串，便于排错
            return json.dumps(completion.model_dump(), ensure_ascii=False)

    def _get_rag_context(self, table_fingerprint):
        """为表中的每一列检索 RAG 上下文"""
        if self.vector_store is None or getattr(self.vector_store, "vector_db", None) is None:
            return ""

        context = ""
        table_data = table_fingerprint
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
            query = f"Column: {col['name']}, Samples: {samples}"[:500]
            results = self.vector_store.search(query, k=3)

            # --- Debug: 打印检索结果 ---
            print(f"\n--- RAG Search Results for query: '{query}' ---")
            if not results:
                print("No results found.")
            else:
                for i, doc in enumerate(results):
                    print(f"Result {i+1}:")
                    # 打印部分页面内容和完整的元数据
                    print(f"  - Page Content: {str(doc.page_content).replace('\n', ' ')[:150]}...")
                    print(f"  - Metadata: {doc.metadata}")
            print("-------------------------------------------------\n")
            # --- End Debug ---

            context += f"\nColumn '{col['name']}' potential matches:\n"
            for doc in results:
                uri = getattr(doc, 'metadata', {}).get('uri') if hasattr(doc, 'metadata') else None
                uri = uri or (doc.metadata['uri'] if isinstance(doc.metadata, dict) and 'uri' in doc.metadata else 'unknown')
                context += f"  - {uri} ({doc.page_content[:50]}...)\n"
        return context

    def run_mapping_agent(self, table_fingerprint):
        """Mapping Agent: 映射列到术语标准"""
        print("🤖 Mapping Agent 正在工作...")
        rag_context = self._get_rag_context(table_fingerprint)
        retrieval_enabled = self._is_retrieval_enabled()
        
        system_prompt = (
            "你是一名资深语义映射智能体。"
            "请将每个列名映射到最合适的标准术语 URI，并且只返回最小化 JSON 对象，不要输出任何额外说明。"
        )
        user_content = f"""
        输入数据（表指纹）:
        {json.dumps(table_fingerprint, ensure_ascii=False)}

        参考知识（RAG 检索上下文，可能为空）:
        {rag_context}

        当前模式:
        - 已启用检索增强: {retrieval_enabled}
        - 允许输出公共本体 URI: {self.allow_public_uri}

        规则:
        1. 综合分析列名、样本值与 RAG 上下文。
        2. 若有 RAG 上下文，优先使用其中最匹配的术语 URI。
        3. 若没有 RAG 上下文（即未提供知识库），必须完全基于列名、样本值和表语义进行推断。
        4. 若未启用检索增强且不允许公共本体 URI，请避免使用 schema.org / w3.org / opengis 等公共词表 URI。
        5. 如果扫描到的数据库缺少明确主键/外键信息，请基于列名语义进行实体抽取（识别核心实体相关列）与关系推断（识别疑似关联列）。
        6. 若无法确定合适映射，使用 null。
        
        仅返回 JSON 对象: {{ "column_name": "term_uri" }}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        raw_mapping = json.loads(content)
        return self._normalize_mapping_output(raw_mapping, table_fingerprint)

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
        raw_relations = json.loads(content)
        return self._normalize_relation_output(table_fingerprint, raw_relations)

    def run_validator_agent(self, table_fingerprint, mapping, relations):
        """Validator Agent: 审查并修正 [创新点]"""
        print("🕵️ Validator Agent 正在审查...")
        system_prompt = (
            "你是一名知识图谱质量审查专家。"
            "请审查并修正映射结果，仅返回最小化 JSON 映射对象。"
        )
        has_rag = self._is_retrieval_enabled()
        user_content = f"""
        表名: {table_fingerprint['table_name']}
        候选映射: {json.dumps(mapping, ensure_ascii=False)}
        候选关系: {json.dumps(relations, ensure_ascii=False)}
        已启用检索增强: {has_rag}
        显式外键: {json.dumps(table_fingerprint.get('explicit_fks', []), ensure_ascii=False)}
        表数量: {table_fingerprint.get('table_count', 0)}
        
        规则:
        1. 确保每个 URI 都是有效且语义一致的术语标识。
        2. 若某列是外键或被推断为关系列，应优先映射为对象属性（关系），而不是数据属性。
        3. 当数据库缺少显式主外键时，结合列名语义与上下文，校正实体抽取与关系推断结果。
        4. 若未启用检索增强，不要依赖外部本体，只基于输入数据进行修正。
        5. 若 table_count=1 且显式外键为空，不要把普通属性列修正成关系列。
        
        仅输出最终修正后的 JSON 映射。
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        raw_mapping = json.loads(content)
        return self._normalize_mapping_output(raw_mapping, table_fingerprint)
