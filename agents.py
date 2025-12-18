import os
import json
from openai import OpenAI

class MultiAgentSystem:
    def __init__(self, vector_store):
        # 使用 DashScope 的 OpenAI 兼容接口（通义千问）
        self.client = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        # 聊天模型可通过环境变量覆盖，默认使用 qwen-plus
        self.chat_model = os.getenv("QWEN_CHAT_MODEL", "qwen-plus")
        self.vector_store = vector_store

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
        context = ""
        table_data = table_fingerprint
        for col in table_data.get('columns', []):
            # 检索与 列名+样本 相关的术语
            samples = ", ".join(col.get('samples', [])[:3])
            query = f"Column: {col['name']}, Samples: {samples}"
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
        """Mapping Agent: 映射列到 Schema.org"""
        print("🤖 Mapping Agent 正在工作...")
        rag_context = self._get_rag_context(table_fingerprint)
        
        system_prompt = (
            "You are an expert Semantic Mapping Agent. "
            "Return ONLY a minified JSON object mapping each column name to a Schema.org URI."
        )
        user_content = f"""
        Input Data (Table Fingerprint):
        {json.dumps(table_fingerprint, ensure_ascii=False)}

        Ontology Knowledge (RAG Context):
        {rag_context}

        Instructions:
        1. Analyze the column name and sample values.
        2. Choose the best matching URI from Schema.org (use the RAG context).
        3. If no good match exists, use null.
        
        Return ONLY a JSON object: {{ "column_name": "schema_uri" }}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        return json.loads(content)

    def run_relation_agent(self, table_fingerprint):
        """Relation Agent: 识别主外键"""
        print("🤖 Relation Agent 正在工作...")
        system_prompt = (
            "Analyze the table structure to identify Primary Keys (PK) and likely Foreign Keys (FK). "
            "A PK can be a single column or multiple columns (composite key). "
            "Return ONLY a minified JSON object."
        )
        user_content = f"""
        Table Data:
        {json.dumps(table_fingerprint, ensure_ascii=False)}

        Rules:
        1. The Primary Key (PK) is the MINIMAL set of columns required to uniquely identify a row. Do not include extra columns.
        2. Columns ending in '_id' are the strongest candidates for being part of a PK or FK.
        3. **CRITICAL RULE**: Descriptive columns (like names, titles), measurement columns (like price, duration, count), and especially **date/time columns (like 'Date') MUST NOT be part of the Primary Key**.
        4. If the PK is a single column, return its name as a string for the \"pk\" value.
        5. If the PK is a composite key (multiple columns), return a list of the column names for the \"pk\" value.
        6. If no clear PK is found, return null for the \"pk\" value.

        Return ONLY a minified JSON object.
        - Example with single PK: {{ \"pk\": \"some_id\", \"fks\": [\"col_a\", \"col_b\"] }}
        - Example with composite PK: {{ \"pk\": [\"part1_id\", \"part2_id\"], \"fks\": [\"col_c\"] }}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        return json.loads(content)

    def run_validator_agent(self, table_fingerprint, mapping, relations):
        """Validator Agent: 审查并修正 [创新点]"""
        print("🕵️ Validator Agent 正在审查...")
        system_prompt = (
            "You are a Knowledge Graph Quality Assurance expert. "
            "Review and correct the mapping. Return ONLY a minified JSON mapping."
        )
        user_content = f"""
        Table: {table_fingerprint['table_name']}
        Proposed Mapping: {json.dumps(mapping, ensure_ascii=False)}
        Proposed Relations: {json.dumps(relations, ensure_ascii=False)}
        
        Rules:
        1. Ensure the URI is a valid Schema.org term.
        2. If a column is a Foreign Key, it should likely be mapped to an ObjectProperty (relationship), not a DataType property.
        
        Output ONLY the FINAL corrected JSON mapping.
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]
        content = self._chat(messages)
        return json.loads(content)
