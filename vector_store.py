import os
import re
import json
import shutil
import hashlib
import time
# 优先使用新包 langchain-chroma，若未安装则回退到社区版

from langchain_chroma import Chroma

from langchain_core.documents import Document
from openai import OpenAI, APIConnectionError, APITimeoutError, RateLimitError, NotFoundError
from progress_utils import ProgressBar


def _env_int(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        print(f"⚠️ 忽略无效整数环境变量 {name}={value!r}")
        return default


def _env_float(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ 忽略无效数字环境变量 {name}={value!r}")
        return default

class QwenEmbeddings:
    """使用 Qwen(DashScope) 的 OpenAI 兼容接口实现最小 Embeddings 适配器，
    复用 openai 客户端，满足 LangChain 向量库所需接口。
    满足 LangChain 向量库所需的 embed_query / embed_documents 接口。
    """
    def __init__(self, model: str | None = None, api_key: str | None = None, base_url: str | None = None):
        self.model = model or os.getenv("QWEN_EMBEDDING_MODEL", "text-embedding-v4")
        self.max_input_chars = int(os.getenv("QWEN_EMBEDDING_MAX_CHARS", "4000"))
        self.timeout = _env_float("QWEN_EMBEDDING_TIMEOUT", 30.0)
        self.max_retries = max(_env_int("QWEN_EMBEDDING_MAX_RETRIES", 3), 1)
        self.client = OpenAI(
            api_key=api_key or os.getenv("DASHSCOPE_API_KEY"),
            base_url=base_url or os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        )

    def _create_embedding(self, input_data):
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self.client.embeddings.create(
                    model=self.model,
                    input=input_data,
                    timeout=self.timeout,
                )
            except NotFoundError as e:
                raise RuntimeError(
                    "Embedding 模型不可用：请将 QWEN_EMBEDDING_MODEL 配置为你账号可用的 DashScope 模型名。"
                    f" 当前值={self.model!r}"
                ) from e
            except (APIConnectionError, APITimeoutError, RateLimitError) as e:
                last_exc = e
                if attempt >= self.max_retries:
                    break
                wait_s = min(2 ** attempt, 20)
                print(f"⚠️ Embedding 请求失败({type(e).__name__})，{wait_s}s 后重试（{attempt}/{self.max_retries}）...")
                time.sleep(wait_s)
        raise last_exc

    def _sanitize_text(self, text: str | None, fallback: str) -> str:
        text = "" if text is None else str(text)
        text = text.strip()
        if not text:
            text = fallback
        if len(text) > self.max_input_chars:
            text = text[:self.max_input_chars]
        return text

    def embed_query(self, text: str):
        cleaned = self._sanitize_text(text, fallback="[empty query]")
        resp = self._create_embedding(cleaned)
        return resp.data[0].embedding

    def embed_documents(self, texts: list[str]):
        if not texts:
            return []
        cleaned_texts = [self._sanitize_text(t, fallback="[empty document]") for t in texts]
        # 按批次请求，降低单次请求体过大导致的失败概率。
        max_batch = int(os.getenv("QWEN_EMBEDDING_BATCH_SIZE", "10"))
        results = []
        for i in range(0, len(cleaned_texts), max_batch):
            batch = cleaned_texts[i:i+max_batch]
            resp = self._create_embedding(batch)
            results.extend([item.embedding for item in resp.data])
        return results


class OntologyVectorStore:
    def __init__(self, persist_dir="./data/chroma_db", enable_retrieval=True):
        self.persist_dir = persist_dir
        self.enable_retrieval = enable_retrieval
        # 使用 Qwen(DashScope)（OpenAI 兼容接口）作为向量嵌入
        self.embedding_fn = QwenEmbeddings()
        self.vector_db = None
        self.meta_file = "_index_meta.json"
        self.index_schema_version = 4

    def _compute_terms_hash(self, schema_terms):
        if not schema_terms:
            return None
        stable_text = json.dumps(schema_terms, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(stable_text.encode("utf-8")).hexdigest()

    def _meta_path(self):
        return os.path.join(self.persist_dir, self.meta_file)

    def _load_index_meta(self):
        path = self._meta_path()
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _save_index_meta(self, terms_hash, doc_count):
        os.makedirs(self.persist_dir, exist_ok=True)
        path = self._meta_path()
        payload = {
            "terms_hash": terms_hash,
            "doc_count": int(doc_count),
            "schema_version": self.index_schema_version,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _reset_persist_dir(self):
        if os.path.exists(self.persist_dir):
            shutil.rmtree(self.persist_dir)
        os.makedirs(self.persist_dir, exist_ok=True)

    def _extract_term_metadata(self, term):
        uri = str(term.get("uri", "")).strip()
        domain = str(term.get("domain", "")).strip()
        label = str(term.get("label", "")).strip()
        term_type = str(term.get("type", "")).strip()
        range_val = str(term.get("range", "")).strip()
        comment = str(term.get("comment", "")).strip()
        role = str(term.get("role", "")).strip()
        priority = str(term.get("priority", "")).strip()
        canonical_concept_id = str(term.get("canonical_concept_id", "")).strip()

        column_code = ""
        if "." in uri:
            _, column_code = uri.rsplit(".", 1)
        else:
            fallback = re.sub(r"^.*[/#]", "", uri)
            column_code = fallback

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
        }

    def _build_document_content(self, term, meta):
        chunk = {
            "uri": meta["uri"],
            "type": meta["type"],
            "label": meta["label"],
            "comment": meta["comment"],
            "domain": meta["domain"],
            "range": meta["range"],
            "role": meta["role"],
            "priority": meta["priority"],
            "canonical_concept_id": meta["canonical_concept_id"],
        }
        return json.dumps(chunk, ensure_ascii=False)

    def _merge_results(self, batches, k):
        merged = []
        seen = set()
        for batch in batches:
            for doc in batch:
                uri = (doc.metadata or {}).get("uri")
                key = uri or id(doc)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(doc)
                if len(merged) >= k:
                    return merged
        return merged

    def create_or_load_index(self, schema_terms=None):
        """如果本地存在索引则加载，否则新建"""
        if not self.enable_retrieval:
            print("未启用检索增强，跳过向量索引初始化。")
            self.vector_db = None
            return

        current_terms_hash = self._compute_terms_hash(schema_terms)
        has_local_index = os.path.exists(self.persist_dir) and os.listdir(self.persist_dir)
        index_meta = self._load_index_meta()
        needs_rebuild = False

        if has_local_index and schema_terms:
            if not index_meta:
                needs_rebuild = True
            elif index_meta.get("terms_hash") != current_terms_hash:
                needs_rebuild = True
            elif int(index_meta.get("schema_version", 0) or 0) != self.index_schema_version:
                needs_rebuild = True

        if has_local_index and not needs_rebuild:
            print("加载本地向量索引...")
            self.vector_db = Chroma(persist_directory=self.persist_dir, embedding_function=self.embedding_fn)
            return

        if not schema_terms:
            raise ValueError("本地索引不存在（或需重建），且未提供术语数据用于构建！")

        if needs_rebuild:
            print("检测到知识库内容变化，重建向量索引...")
            self._reset_persist_dir()
        else:
            print("构建新向量索引...")

        docs = []
        for term in schema_terms:
            meta = self._extract_term_metadata(term)
            content = self._build_document_content(term, meta)
            docs.append(Document(page_content=content, metadata=meta))

        batch_size = max(_env_int("VECTOR_INDEX_BATCH_SIZE", 50), 1)
        index_progress = ProgressBar(total=len(docs), label="向量索引构建", unit="术语", min_interval=1.0)
        self.vector_db = Chroma(persist_directory=self.persist_dir, embedding_function=self.embedding_fn)
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            self.vector_db.add_documents(batch)
            index_progress.update(step=len(batch), detail=f"batch {i // batch_size + 1}", force=True)
        index_progress.close()
        self._save_index_meta(current_terms_hash, len(docs))
        print("索引构建完成并已保存。")

    def _similarity_search_by_vector(self, query_vector, query_text, k, filter_data=None):
        if hasattr(self.vector_db, "similarity_search_by_vector"):
            return self.vector_db.similarity_search_by_vector(
                query_vector,
                k=k,
                filter=filter_data,
            )
        return self.vector_db.similarity_search(query_text, k=k, filter=filter_data)

    def search(self, query, k=5, domain=None, column_code=None):
        """语义检索（优先精确列名/域过滤，再回退全局检索）"""
        if self.vector_db is None:
            return []

        candidate_batches = []
        fetch_k = max(int(k), 3)
        query_vector = self.embedding_fn.embed_query(query)

        if domain and column_code:
            try:
                candidate_batches.append(
                    self._similarity_search_by_vector(
                        query_vector,
                        query,
                        k=fetch_k,
                        filter_data={"domain": str(domain), "column_code": str(column_code)},
                    )
                )
            except Exception:
                pass

        if column_code:
            try:
                candidate_batches.append(
                    self._similarity_search_by_vector(
                        query_vector,
                        query,
                        k=fetch_k,
                        filter_data={"column_code": str(column_code)},
                    )
                )
            except Exception:
                pass

        if domain:
            try:
                candidate_batches.append(
                    self._similarity_search_by_vector(
                        query_vector,
                        query,
                        k=max(fetch_k, 5),
                        filter_data={"domain": str(domain)},
                    )
                )
            except Exception:
                pass

        candidate_batches.append(
            self._similarity_search_by_vector(
                query_vector,
                query,
                k=max(fetch_k, 8),
            )
        )
        return self._merge_results(candidate_batches, int(k))
