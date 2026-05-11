import os

from rdb2g.common.paths import build_index_dir
from rdb2g.retrieval.schema_parser import parse_private_kb, parse_schema_org
from rdb2g.retrieval.vector_store import OntologyVectorStore


def init_vector_store(kb_file=None, schema_file=None):
    """初始化向量库：私域知识库优先，其次兼容 schema.org，最后可无知识库模式。"""

    def fallback_no_retrieval(reason):
        print(f"⚠️ 向量检索初始化失败，降级为无检索模式：{reason}")
        print("   提示：将 QWEN_EMBEDDING_MODEL 改为你账号可用的 DashScope Embedding 模型后可恢复 RAG。")
        store = OntologyVectorStore(enable_retrieval=False)
        store.create_or_load_index()
        return store

    if kb_file:
        if not os.path.exists(kb_file):
            raise FileNotFoundError(f"⚠️ 未找到私域知识库文件: {kb_file}")
        terms = parse_private_kb(kb_file)
        store = OntologyVectorStore(persist_dir=build_index_dir(kb_file, "private"), enable_retrieval=True)
        try:
            store.create_or_load_index(terms)
        except RuntimeError as e:
            if "Embedding 模型不可用" in str(e):
                return fallback_no_retrieval(e)
            raise
        return store

    if schema_file:
        if not os.path.exists(schema_file):
            raise FileNotFoundError(f"⚠️ 未找到本体文件: {schema_file}")
        terms = parse_schema_org(schema_file)
        store = OntologyVectorStore(persist_dir=build_index_dir(schema_file, "schema"), enable_retrieval=True)
        try:
            store.create_or_load_index(terms)
        except RuntimeError as e:
            if "Embedding 模型不可用" in str(e):
                return fallback_no_retrieval(e)
            raise
        return store

    store = OntologyVectorStore(enable_retrieval=False)
    store.create_or_load_index()
    return store
