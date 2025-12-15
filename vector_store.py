import os
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from openai import OpenAI

class QwenEmbeddings:
    """使用 DashScope 的 OpenAI 兼容接口实现的最小 Embeddings 适配器，
    以避免额外安装 dashscope SDK，直接复用 openai 客户端。
    满足 LangChain 向量库所需的 embed_query / embed_documents 接口。
    """
    def __init__(self, model: str | None = None, api_key: str | None = None, base_url: str | None = None):
        self.model = model or os.getenv("QWEN_EMBEDDING_MODEL")
        self.client = OpenAI(
            api_key=api_key or os.getenv("DASHSCOPE_API_KEY"),
            base_url=base_url or os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        )

    def embed_query(self, text: str):
        if text is None:
            return []
        resp = self.client.embeddings.create(model=self.model, input=text)
        return resp.data[0].embedding

    def embed_documents(self, texts: list[str]):
        if not texts:
            return []
        # DashScope 兼容接口限制每次最多 10 条输入，需做分批
        max_batch = int(os.getenv("QWEN_EMBEDDING_BATCH_SIZE", "10"))
        results = []
        for i in range(0, len(texts), max_batch):
            batch = texts[i:i+max_batch]
            resp = self.client.embeddings.create(model=self.model, input=batch)
            results.extend([item.embedding for item in resp.data])
        return results


class OntologyVectorStore:
    def __init__(self, persist_dir="./data/chroma_db"):
        self.persist_dir = persist_dir
        # 使用通义千问（DashScope 兼容接口）作为向量嵌入
        self.embedding_fn = QwenEmbeddings()
        self.vector_db = None

    def create_or_load_index(self, schema_terms=None):
        """如果本地存在索引则加载，否则新建"""
        if os.path.exists(self.persist_dir) and os.listdir(self.persist_dir):
            print("加载本地向量索引...")
            self.vector_db = Chroma(persist_directory=self.persist_dir, embedding_function=self.embedding_fn)
        else:
            if not schema_terms:
                raise ValueError("本地索引不存在，且未提供 schema_terms 用于构建！")
            print("构建新向量索引...")
            docs = []
            for term in schema_terms:
                # 构造富语义文本：把 Schema.org 术语的核心字段拼接为文档
                content = (f"Term: {term['label']}\nType: {term['type']}\n"
                           f"Desc: {term['comment']}\nDomain: {term['domain']}\nRange: {term['range']}")
                docs.append(Document(page_content=content, metadata={"uri": term['uri']}))
            
            self.vector_db = Chroma.from_documents(docs, self.embedding_fn, persist_directory=self.persist_dir)
            print("索引构建完成并已保存。")

    def search(self, query, k=5):
        """语义检索"""
        if self.vector_db is None:
            raise ValueError("Vector DB not initialized!")
        return self.vector_db.similarity_search(query, k=k)
