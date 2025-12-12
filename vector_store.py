import os
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain.docstore.document import Document

class OntologyVectorStore:
    def __init__(self, persist_dir="./data/chroma_db"):
        self.persist_dir = persist_dir
        self.embedding_fn = OpenAIEmbeddings(model="text-embedding-3-small")
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
                # 构造富语义文本：论文提到的 "One-hop subgraph" 文本化表示
                content = (f"Term: {term['label']}\nType: {term['type']}\n"
                           f"Desc: {term['comment']}\nDomain: {term['domain']}\nRange: {term['range']}")
                docs.append(Document(page_content=content, metadata={"uri": term['uri']}))
            
            self.vector_db = Chroma.from_documents(docs, self.embedding_fn, persist_directory=self.persist_dir)
            print("索引构建完成并已保存。")

    def search(self, query, k=5):
        """语义检索"""
        if not self.vector_db: raise ValueError("Vector DB not initialized!")
        return self.vector_db.similarity_search(query, k=k)