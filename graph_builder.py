from rdflib import Graph, URIRef, Literal, RDF, Namespace
from rdflib.namespace import RDFS
import urllib.parse
import pandas as pd
import re

class RDFGraphBuilder:
    def __init__(self):
        self.g = Graph()
        self.SCHEMA = Namespace("http://schema.org/")
        self.TERM = Namespace("http://example.org/term-meta/")
        self.g.bind("schema", self.SCHEMA)
        self.g.bind("term", self.TERM)
        self.g.bind("rdfs", RDFS)
        self.base_uri = "http://example.org/data/"
        self._declared_terms = set()

    def _infer_referenced_table(self, fk_column_name):
        """
        根据外键列名推断引用的表名。
        这是一个简单的启发式规则，例如 'Cinema_ID' -> 'cinema'。
        """
        base_name = re.sub(r'(_id|_fk|id|fk)$', '', fk_column_name, flags=re.IGNORECASE)
        return base_name.lower()

    def _extract_term_uri(self, mapping_value):
        """兼容两种映射格式：
        1) 旧格式: column -> "uri"
        2) 新格式: column -> {"uri": "...", ...}
        """
        if mapping_value is None:
            return None
        if isinstance(mapping_value, dict):
            uri = mapping_value.get("uri")
            if isinstance(uri, str):
                uri = uri.strip()
                return uri or None
            return None
        if isinstance(mapping_value, str):
            uri = mapping_value.strip()
            return uri or None
        return None

    def _ensure_term_semantics(self, mapping_value):
        """为术语补充最小语义注释（仅一次）"""
        if not isinstance(mapping_value, dict):
            return
        uri = self._extract_term_uri(mapping_value)
        if not uri or uri in self._declared_terms:
            return

        term_uri = URIRef(uri)
        label = str(mapping_value.get("label", "")).strip()
        comment = str(mapping_value.get("comment", "")).strip()
        reason = str(mapping_value.get("reason", "")).strip()

        self.g.add((term_uri, RDF.type, RDF.Property))
        if label:
            self.g.add((term_uri, RDFS.label, Literal(label, lang="zh")))
        if comment:
            self.g.add((term_uri, RDFS.comment, Literal(comment, lang="zh")))
        if reason:
            self.g.add((term_uri, self.TERM.mappingReason, Literal(reason)))

        self._declared_terms.add(uri)

    def add_table_data(self, dataframe, table_name, mapping, primary_key=None, foreign_keys=None):
        """
        将 DataFrame 的每一行转换为 RDF 子图。
        通用化 URI 构建，并增加了防御性代码以确保复合主键的正确性。
        """
        print(f"🔨 正在为表 '{table_name}' 生成图谱 (包含关系链接)...")
        
        fk_set = set(foreign_keys or [])
        for _, mapping_value in (mapping or {}).items():
            self._ensure_term_semantics(mapping_value)
        
        for _, row in dataframe.iterrows():
            # 1. 构建当前行的主语 URI
            entity_id = None
            is_composite = isinstance(primary_key, list) and len(primary_key) > 0

            if is_composite:
                try:
                    # --- 防御性代码：只选择结尾是 '_id' 的列来构建复合主键 --- #
                    # 这可以忽略 Agent 可能错误返回的任何其他列（如 'Date'）
                    pk_columns = [c for c in primary_key if c.lower().endswith('_id')]
                    
                    id_parts = [f"{col}_{row[col]}" for col in pk_columns if not pd.isna(row[col])]
                    
                    # 仅当所有预期的主键部分都存在时才创建复合 ID
                    if len(id_parts) == len(pk_columns) and pk_columns:
                        entity_id = "-".join(id_parts)
                except KeyError:
                    pass
            
            if not entity_id:
                pk_col = primary_key if isinstance(primary_key, str) else None
                if pk_col and pk_col in row and not pd.isna(row[pk_col]):
                    entity_id = str(row[pk_col])
            
            if not entity_id:
                entity_id = f"row_{_}"
            
            safe_entity_id = urllib.parse.quote(entity_id)
            subject_uri = URIRef(f"{self.base_uri}{table_name}/{safe_entity_id}")

            # 2. 添加实体类型定义
            self.g.add((subject_uri, RDF.type, self.SCHEMA.Thing))

            if is_composite and entity_id and "row_" not in entity_id:
                 self.g.add((subject_uri, self.SCHEMA.name, Literal(entity_id)))

            # 3. 遍历所有列，添加属性三元组
            for col, val in row.items():
                if pd.isna(val):
                    continue
                
                mapping_value = mapping.get(col)
                schema_term = self._extract_term_uri(mapping_value)
                if not schema_term or schema_term.lower() == 'null':
                    continue

                prop_uri_str = schema_term.replace("https://", "http://")
                if prop_uri_str.startswith("schema:"):
                    prop_uri = self.SCHEMA[prop_uri_str.split(":")[1]]
                else:
                    prop_uri = URIRef(prop_uri_str)

                if col in fk_set:
                    referenced_table = self._infer_referenced_table(col)
                    referenced_id = urllib.parse.quote(str(val))
                    object_uri = URIRef(f"{self.base_uri}{referenced_table}/{referenced_id}")
                    self.g.add((subject_uri, prop_uri, object_uri))
                else:
                    self.g.add((subject_uri, prop_uri, Literal(val)))

    def save_graph(self, output_path="knowledge_graph.ttl"):
        self.g.serialize(destination=output_path, format="turtle")
        print(f"✅ 知识图谱已保存至: {output_path}")
