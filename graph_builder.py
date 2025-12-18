from rdflib import Graph, URIRef, Literal, RDF, Namespace
import urllib.parse
import pandas as pd
import re

class RDFGraphBuilder:
    def __init__(self):
        self.g = Graph()
        self.SCHEMA = Namespace("http://schema.org/")
        self.g.bind("schema", self.SCHEMA)
        self.base_uri = "http://example.org/data/"

    def _infer_referenced_table(self, fk_column_name):
        """
        根据外键列名推断引用的表名。
        这是一个简单的启发式规则，例如 'Cinema_ID' -> 'cinema'。
        """
        base_name = re.sub(r'(_id|_fk|id|fk)$', '', fk_column_name, flags=re.IGNORECASE)
        return base_name.lower()

    def add_table_data(self, dataframe, table_name, mapping, primary_key=None, foreign_keys=None):
        """
        将 DataFrame 的每一行转换为 RDF 子图。
        通用化 URI 构建，并增加了防御性代码以确保复合主键的正确性。
        """
        print(f"🔨 正在为表 '{table_name}' 生成图谱 (包含关系链接)...")
        
        fk_set = set(foreign_keys or [])
        
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
                
                schema_term = mapping.get(col)
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
