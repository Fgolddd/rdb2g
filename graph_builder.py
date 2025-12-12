from rdflib import Graph, URIRef, Literal, RDF, Namespace
import urllib.parse

class RDFGraphBuilder:
    def __init__(self):
        self.g = Graph()
        self.SCHEMA = Namespace("http://schema.org/")
        self.g.bind("schema", self.SCHEMA)
        self.base_uri = "http://example.org/data/"

    def add_table_data(self, dataframe, table_name, mapping, primary_key=None):
        """
        将 DataFrame 的每一行转换为 RDF 子图
        mapping: {"col_name": "schema:email", ...}
        """
        print(f"🔨 正在为表 {table_name} 生成图谱...")
        
        for _, row in dataframe.iterrows():
            # 1. 构建 Subject URI
            # 如果有主键，用主键值；否则用行号或随机ID
            if primary_key and primary_key in row:
                entity_id = urllib.parse.quote(str(row[primary_key]))
            else:
                entity_id = f"row_{_}"
            
            subject_uri = URIRef(f"{self.base_uri}{table_name}/{entity_id}")

            # 2. 添加类型定义 (这里简化为 schema:Thing，可进一步让 Agent 预测表类型)
            self.g.add((subject_uri, RDF.type, self.SCHEMA.Thing))

            # 3. 添加属性三元组
            for col, val in row.items():
                if pd.isna(val): continue # 跳过空值
                
                # 获取对应的 schema 属性
                schema_term = mapping.get(col)
                if schema_term and schema_term.lower() != 'null':
                    # 处理 schema: 前缀
                    if schema_term.startswith("schema:"):
                        prop_uri = self.SCHEMA[schema_term.split(":")[1]]
                    elif "schema.org" in schema_term:
                        prop_uri = URIRef(schema_term)
                    else:
                        prop_uri = self.SCHEMA[schema_term]
                    
                    self.g.add((subject_uri, prop_uri, Literal(val)))

    def save_graph(self, output_path="output.ttl"):
        self.g.serialize(destination=output_path, format="turtle")
        print(f"✅ 知识图谱已保存至: {output_path}")