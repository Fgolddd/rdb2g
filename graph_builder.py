from rdflib import Graph, URIRef, Literal, RDF, Namespace
from rdflib.namespace import RDFS
import urllib.parse
import json
import pandas as pd
import re
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter, defaultdict

class RDFGraphBuilder:
    def __init__(self, kb_file=None):
        self.g = Graph()
        self.SCHEMA = Namespace("http://schema.org/")
        self.g.bind("schema", self.SCHEMA)
        self.g.bind("rdfs", RDFS)
        self.base_uri = "http://example.org/data/"
        self._declared_terms = set()
        self.field_display_map, self.table_field_display_map = self._build_display_name_maps(kb_file=kb_file)

    def _extract_tail(self, text):
        text = str(text or "").strip()
        if not text:
            return ""
        if text.startswith("http://") or text.startswith("https://"):
            try:
                tail = urlparse(text).path.rsplit("/", 1)[-1]
                if tail:
                    return tail
            except Exception:
                pass
            return text.rsplit("/", 1)[-1]
        return text

    def _split_table_column(self, uri_or_key, domain_hint=None):
        tail = self._extract_tail(uri_or_key)
        table_name = str(domain_hint or "").strip()
        column_name = tail
        if "." in tail:
            left, right = tail.rsplit(".", 1)
            if left and right:
                table_name = left
                column_name = right
        return table_name, column_name

    def _load_kb_entries(self, kb_file=None):
        default_kb = Path("data/company/zhongshan_rag_terms.json")
        kb_path = Path(kb_file) if kb_file else default_kb
        if not kb_path.exists():
            return []
        try:
            with kb_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return []

        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            if isinstance(data.get("terms"), list):
                entries = data["terms"]
            elif isinstance(data.get("@graph"), list):
                entries = data["@graph"]
            else:
                entries = [data]
        else:
            entries = []
        return [entry for entry in entries if isinstance(entry, dict)]

    def _build_display_name_maps(self, kb_file=None):
        """构建字段中文显示名：优先读取私域知识库，再补充兜底。"""
        field_counters = defaultdict(Counter)
        table_field_display_map = {}

        for entry in self._load_kb_entries(kb_file=kb_file):
            label = str(entry.get("label", "")).strip()
            if not label:
                continue
            domain = str(entry.get("domain", "")).strip()
            uri = str(entry.get("uri", "")).strip()
            if not uri:
                continue
            table_name, column_name = self._split_table_column(uri, domain_hint=domain)
            column_name = str(column_name).strip()
            if not column_name:
                continue
            field_counters[column_name][label] += 1
            field_counters[column_name.lower()][label] += 1
            if table_name:
                table_field_display_map[f"{table_name.lower()}.{column_name.lower()}"] = label

        field_display_map = {
            field: counter.most_common(1)[0][0]
            for field, counter in field_counters.items()
            if counter
        }

        # KB 未覆盖/在本项目需要统一显示的字段兜底
        field_display_map.setdefault("gid", "记录ID")
        field_display_map.setdefault("geom", "空间几何")
        field_display_map.setdefault("Shape_Length", "几何长度")
        field_display_map.setdefault("Shape_Area", "几何面积")
        field_display_map.setdefault("S_GUID", "上级系统标识码")

        # 多义字段按 table.column 覆盖（优先级高于知识库和全局）
        table_field_display_map.update({
            "zs_mp_bz.dm": "门牌代码",
            "zs_dh_bz.dm": "楼栋代码",
            "zs_dy_bz.dm": "单元代码",
            "zs_fj_bz.dm": "房间代码",
            "zs_yl_bz.dm": "院落代码",
            "zs_mp_bz.dhbm": "门牌别名",
            "zs_dh_bz.dhbm": "栋号别名",
            "zs_gaj_xq.gadm": "公安组织机构_代码",
            "zs_gafj_xq.gadm": "公安组织机构_代码",
            "zs_pcs_xq.gadm": "公安组织机构_代码",
            "zs_jws_xq.gadm": "公安组织机构_代码",
            "zs_gaj_xq.gamc": "公安组织机构_名称",
            "zs_gafj_xq.gamc": "公安组织机构_名称",
            "zs_pcs_xq.gamc": "公安组织机构_名称",
            "zs_jws_xq.gamc": "公安组织机构_名称",
        })
        return field_display_map, table_field_display_map

    def _field_display_name(self, field_name, table_name=None):
        field_name = str(field_name).strip()
        if not field_name:
            return ""
        if table_name:
            key = f"{str(table_name).strip().lower()}.{field_name.lower()}"
            mapped = self.table_field_display_map.get(key)
            if mapped:
                return mapped
        return self.field_display_map.get(field_name, self.field_display_map.get(field_name.lower(), field_name))

    def _extract_display_title(self, row, table_name, fallback_value):
        preferred_cols = [
            "MC", "MPQC", "DZ", "SSMC", "LKMC", "FLMC", "XLMC", "ZLMC", "DLMC", "DM", "gid"
        ]
        for col in preferred_cols:
            if col not in row:
                continue
            val = row[col]
            if pd.isna(val):
                continue
            val_text = str(val).strip()
            if val_text:
                return val_text
        return str(fallback_value)

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

        if not label:
            tail = uri.rsplit("/", 1)[-1]
            if "." in tail:
                table_name, col = tail.rsplit(".", 1)
                label = self._field_display_name(col, table_name=table_name)
            else:
                label = self._field_display_name(tail)

        self.g.add((term_uri, RDF.type, RDF.Property))
        if label:
            self.g.add((term_uri, RDFS.label, Literal(label, lang="zh")))
        if comment:
            self.g.add((term_uri, RDFS.comment, Literal(comment, lang="zh")))

        self._declared_terms.add(uri)

    def add_table_data(self, dataframe, table_name, mapping, primary_key=None, foreign_keys=None, foreign_key_refs=None):
        """
        将 DataFrame 的每一行转换为 RDF 子图。
        通用化 URI 构建，并增加了防御性代码以确保复合主键的正确性。
        """
        print(f"🔨 正在为表 '{table_name}' 生成图谱 (包含关系链接)...")
        
        fk_set = {str(col).lower() for col in (foreign_keys or [])}
        fk_ref_map = {}
        for fk_col, ref_table in (foreign_key_refs or {}).items():
            fk_col_str = str(fk_col).strip().lower()
            ref_table_str = str(ref_table).strip().lower()
            if fk_col_str and ref_table_str:
                fk_ref_map[fk_col_str] = ref_table_str
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

            display_title = self._extract_display_title(row, table_name, entity_id)
            self.g.add((subject_uri, self.SCHEMA.name, Literal(display_title, lang="zh")))
            self.g.add((subject_uri, RDFS.label, Literal(display_title, lang="zh")))

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

                col_lower = str(col).lower()
                if col_lower in fk_set:
                    referenced_table = fk_ref_map.get(col_lower) or self._infer_referenced_table(col)
                    referenced_id = urllib.parse.quote(str(val))
                    object_uri = URIRef(f"{self.base_uri}{referenced_table}/{referenced_id}")
                    self.g.add((subject_uri, prop_uri, object_uri))
                else:
                    self.g.add((subject_uri, prop_uri, Literal(val)))

    def save_graph(self, output_path="knowledge_graph.ttl"):
        self.g.serialize(destination=output_path, format="turtle")
        print(f"✅ 知识图谱已保存至: {output_path}")
