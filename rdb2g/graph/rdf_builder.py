from rdflib import Graph, URIRef, Literal, RDF, Namespace
from rdflib.namespace import RDFS
import urllib.parse
import json
import time
import pandas as pd
import re
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter, defaultdict
from rdb2g.common.progress import ProgressBar, format_elapsed
from rdb2g.common.ignored_columns import is_ignored_rdf_property

class RDFGraphBuilder:
    def __init__(self, kb_file=None, relation_rules=None, entity_index=None):
        self.SCHEMA = Namespace("http://schema.org/")
        self.base_uri = "http://example.org/data/"
        self.ZS = Namespace("http://example.org/zhongshan/")
        self.relation_rules = relation_rules
        self.entity_index = entity_index or {}
        self._declared_terms = set()
        self.field_display_map, self.table_field_display_map = self._build_display_name_maps(kb_file=kb_file)
        self._reset_graph()

    def _reset_graph(self):
        self.g = Graph()
        self.g.bind("schema", self.SCHEMA)
        self.g.bind("rdfs", RDFS)
        self.g.bind("zs", self.ZS)

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
        if self.relation_rules:
            row_dict = self._row_to_dict(row)
            return self.relation_rules.label_for_row(table_name, row_dict, fallback=fallback_value)
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

    def _row_to_dict(self, row):
        if hasattr(row, "to_dict"):
            return row.to_dict()
        if isinstance(row, dict):
            return row
        return dict(row)

    def _clean_value(self, value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        text = str(value).strip()
        if not text or text.lower() in {"null", "none", "nan"}:
            return ""
        return text

    def _subject_uri_for_row(self, table_name, row, fallback_id):
        if self.relation_rules:
            row_dict = self._row_to_dict(row)
            entity_id = self.relation_rules.entity_id_for_row(table_name, row_dict, fallback=fallback_id)
            return URIRef(self.relation_rules.uri_for_entity(table_name, entity_id)), entity_id
        safe_entity_id = urllib.parse.quote(str(fallback_id))
        return URIRef(f"{self.base_uri}{table_name}/{safe_entity_id}"), str(fallback_id)

    def _add_entity_metadata(self, subject_uri, table_name, row, entity_id, display_title):
        self.g.add((subject_uri, RDF.type, self.SCHEMA.Thing))
        if self.relation_rules:
            entity_type = self.relation_rules.entity_type_for_table(table_name)
            if entity_type:
                self.g.add((subject_uri, RDF.type, URIRef(self.relation_rules.type_uri(entity_type))))
                self.g.add((subject_uri, self.ZS.entityType, Literal(entity_type)))
            self.g.add((subject_uri, self.ZS.sourceTable, Literal(table_name)))
            self.g.add((subject_uri, self.ZS.entityId, Literal(entity_id)))

        self.g.add((subject_uri, self.SCHEMA.name, Literal(display_title, lang="zh")))
        self.g.add((subject_uri, RDFS.label, Literal(display_title, lang="zh")))

    def _lookup_entity_uri(self, table_name, key, value):
        clean = self._clean_value(value)
        if not clean:
            return None
        return self.entity_index.get((str(table_name), str(key), clean))

    def _source_values_for_rule(self, rule, row_dict):
        split_mode = str(rule.get("match_mode", "exact")) == "split_exact"
        delimiter = str(rule.get("split_delimiter", ",") or ",")
        values = []
        for source_key in self.relation_rules.source_key_candidates(rule):
            source_value = self._clean_value(row_dict.get(source_key))
            if not source_value:
                continue
            if split_mode:
                parts = [part.strip() for part in source_value.split(delimiter)]
                values.extend(part for part in parts if part)
            else:
                values.append(source_value)
        seen = set()
        result = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _target_uri_for_rule(self, rule, row_dict):
        for source_value in self._source_values_for_rule(rule, row_dict):
            for target_table, target_key in self.relation_rules.target_specs_for_rule(rule, row=row_dict):
                target_uri = self._lookup_entity_uri(target_table, target_key, source_value)
                if target_uri:
                    return target_uri
        return None

    def _target_uris_for_rule(self, rule, row_dict):
        target_uris = []
        seen = set()
        for source_value in self._source_values_for_rule(rule, row_dict):
            for target_table, target_key in self.relation_rules.target_specs_for_rule(rule, row=row_dict):
                target_uri = self._lookup_entity_uri(target_table, target_key, source_value)
                if not target_uri or target_uri in seen:
                    continue
                seen.add(target_uri)
                target_uris.append(target_uri)
        return target_uris

    def _add_relation_edges(self, subject_uri, table_name, row_dict):
        if not self.relation_rules:
            return 0
        added = 0
        for rule in self.relation_rules.relation_rules:
            if rule.get("enabled") is False:
                continue
            if table_name not in (rule.get("source_tables") or []):
                continue
            if str(rule.get("match_mode", "exact")) not in {"exact", "split_exact"}:
                continue
            target_uris = self._target_uris_for_rule(rule, row_dict)
            if not target_uris:
                continue
            relation_uri = URIRef(self.relation_rules.relation_uri(rule.get("name")))
            for target_uri in target_uris:
                self.g.add((subject_uri, relation_uri, URIRef(target_uri)))
                added += 1
        return added

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

    def _predicate_matches_table_column(self, uri, table_name, column_name):
        if not uri:
            return False
        tail = str(uri).strip().rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        if "." not in tail:
            return True
        pred_table, pred_column = tail.rsplit(".", 1)
        return pred_table == str(table_name).strip() and pred_column == str(column_name).strip()

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
        started = time.perf_counter()
        row_total = len(dataframe)
        print(f"🔨 正在为表 '{table_name}' 生成图谱 (包含关系链接)，行数={row_total}...")
        
        fk_set = {str(col).lower() for col in (foreign_keys or [])}
        fk_ref_map = {}
        skipped_cross_table_predicates = Counter()
        relation_edge_counts = Counter()
        for fk_col, ref_table in (foreign_key_refs or {}).items():
            fk_col_str = str(fk_col).strip().lower()
            ref_table_str = str(ref_table).strip().lower()
            if fk_col_str and ref_table_str:
                fk_ref_map[fk_col_str] = ref_table_str
        for col, mapping_value in (mapping or {}).items():
            term_uri = self._extract_term_uri(mapping_value)
            if self._predicate_matches_table_column(term_uri, table_name, col):
                self._ensure_term_semantics(mapping_value)
        
        row_progress = ProgressBar(total=row_total, label=f"构图 {table_name}", unit="行", min_interval=2.0)
        for row_num, (row_index, row) in enumerate(dataframe.iterrows(), start=1):
            row_dict = self._row_to_dict(row)
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
                entity_id = f"row_{row_index}"

            subject_uri, entity_id = self._subject_uri_for_row(table_name, row_dict, entity_id)

            display_title = self._extract_display_title(row, table_name, entity_id)
            self._add_entity_metadata(subject_uri, table_name, row_dict, entity_id, display_title)

            if is_composite and entity_id and "row_" not in entity_id:
                 self.g.add((subject_uri, self.SCHEMA.name, Literal(entity_id)))

            added_edges = self._add_relation_edges(subject_uri, table_name, row_dict)
            if added_edges:
                relation_edge_counts[table_name] += added_edges

            # 3. 遍历所有列，添加属性三元组
            for col, val in row.items():
                if pd.isna(val):
                    continue
                
                mapping_value = mapping.get(col)
                schema_term = self._extract_term_uri(mapping_value)
                if not schema_term or schema_term.lower() == 'null':
                    continue
                if not self._predicate_matches_table_column(schema_term, table_name, col):
                    skipped_cross_table_predicates[f"{col}->{schema_term}"] += 1
                    continue

                prop_uri_str = schema_term.replace("https://", "http://")
                if prop_uri_str.startswith("schema:"):
                    prop_uri = self.SCHEMA[prop_uri_str.split(":")[1]]
                else:
                    prop_uri = URIRef(prop_uri_str)

                col_lower = str(col).lower()
                if is_ignored_rdf_property(col_lower):
                    continue
                if col_lower in fk_set:
                    referenced_table = fk_ref_map.get(col_lower) or self._infer_referenced_table(col)
                    referenced_id = urllib.parse.quote(str(val))
                    object_uri = URIRef(f"{self.base_uri}{referenced_table}/{referenced_id}")
                    self.g.add((subject_uri, prop_uri, object_uri))
                else:
                    self.g.add((subject_uri, prop_uri, Literal(val)))
            row_progress.update(detail=f"row={row_num}")
        if skipped_cross_table_predicates:
            examples = ", ".join(
                f"{key}({count})"
                for key, count in skipped_cross_table_predicates.most_common(5)
            )
            print(f"⚠️ [{table_name}] 跳过跨表谓词 {sum(skipped_cross_table_predicates.values())} 条: {examples}")
        if relation_edge_counts:
            print(f"🔗 [{table_name}] 关系边 {sum(relation_edge_counts.values())} 条")
        row_progress.close(detail=f"完成，耗时 {format_elapsed(time.perf_counter() - started)}")

    def save_graph(self, output_path="knowledge_graph.ttl", append=False, reset=False):
        serialized = self.g.serialize(format="turtle")
        if append:
            serialized_lines = []
            for line in serialized.splitlines():
                if line.startswith("@prefix ") or line.startswith("PREFIX "):
                    continue
                serialized_lines.append(line)
            serialized = "\n".join(serialized_lines).strip()
            if serialized:
                serialized = "\n" + serialized + "\n"
        else:
            serialized = serialized if serialized.endswith("\n") else serialized + "\n"

        mode = "a" if append else "w"
        with open(output_path, mode, encoding="utf-8") as f:
            f.write(serialized)
        if reset:
            self._reset_graph()
