import sqlite3
import pandas as pd
import json
import os

from ignored_columns import is_ignored_rag_column

class SpiderDataLoader:
    def __init__(self, db_path):
        """初始化加载器，连接 SQLite 数据库"""
        if db_path != ":memory:" and not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        self.db_path = db_path
        # check_same_thread=False 允许在多线程/Agent环境中使用
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def _quote_identifier(self, identifier):
        safe = str(identifier).replace('"', '""')
        return f'"{safe}"'

    def get_table_columns(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        return cursor.fetchall()

    def _fetch_scalar(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None

    def get_all_table_names(self):
        """获取数据库中所有非系统表的名称"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        return [t for t in tables if t != 'sqlite_sequence']

    def get_table_constraints(self, table_name):
        """获取表的显式主键/外键约束信息"""
        cursor = self.conn.cursor()

        table_info = self.get_table_columns(table_name)
        pk_cols = [(row[5], row[1]) for row in table_info if row[5] > 0]
        explicit_pk = [name for _, name in sorted(pk_cols, key=lambda x: x[0])]

        cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)")
        fk_rows = cursor.fetchall()
        fk_details = []
        explicit_fks = []
        for row in fk_rows:
            fk_col = row[3]
            fk_details.append({
                "column": fk_col,
                "ref_table": row[2],
                "ref_column": row[4],
            })
            if fk_col not in explicit_fks:
                explicit_fks.append(fk_col)

        return {
            "explicit_pk": explicit_pk,
            "explicit_fks": explicit_fks,
            "explicit_fk_details": fk_details,
        }

    def generate_table_fingerprint(self, table_name, k_samples=5):
        """生成表的语义指纹：包含列名、类型、统计信息和样本数据"""
        try:
            table_info = self.get_table_columns(table_name)
            row_count = int(self._fetch_scalar(f"SELECT COUNT(*) FROM `{table_name}`") or 0)
        except Exception as e:
            return {"error": str(e)}

        stats_threshold = int(os.getenv("FINGERPRINT_FULL_SCAN_ROW_THRESHOLD", "50000"))
        use_full_stats = row_count <= stats_threshold
        column_infos = []
        for col_info in table_info:
            col = col_info[1]
            if is_ignored_rag_column(col):
                continue

            quoted_col = self._quote_identifier(col)
            stats = {
                "name": col,
                "dtype": str(col_info[2] or "TEXT"),
                "unique_count": None,
                "null_ratio": None,
            }

            if use_full_stats:
                unique_sql = f"SELECT COUNT(DISTINCT {quoted_col}) FROM `{table_name}`"
                non_null_sql = f"SELECT COUNT({quoted_col}) FROM `{table_name}`"
                unique_count = self._fetch_scalar(unique_sql)
                non_null_count = int(self._fetch_scalar(non_null_sql) or 0)
                stats["unique_count"] = int(unique_count or 0)
                stats["null_ratio"] = round((row_count - non_null_count) / row_count, 2) if row_count else 0.0

            # 提取非空样本并转为字符串（裁剪超长值，避免提示词超长）
            sample_values = []
            sample_sql = (
                f"SELECT {quoted_col} FROM `{table_name}` "
                f"WHERE {quoted_col} IS NOT NULL LIMIT {max(int(k_samples), 1)}"
            )
            cursor = self.conn.cursor()
            cursor.execute(sample_sql)
            for (raw,) in cursor.fetchall():
                s = str(raw).strip()
                if col.lower() == "geom":
                    s = f"[geom:{len(s)}chars]"
                elif len(s) > 180:
                    s = s[:180] + "..."
                sample_values.append(s)
            stats["samples"] = sample_values
            column_infos.append(stats)

        all_tables = self.get_all_table_names()
        constraints = self.get_table_constraints(table_name)

        fingerprint = {
            "source": os.path.basename(self.db_path),
            "table_name": table_name,
            "row_count": row_count,
            "columns": column_infos,
            "all_columns": [row[1] for row in table_info],
            "table_count": len(all_tables),
            "all_tables": all_tables,
            "explicit_pk": constraints["explicit_pk"],
            "explicit_fks": constraints["explicit_fks"],
            "explicit_fk_details": constraints["explicit_fk_details"],
        }
        return fingerprint

    def get_dataframe(self, table_name, columns=None, chunksize=None):
        """按需读取 DataFrame，用于后续图谱生成"""
        if columns:
            projected = ", ".join(self._quote_identifier(col) for col in columns)
        else:
            projected = "*"
        sql = f"SELECT {projected} FROM `{table_name}`"
        return pd.read_sql_query(sql, self.conn, chunksize=chunksize)

    def close(self):
        self.conn.close()
