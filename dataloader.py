import sqlite3
import pandas as pd
import json
import os

class SpiderDataLoader:
    def __init__(self, db_path):
        """初始化加载器，连接 SQLite 数据库"""
        if db_path != ":memory:" and not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        self.db_path = db_path
        # check_same_thread=False 允许在多线程/Agent环境中使用
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def get_all_table_names(self):
        """获取数据库中所有非系统表的名称"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        return [t for t in tables if t != 'sqlite_sequence']

    def get_table_constraints(self, table_name):
        """获取表的显式主键/外键约束信息"""
        cursor = self.conn.cursor()

        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        table_info = cursor.fetchall()
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
            df = pd.read_sql_query(f"SELECT * FROM `{table_name}`", self.conn)
        except Exception as e:
            return {"error": str(e)}

        column_infos = []
        for col in df.columns:
            col_data = df[col]
            
            stats = {
                "name": col,
                "dtype": str(col_data.dtype),
                "unique_count": int(col_data.nunique()), # 基数，判断是否为枚举的关键
                "null_ratio": round(col_data.isnull().mean(), 2),
            }
            
            # 提取非空样本并转为字符串（裁剪超长值，避免提示词超长）
            sample_values = []
            for raw in col_data.dropna().head(k_samples).tolist():
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
            "row_count": len(df),
            "columns": column_infos,
            "table_count": len(all_tables),
            "all_tables": all_tables,
            "explicit_pk": constraints["explicit_pk"],
            "explicit_fks": constraints["explicit_fks"],
            "explicit_fk_details": constraints["explicit_fk_details"],
        }
        return fingerprint

    def get_dataframe(self, table_name):
        """获取完整的 DataFrame，用于后续图谱生成"""
        return pd.read_sql_query(f"SELECT * FROM `{table_name}`", self.conn)

    def close(self):
        self.conn.close()

