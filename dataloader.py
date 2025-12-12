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
            
            # 提取非空样本并转为字符串
            sample_values = col_data.dropna().head(k_samples).astype(str).tolist()
            stats["samples"] = sample_values
            column_infos.append(stats)

        fingerprint = {
            "source": os.path.basename(self.db_path),
            "table_name": table_name,
            "row_count": len(df),
            "columns": column_infos
        }
        return fingerprint

    def get_dataframe(self, table_name):
        """获取完整的 DataFrame，用于后续图谱生成"""
        return pd.read_sql_query(f"SELECT * FROM `{table_name}`", self.conn)

    def close(self):
        self.conn.close()

# --- 单元测试模块 ---
if __name__ == "__main__":
    # 直接使用已有的 .sqlite 文件进行测试，不再创建或插入示例数据
    db_path = "./data/spider_data/battle_death/battle_death.sqlite"
    if not os.path.exists(db_path):
        print(f"未找到数据库文件: {db_path}")
    else:
        loader = SpiderDataLoader(db_path)
        try:
            tables = loader.get_all_table_names()
            print("Tables:", tables)
            if tables:
                first_table = tables[1]
                fp = loader.generate_table_fingerprint(first_table)
                print("Fingerprint:", json.dumps(fp, indent=2, ensure_ascii=False))
            else:
                print("数据库中未发现任何用户表。")
        finally:
            loader.close()
