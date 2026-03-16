import sqlite3

def convert_sql_to_sqlite():
    # 1. 连接到 SQLite 数据库（如果文件不存在则会自动创建）
    conn = sqlite3.connect('poi.sqlite')
    cursor = conn.cursor()

    # 2. 创建表结构（根据你 SQL 文件中的 INSERT 字段推断）
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS zs_POI_dm (
        gid INTEGER PRIMARY KEY,
        geom TEXT,
        GUID TEXT,
        MC TEXT,
        DZ TEXT,
        DLMC TEXT,
        ZLMC TEXT,
        XLMC TEXT,
        SJMC TEXT,
        SSJMC TEXT,
        QXMC TEXT,
        LXFS TEXT,
        BZ_GUID TEXT,
        BZDZMC TEXT,
        ZDGACM TEXT,
        ZDGADM TEXT,
        SYZT TEXT,
        SLSJ TEXT,
        QYSJ TEXT,
        TYSJ TEXT,
        GXSJ TEXT,
        DZLX TEXT,
        ing_2000 REAL,
        lat_2000 REAL,
        BZ TEXT
    );
    """
    cursor.execute(create_table_sql)

    print("正在读取 poi.sql 文件...")
    # 3. 读取原始 PostgreSQL 导出的 SQL 文件
    with open('./data/company/poi.sql', 'r', encoding='utf-8') as f:
        sql_content = f.read()

    print("正在清洗不兼容的 PostgreSQL 语法...")
    # 4. 语法转换与清洗
    # 去除 PostgreSQL 特有的几何类型强转，在 SQLite 中将坐标点串直接作为 TEXT 存储
    sql_content = sql_content.replace("::public.geometry", "")
    # 去除表名的 public 模式前缀
    sql_content = sql_content.replace('public."zs_POI_dm"', '"zs_POI_dm"')

    print("正在向 SQLite 写入数据，请稍候...")
    # 5. 执行清洗后的 SQL 插入语句
    try:
        cursor.executescript(sql_content)
        conn.commit()
        print("✅ 转换成功！已在当前目录下生成 poi.sqlite 文件。")
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    convert_sql_to_sqlite()