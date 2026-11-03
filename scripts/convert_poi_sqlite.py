import re
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SQL_PATH = BASE_DIR / "data/company/poi.sql"
DB_PATH = BASE_DIR / "data/company/poi.sqlite"

CREATE_TABLE_SQLS = [
    """
    CREATE TABLE IF NOT EXISTS "zs_POI_dm" (
        "gid" INTEGER PRIMARY KEY,
        "geom" TEXT,
        "GUID" TEXT,
        "MC" TEXT,
        "DZ" TEXT,
        "DLMC" TEXT,
        "ZLMC" TEXT,
        "XLMC" TEXT,
        "SJMC" TEXT,
        "SSJMC" TEXT,
        "QXMC" TEXT,
        "LXFS" TEXT,
        "BZ_GUID" TEXT,
        "BZDZMC" TEXT,
        "ZDGACM" TEXT,
        "ZDGADM" TEXT,
        "SYZT" TEXT,
        "SLSJ" TEXT,
        "QYSJ" TEXT,
        "TYSJ" TEXT,
        "GXSJ" TEXT,
        "DZLX" TEXT,
        "ing_2000" REAL,
        "lat_2000" REAL,
        "BZ" TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "zs_AOI_dm" (
        "gid" INTEGER PRIMARY KEY,
        "geom" TEXT,
        "GUID" TEXT,
        "MC" TEXT,
        "DZ" TEXT,
        "DLMC" TEXT,
        "ZLMC" TEXT,
        "XLMC" TEXT,
        "SJMC" TEXT,
        "SSJMC" TEXT,
        "QXMC" TEXT,
        "LXFS" TEXT,
        "BZ_GUID" TEXT,
        "BZDZMC" TEXT,
        "ZDGACM" TEXT,
        "ZDGADM" TEXT,
        "SYZT" TEXT,
        "SLSJ" TEXT,
        "QYSJ" TEXT,
        "TYSJ" TEXT,
        "GXSJ" TEXT,
        "DZLX" TEXT,
        "ing_2000" REAL,
        "lat_2000" REAL,
        "BZ" TEXT,
        "Shape_Length" REAL,
        "Shape_Area" REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "zs_FJ_bz" (
        "gid" INTEGER PRIMARY KEY,
        "geom" TEXT,
        "GUID" TEXT,
        "DM" TEXT,
        "FJH" TEXT,
        "FJHQC" TEXT,
        "FJHBM" TEXT,
        "FLMC" TEXT,
        "FLDM" TEXT,
        "DLMC" TEXT,
        "ZLMC" TEXT,
        "XLMC" TEXT,
        "CJMC" TEXT,
        "CJDM" TEXT,
        "GAMC" TEXT,
        "GADM" TEXT,
        "XNBS" TEXT,
        "SYPL" TEXT,
        "S_GUID" TEXT,
        "S_DM" TEXT,
        "S_MC" TEXT,
        "S_FLMC" TEXT,
        "S_FLDM" TEXT,
        "S_CJMC" TEXT,
        "S_CJDM" TEXT,
        "SYZT" TEXT,
        "SLSJ" TEXT,
        "QYSJ" TEXT,
        "TYSJ" TEXT,
        "GXSJ" TEXT,
        "DZLX" TEXT,
        "BZ" TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "zs_MP_bz" (
        "gid" INTEGER PRIMARY KEY,
        "geom" TEXT,
        "GUID" TEXT,
        "DM" TEXT,
        "MPH" TEXT,
        "MPQC" TEXT,
        "DHBM" TEXT,
        "FLMC" TEXT,
        "FLDM" TEXT,
        "DLMC" TEXT,
        "ZLMC" TEXT,
        "XLMC" TEXT,
        "CJMC" TEXT,
        "CJDM" TEXT,
        "GAMC" TEXT,
        "GADM" TEXT,
        "XNBS" TEXT,
        "SYPL" TEXT,
        "S_GUID" TEXT,
        "S_DM" TEXT,
        "S_MC" TEXT,
        "S_FLMC" TEXT,
        "S_FLDM" TEXT,
        "S_CJMC" TEXT,
        "S_CJDM" TEXT,
        "SYZT" TEXT,
        "SLSJ" TEXT,
        "QYSJ" TEXT,
        "TYSJ" TEXT,
        "GXSJ" TEXT,
        "DZLX" TEXT,
        "BZ" TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "zs_street_bz" (
        "gid" INTEGER PRIMARY KEY,
        "geom" TEXT,
        "Shape_Length" REAL,
        "GUID" TEXT,
        "DM" TEXT,
        "MC" TEXT,
        "BM" TEXT,
        "FLMC" TEXT,
        "FLDM" TEXT,
        "CJMC" TEXT,
        "CJDM" TEXT,
        "GAMC" TEXT,
        "GADM" TEXT,
        "S_GUID" TEXT,
        "S_DM" TEXT,
        "S_MC" TEXT,
        "S_FLMC" TEXT,
        "S_FLDM" TEXT,
        "S_CJMC" TEXT,
        "S_CJDM" TEXT,
        "S_GAMC" TEXT,
        "S_GADM" TEXT,
        "SYZT" TEXT,
        "SLSJ" TEXT,
        "QYSJ" TEXT,
        "TYSJ" TEXT,
        "GXSJ" TEXT,
        "BZ" TEXT
    );
    """,
]


def clean_postgres_sql(sql_content: str) -> str:
    sql_content = re.sub(r"::public\.geometry", "", sql_content)
    sql_content = re.sub(
        r'INSERT INTO\s+public\."([^"]+)"', r'INSERT INTO "\1"', sql_content
    )
    sql_content = re.sub(
        r"INSERT INTO\s+public\.([A-Za-z_][A-Za-z0-9_]*)",
        r'INSERT INTO "\1"',
        sql_content,
    )
    return sql_content


def convert_sql_to_sqlite():
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for create_sql in CREATE_TABLE_SQLS:
        cursor.execute(create_sql)

    print(f"正在读取 {SQL_PATH} ...")
    sql_content = SQL_PATH.read_text(encoding="utf-8")
    sql_content = clean_postgres_sql(sql_content)

    print("正在向 SQLite 写入数据，请稍候...")
    try:
        cursor.executescript(sql_content)
        conn.commit()
        print(f"✅ 转换成功！已生成 {DB_PATH}")
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    convert_sql_to_sqlite()