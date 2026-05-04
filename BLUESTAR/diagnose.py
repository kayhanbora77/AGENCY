import duckdb
import pandas as pd
import time
from pathlib import Path
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "BLUESTAR"
TARGET_TABLE = "BLUESTAR_TARGET_2"
REJECTION_TABLE = "BLUESTAR_REJECTIONS"


def log(msg: str):
    print(msg, flush=True)


def diagnose_duplicates(con):
    log("\n🔍 Duplicate Diagnosis:")

    # 1. True identical rows
    r1 = con.execute(f"""
        SELECT COUNT(*) - COUNT(DISTINCT (
            PNRNo, AirlineName, FltNo1, FltDate1, 
            FltNo2, FltNo3, FltNo4, 
            Airport1, Airport2, Airport3, Airport4, Airport5
        ))
        FROM {SOURCE_TABLE}
    """).fetchone()[0]
    log(f"  Fully identical source rows    : {r1:>10,}")

    # 2. Rows sharing KEY_COLS only
    r2 = con.execute(f"""
        SELECT SUM(cnt - 1) FROM (
            SELECT COUNT(*) as cnt
            FROM {SOURCE_TABLE}
            GROUP BY PNRNo, AirlineName, FltNo1, FltDate1, Airport1, Airport2
            HAVING cnt > 1
        )
    """).fetchone()[0]
    log(f"  Excess rows by KEY_COLS        : {r2:>10,}")

    # 3. Are they truly identical or just same key?
    r3 = con.execute(f"""
        SELECT SUM(cnt - 1) FROM (
            SELECT COUNT(*) as cnt
            FROM {SOURCE_TABLE}
            GROUP BY PNRNo, AirlineName, FltNo1, FltDate1, Airport1, Airport2,
                     FltNo2, FltNo3, FltNo4, FltDate2, FltDate3, FltDate4
            HAVING cnt > 1
        )
    """).fetchone()[0]
    log(f"  Excess rows (extended key)     : {r3:>10,}")

    log(f"  → If r1≈r2≈r3: pure source dupes, current logic is correct")
    log(f"  → If r3 << r2: rows share KEY_COLS but differ — key is too narrow")


def main():
    con = duckdb.connect(DB_PATH)
    diagnose_duplicates(con)


if __name__ == "__main__":
    main()
