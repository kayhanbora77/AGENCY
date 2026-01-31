import time
from pathlib import Path

import duckdb
import pandas as pd


# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
EXCEL_DIR = Path("/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/")
TABLE_NAME = "TRUST_TRAVEL"


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def load_excel_files(con) -> None:
    dfs = []

    for file in sorted(EXCEL_DIR.glob("*.xlsx")):
        log(f"⏰ Loading {file.name}")

        if file.name != "TRUST_TRAVEL_RAW DATA.xlsx":
            continue

        # Read ALL sheets as dict {sheet_name: DataFrame}
        sheets = pd.read_excel(
            file,
            sheet_name=None,
            dtype=str,
        )

        for sheet_name, df in sheets.items():
            if df.empty:
                continue

            df["source_file"] = file.name
            df["source_sheet"] = sheet_name
            dfs.append(df)

    if not dfs:
        raise RuntimeError("No Excel data found")

    # Concatenate all sheets
    final_df = pd.concat(dfs, ignore_index=True)

    # Recreate table
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.register("final_df", final_df)

    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT * FROM final_df
    """)


def main() -> None:
    start = time.time()
    log(f"Starting at {now_str()}")

    con = connect_db()
    load_excel_files(con)
    con.close()

    log(f"Finished at {now_str()}")
    elapsed = time.time() - start
    log(f"🎉 Loading completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")


if __name__ == "__main__":
    main()
