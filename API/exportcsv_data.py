import os
import time
from pathlib import Path

import duckdb

import glob
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
TABLE_NAME = "API"
CHUNK_SIZE = 500_000
TOTAL_ROWS = 25_650_825


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def main():
    con = connect_db()

    for i in range(0, TOTAL_ROWS, CHUNK_SIZE):
        output_file = f"/home/kayhan/Desktop/Gelen_Datalar/API/EXPORT/OUTPUT_PART_{i // CHUNK_SIZE}.csv"
        query = f"""
        COPY (SELECT * FROM {TABLE_NAME} LIMIT {CHUNK_SIZE} OFFSET {i}) TO '{output_file}' (HEADER, DELIMITER ',');
        """
        con.execute(query)
        print(f"Exported {i // CHUNK_SIZE + 1} / {TOTAL_ROWS // CHUNK_SIZE + 1} chunks")

    con.close()


if __name__ == "__main__":
    main()
