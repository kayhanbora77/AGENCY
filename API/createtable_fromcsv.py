import duckdb
import pandas as pd
import glob
from pathlib import Path

DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
TABLE_NAME = "API_OLD"
CVS_DIR = Path("/home/kayhan/Desktop/Gelen_Datalar/API_OLD/PROCEED/")

files = glob.glob("/home/kayhan/Desktop/Gelen_Datalar/API_OLD/PROCEED/*.csv")

dtype_map = {
    "DepartureRunwayTimeLocal": "string",
    "DepartureRunwayTimeUtc": "string",
    "ArrivalRunwayTimeLocal": "string",
    "ArrivalRunwayTimeUtc": "string",
}


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def main():
    con = connect_db()
    first_chunk = True
    con.execute("DROP TABLE IF EXISTS main.API_OLD")

    for file in files:
        for chunk in pd.read_csv(file, chunksize=100_000, dtype=dtype_map):
            if first_chunk:
                # Create table from first chunk
                con.execute("CREATE TABLE main.API_OLD AS SELECT * FROM chunk")
                first_chunk = False
            else:
                # Insert next chunks
                con.execute("INSERT INTO main.API_OLD SELECT * FROM chunk")
            print(f"Inserted {len(chunk)} rows\n")
    con.close()


if __name__ == "__main__":
    main()
