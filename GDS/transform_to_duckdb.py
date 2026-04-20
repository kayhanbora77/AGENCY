import sys
import pandas as pd
import duckdb
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE = "/home/kayhan/Desktop/Gelen_Datalar/NEW_DATA_MURAT/GDS/Last6MonthsGDS.xlsx"
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

TABLE_NAME = "GDS"
CHUNK_SIZE = 100_000
# ─────────────────────────────────────────────────────────────────────────────

# Keep ticket/flight numbers as strings so leading zeros aren't dropped
COLUMN_DTYPES = {
    "TicketNumber": str,
    "PNR": str,
    "FlightNo1": str,
    "FlightNo2": str,
    "FlightNo3": str,
    "FlightNo4": str,
}

DATE_COLUMNS = [
    "TicketDate",
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
]


def sanitize(name: str) -> str:
    """Turn a sheet/column name into a safe SQL identifier."""
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def load_xlsx_to_duckdb(xlsx_path: str, db_path: str) -> None:
    sheets: dict[str, pd.DataFrame] = pd.read_excel(
        xlsx_path,
        sheet_name=None,
        dtype=COLUMN_DTYPES,
        parse_dates=DATE_COLUMNS,
    )
    print(f"Found {len(sheets)} sheet(s) in '{xlsx_path}': {list(sheets.keys())}")

    con = duckdb.connect(db_path)

    for sheet_name, df in sheets.items():
        # Sanitize column names
        df.columns = [sanitize(str(c)) for c in df.columns]

        # Drop and recreate table (idempotent)
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(f"""
            CREATE TABLE {TABLE_NAME} AS
            SELECT gen_random_uuid()::UUID AS id, *
            FROM df
        """)

        row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(
            f"\n  ✓ Table '{TABLE_NAME}': {row_count} rows, {len(df.columns) + 1} columns (incl. id)"
        )

        # Print schema
        schema = con.execute(f"DESCRIBE {TABLE_NAME}").fetchall()
        print(f"  {'Column':<20} {'Type'}")
        print(f"  {'-' * 20} {'-' * 15}")
        for col_name, col_type, *_ in schema:
            print(f"  {col_name:<20} {col_type}")

    tables = con.execute("SHOW TABLES").fetchall()
    print(f"\nTables in '{db_path}': {[t[0] for t in tables]}")

    # Sample output
    print(f"\nSample — first 3 rows of '{TABLE_NAME}':")
    print(
        con.execute(f"""
        SELECT id, ticketnumber, paxname, airlinename
        FROM {TABLE_NAME}
        LIMIT 3
    """)
        .df()
        .to_string(index=False)
    )

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    load_xlsx_to_duckdb(INPUT_FILE, DB_PATH)
