import os
import time
from pathlib import Path

import duckdb

import glob

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
TABLE_NAME = "TRAVELPACK"
CSV_DIR = Path("/home/kayhan/Desktop/Gelen_Datalar/TRAVELPACK/PROCEED/")


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def get_csv_row_count(csv_path: str) -> int:
    """Get the number of rows in a CSV file (excluding header)"""
    try:
        con = duckdb.connect()
        result = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto(
                '{csv_path}',
                all_varchar=true,
                encoding='utf-8',
                ignore_errors=true,
                header=true
            )
        """).fetchone()
        con.close()
        return result[0] if result else 0
    except:
        return 0


def load_csv_files(con) -> None:
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    if not csv_files:
        print("❌ No CSV files found.")
        return

    print(f"📁 Found {len(csv_files)} CSV file(s).\n")

    # Display row counts for all files
    print("📊 CSV FILE ROW COUNTS:")
    print("-" * 60)
    total_csv_rows = 0
    file_row_counts = {}

    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        row_count = get_csv_row_count(csv_path)
        file_row_counts[csv_path] = row_count
        total_csv_rows += row_count
        print(f"  {filename:<40} {row_count:>10,} rows")

    print("-" * 60)
    print(f"  {'TOTAL:':<40} {total_csv_rows:>10,} rows")
    print()

    # Load files into database
    print("📥 LOADING CSV FILES INTO DATABASE:")
    print("-" * 60)

    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    for i, csv_path in enumerate(csv_files):
        filename = os.path.basename(csv_path)
        expected_rows = file_row_counts[csv_path]

        print(f"\n📄 Processing {i + 1}/{len(csv_files)}: {filename}")
        print(f"   Expected rows: {expected_rows:,}")

        start_time = time.time()

        if i == 0:
            # For the first file, create the table
            con.execute(f"""
            CREATE TABLE "{TABLE_NAME}" AS
            SELECT
                PaxName,
                AirlineBooking,
                Airline,
                ETicketNo,
                Direction,
                FlightNumber1,
                FlightNumber2,
                try_strptime(FlightDate1, '%d/%m/%Y')::TIMESTAMP AS FlightDate1,
                try_strptime(FlightDate2, '%d/%m/%Y')::TIMESTAMP AS FlightDate2,
                Airport1,
                Airport2,
                Airport3,
                TpackRef                
            FROM read_csv_auto(
                '{csv_path}',
                all_varchar=true,
                encoding='utf-8',
                ignore_errors=true,
                sample_size=100000,
                header=true
            )
            """)
        else:
            # For subsequent files, insert into the existing table
            con.execute(f"""
            INSERT INTO "{TABLE_NAME}"
            SELECT
                PaxName,
                AirlineBooking,
                Airline,
                ETicketNo,
                Direction,
                FlightNumber1,
                FlightNumber2,                
                try_strptime(FlightDate1, '%d/%m/%Y')::TIMESTAMP AS FlightDate1,
                try_strptime(FlightDate2, '%d/%m/%Y')::TIMESTAMP AS FlightDate2,
                Airport1,
                Airport2,
                Airport3,
                TpackRef
            FROM read_csv_auto(
                '{csv_path}',
                all_varchar=true,
                encoding='utf-8',
                ignore_errors=true,
                sample_size=100000,
                header=true
            )
            """)

        elapsed = time.time() - start_time

        # Get actual rows loaded (simplified count check)
        result = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()
        actual_rows = result[0] if result else 0

        rows_added = (
            actual_rows
            if i == 0
            else actual_rows - sum(list(file_row_counts.values())[:i])
        )

        print(f"   Rows loaded: {rows_added:,} in {elapsed:.2f} seconds")

    # Final statistics
    print("\n" + "=" * 60)
    print("📊 FINAL DATABASE STATISTICS:")
    print("=" * 60)

    result = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()
    total_db_rows = result[0] if result else 0

    print(f"Total rows in database: {total_db_rows:,}")

    if total_db_rows < total_csv_rows:
        skipped = total_csv_rows - total_db_rows
        print(
            f"Rows skipped due to errors: {skipped:,} ({skipped / total_csv_rows * 100:.2f}%)"
        )

    print("=" * 60)


def main() -> None:
    start = time.time()
    log(f"⏰ Starting at {now_str()}")

    # Create directories if they don't exist
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    con = connect_db()
    load_csv_files(con)
    con.close()

    log(f"\n⏰ Finished at {now_str()}")
    elapsed = time.time() - start
    log(f"🎉 Loading completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")


if __name__ == "__main__":
    main()
