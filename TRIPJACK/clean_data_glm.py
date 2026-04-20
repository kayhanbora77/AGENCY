import duckdb
import pandas as pd
import time
from pathlib import Path
import re
import os
from datetime import timedelta

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TRIPJACK"
TARGET_TABLE = "TRIPJACK_TARGET"

BATCH_SIZE = 100_000
MAX_FLTNO_DIGITS = 8
FLTNO_REGEX = r"^([A-Z]{2,3}|\d[A-Z])0+([1-9][0-9]*)$"
ROUTE_MAX_DAYS = 1

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

# ==================================================
# CSV PATHS
# ==================================================
BASE = "/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/FILTER-1(CLEANED)/"

ELIMINATED_NO_FLIGHTS_CSV = BASE + "eliminated_no_flights.csv"
ELIMINATED_DATE_FILTER_CSV = BASE + "eliminated_date_filter.csv"  # Now used
ELIMINATED_SOURCE_DUPLICATES_CSV = BASE + "eliminated_source_duplicates.csv"
ELIMINATED_DB_DUPLICATES_CSV = BASE + "eliminated_db_duplicates.csv"
ELIMINATED_PANDAS_DUPLICATES_CSV = BASE + "eliminated_pandas_duplicates.csv"
TRANSFORMED_ROWS_CSV = BASE + "transformed_rows.csv"
SUMMARY_LOG_FILE = BASE + "elimination_summary.txt"

_csv_headers_written = set()


def write_csv(path, rows):
    if not rows:
        return
    df = pd.DataFrame(rows)
    write_header = path not in _csv_headers_written
    df.to_csv(path, mode="a", index=False, header=write_header)
    _csv_headers_written.add(path)


def reset_csv_files():
    for f in [
        ELIMINATED_NO_FLIGHTS_CSV,
        ELIMINATED_DATE_FILTER_CSV,
        ELIMINATED_SOURCE_DUPLICATES_CSV,
        ELIMINATED_DB_DUPLICATES_CSV,
        ELIMINATED_PANDAS_DUPLICATES_CSV,
        SUMMARY_LOG_FILE,  # Also clear the summary log
    ]:
        if os.path.exists(f):
            os.remove(f)


# ==================================================
# DB
# ==================================================
def connect_db():
    return duckdb.connect(DB_PATH)


def create_target_table(con):
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            BookingId VARCHAR,
            PaxName VARCHAR,
            JourneyBucket VARCHAR,
            BookingRef_PNR VARCHAR,
            Airline VARCHAR,
            ETicketNo VARCHAR,
            FlightNumber1 VARCHAR,
            FlightDate1 TIMESTAMP,
            Airport1 VARCHAR,
            Airport2 VARCHAR,
            CONSTRAINT uq UNIQUE(BookingId, PaxName, FlightNumber1, FlightDate1, Airport1, Airport2)
        )
    """)


# ==================================================
# DUPLICATE SOURCE DETECTION
# ==================================================
def create_source_with_dups(con):
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW source_with_dups AS
        SELECT *,
               COUNT(*) OVER (
                   PARTITION BY 
                       BookingId, 
                       PaxName, 
                       FlightNumber1, 
                       DepartureDateLocal1, 
                       Airport1, 
                       Airport2
               ) AS dup_count
        FROM {SOURCE_TABLE}
    """)


def log_source_duplicates(con):
    # Log the duplicates to CSV
    df = con.execute("""
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY BookingId, PaxName, FlightNumber1, DepartureDateLocal1, Airport1, Airport2
                    ORDER BY BookingId
                ) AS rn
            FROM source_with_dups
        )
        WHERE rn > 1
    """).df()

    if not df.empty:
        df["Reason"] = "Duplicate in source"
        write_csv(ELIMINATED_SOURCE_DUPLICATES_CSV, df.to_dict("records"))

    # Return the count
    return len(df)


def create_clean_view(con):
    con.execute("""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (                    
                    PARTITION BY BookingId, PaxName, FlightNumber1, DepartureDateLocal1, Airport1, Airport2
                    ORDER BY BookingId
                ) AS rn
            FROM source_with_dups
        )
        WHERE rn = 1
    """)


# ==================================================
# VALIDATION
# ==================================================
def is_valid_flightno(fn, dt):
    if pd.isna(fn) or pd.isna(dt):
        return False
    fn = str(fn).strip().upper()
    return bool(re.fullmatch(r"[A-Z0-9]{2,3}\d+", fn))


def is_valid_date(dt):
    """New validation based on your config settings"""
    if pd.isna(dt):
        return False
    try:
        year = dt.year
        return VALID_YEAR_MIN <= year <= VALID_YEAR_MAX
    except:
        return False


# ==================================================
# PROCESS
# ==================================================
def process_batch(con, offset, stats):
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out = []
    eliminated = []

    for row in df.itertuples(index=False):
        # 1. Flight No Validation
        if not is_valid_flightno(row.FlightNumber1, row.DepartureDateLocal1):
            eliminated.append(
                {"BookingId": row.BookingId, "Reason": "Invalid flight number"}
            )
            stats["invalid_flight"] += 1
            continue

        # 2. Date Validation (Newly Added)
        if not is_valid_date(row.DepartureDateLocal1):
            eliminated.append(
                {
                    "BookingId": row.BookingId,
                    "DepartureDate": row.DepartureDateLocal1,
                    "Reason": "Invalid date (year)",
                }
            )
            stats["invalid_date"] += 1
            continue

        out.append(
            (
                row.BookingId,
                row.PaxName,
                row.JourneyBucket,
                row.BookingRef_PNR,
                row.Airline,
                row.ETicketNo,
                row.FlightNumber1,
                row.DepartureDateLocal1,
                row.Airport1,
                row.Airport2,
            )
        )

    # Write Invalid Rows to CSVs
    write_csv(
        ELIMINATED_NO_FLIGHTS_CSV,
        [r for r in eliminated if "flight" in r["Reason"].lower()],
    )
    write_csv(
        ELIMINATED_DATE_FILTER_CSV,
        [r for r in eliminated if "date" in r["Reason"].lower()],
    )

    if not out:
        return 0

    columns = [
        "BookingId",
        "PaxName",
        "JourneyBucket",
        "BookingRef_PNR",
        "Airline",
        "ETicketNo",
        "FlightNumber1",
        "DepartureDateLocal1",
        "Airport1",
        "Airport2",
    ]
    df_full = pd.DataFrame(out, columns=columns)

    # 3. Pandas Dedup (Batch-level duplicates)
    df_dedup = df_full.drop_duplicates()
    dropped_count = len(df_full) - len(df_dedup)
    stats["pandas_dups"] += dropped_count

    if dropped_count > 0:
        dropped_df = df_full[df_full.duplicated()]
        dropped_df["Reason"] = "Pandas duplicate (within batch)"
        write_csv(ELIMINATED_PANDAS_DUPLICATES_CSV, dropped_df.to_dict("records"))

    con.register("batch_df", df_dedup)

    # 4. DB Duplicate Check (Cross-batch duplicates)
    # These are rows in the current batch that match rows inserted in PREVIOUS batches
    dup_db = con.execute(f"""
        SELECT b.*
        FROM batch_df b
        JOIN {TARGET_TABLE} t
        ON b.BookingId = t.BookingId
           AND b.PaxName = t.PaxName
           AND b.FlightNumber1 = t.FlightNumber1           
           AND CAST(b.DepartureDateLocal1 AS DATE) = CAST(t.FlightDate1 AS DATE)
           AND b.Airport1 = t.Airport1
           AND b.Airport2 = t.Airport2
    """).df()

    dup_db_count = len(dup_db)
    stats["db_dups"] += dup_db_count

    if not dup_db.empty:
        dup_db["Reason"] = "DB duplicate (match with previous batch)"
        write_csv(ELIMINATED_DB_DUPLICATES_CSV, dup_db.to_dict("records"))

    # 5. Insert Non-Duplicates
    con.execute(f"""
        INSERT INTO {TARGET_TABLE}
        SELECT *
        FROM batch_df b
        WHERE NOT EXISTS (
            SELECT 1 FROM {TARGET_TABLE} t
            WHERE b.BookingId = t.BookingId
              AND b.PaxName = t.PaxName
              AND b.FlightNumber1 = t.FlightNumber1
              AND CAST(b.DepartureDateLocal1 AS DATE) = CAST(t.FlightDate1 AS DATE)              
              AND b.Airport1 = t.Airport1
              AND b.Airport2 = t.Airport2
        )
    """)

    return len(df_dedup)


# ==================================================
# MAIN
# ==================================================
def main():
    start = time.time()
    reset_csv_files()

    # Stats Dictionary
    stats = {
        "raw_rows": 0,
        "source_dups": 0,
        "invalid_flight": 0,
        "invalid_date": 0,
        "pandas_dups": 0,
        "db_dups": 0,
        "inserted": 0,
    }

    con = connect_db()
    create_target_table(con)

    # 1. Count Raw Rows
    stats["raw_rows"] = con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[
        0
    ]

    # 2. Create Views and Calculate Source Duplicates
    create_source_with_dups(con)
    stats["source_dups"] = log_source_duplicates(con)
    create_clean_view(con)

    # 3. Processing Loop
    total_clean = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()[0]

    offset = 0

    print(f"Starting Process...")
    print(f"Total Raw Rows: {stats['raw_rows']:,}")
    print(f"Total Clean Rows (after source dedup): {total_clean:,}")
    print("-" * 50)

    while offset < total_clean:
        inserted_in_batch = process_batch(con, offset, stats)
        stats["inserted"] += inserted_in_batch

        offset += BATCH_SIZE
        if offset % 500000 == 0:
            print(f"Processed Offset: {offset:,}")

    # 4. Write Summary Log
    print("-" * 50)
    print("GENERATING SUMMARY LOG...")

    with open(SUMMARY_LOG_FILE, "w") as f:
        f.write("ELIMINATION REPORT\n")
        f.write("==================\n\n")
        f.write(f"1. RAW SOURCE ROWS           : {stats['raw_rows']:,}\n")
        f.write(f"   (-) Source Duplicates      : {stats['source_dups']:,}\n")
        f.write(f"   -----------------------\n")
        f.write(
            f"2. ENTERED BATCH PROCESSING : {stats['raw_rows'] - stats['source_dups']:,}\n"
        )
        f.write(f"   (-) Invalid Flights        : {stats['invalid_flight']:,}\n")
        f.write(f"   (-) Invalid Dates          : {stats['invalid_date']:,}\n")
        f.write(f"   (-) Pandas Duplicates      : {stats['pandas_dups']:,}\n")
        f.write(f"   (-) DB/Cross-Batch Dups   : {stats['db_dups']:,}\n")
        f.write(f"   -----------------------\n")
        f.write(f"3. FINAL INSERTED ROWS        : {stats['inserted']:,}\n")
        f.write(
            f"\nTOTAL ELIMINATED              : {stats['raw_rows'] - stats['inserted']:,}\n"
        )

    # Also print to console
    with open(SUMMARY_LOG_FILE, "r") as f:
        print(f.read())

    print(f"DONE in {(time.time() - start) / 60:.2f} min")
    print(f"Summary log saved to: {SUMMARY_LOG_FILE}")


if __name__ == "__main__":
    main()
