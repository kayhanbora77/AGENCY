import duckdb
import pandas as pd
import time
from pathlib import Path
import re
from datetime import timedelta
import os

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
ELIMINATED_DATE_FILTER_CSV = BASE + "eliminated_date_filter.csv"
ELIMINATED_SOURCE_DUPLICATES_CSV = BASE + "eliminated_source_duplicates.csv"
ELIMINATED_DB_DUPLICATES_CSV = BASE + "eliminated_db_duplicates.csv"
ELIMINATED_PANDAS_DUPLICATES_CSV = BASE + "eliminated_pandas_duplicates.csv"
TRANSFORMED_ROWS_CSV = BASE + "transformed_rows.csv"

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
        TRANSFORMED_ROWS_CSV,
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
        df.to_csv(ELIMINATED_SOURCE_DUPLICATES_CSV, index=False)


# ==================================================
# CLEAN VIEW
# ==================================================
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


def process_batch(con, offset):
    # 1. FETCH BATCH
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out = []
    eliminated = []

    # 2. FILTER INVALID FLIGHTS
    for row in df.itertuples(index=False):
        if not is_valid_flightno(row.FlightNumber1, row.DepartureDateLocal1):
            eliminated.append({"BookingId": row.BookingId, "Reason": "Invalid flight"})
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

    write_csv(ELIMINATED_NO_FLIGHTS_CSV, eliminated)

    if not out:
        return 0

    # 3. PANDAS DEDUP
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
    df_dedup = df_full.drop_duplicates()

    dropped = df_full[df_full.duplicated()]
    if not dropped.empty:
        dropped["Reason"] = "Pandas duplicate"
        write_csv(ELIMINATED_PANDAS_DUPLICATES_CSV, dropped.to_dict("records"))

    con.register("batch_df", df_dedup)

    # ==================================================
    #   DEEP DEBUG: CHECK TARGET TABLE STATUS
    # ==================================================
    # Check how many rows are in Target RIGHT NOW
    target_count = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]

    # Check if the specific row from your CSV is already in Target
    # We use the values from your provided example: TJ119867230186, SHJ, ATQ
    suspicious_row = con.execute(f"""
        SELECT * 
        FROM {TARGET_TABLE} 
        WHERE BookingId = 'TJ119867230186' 
          AND Airport1 = 'SHJ' 
          AND Airport2 = 'ATQ'
    """).df()

    print(f"\n[DEBUG BATCH {offset}]")
    print(f"  -> Target Table Row Count: {target_count}")
    if not suspicious_row.empty:
        print("  -> FOUND THE DUPLICATE ROW IN TARGET TABLE:")
        print(suspicious_row.to_string(index=False))
    # ==================================================

    # 4. LOG DB DUPLICATES
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

    if not dup_db.empty:
        print(f"  -> {len(dup_db)} DB Duplicates found in this batch.")
        dup_db["Reason"] = "DB duplicate"
        write_csv(ELIMINATED_DB_DUPLICATES_CSV, dup_db.to_dict("records"))
    else:
        print("  -> No DB Duplicates found.")

    # 5. INSERT NON DUP
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

    con = connect_db()
    create_target_table(con)

    create_source_with_dups(con)
    log_source_duplicates(con)
    create_clean_view(con)

    total = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()[0]

    offset = 0
    processed = 0

    while offset < total:
        processed += process_batch(con, offset)
        offset += BATCH_SIZE
        print(f"Processed: {processed:,}")

    print(f"DONE in {(time.time() - start) / 60:.2f} min")


if __name__ == "__main__":
    main()
