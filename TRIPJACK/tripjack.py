import duckdb
import pandas as pd
import time
from pathlib import Path
import re
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

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"


# ==================================================
# UTILS
# ==================================================
def log(msg: str):
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ==================================================
# DATABASE
# ==================================================
def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def create_target_table(con):
    log("♻️ Creating target table")
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
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            FlightNumber5 VARCHAR,

            FlightDate1 TIMESTAMP_NS,
            FlightDate2 TIMESTAMP_NS,
            FlightDate3 TIMESTAMP_NS,
            FlightDate4 TIMESTAMP_NS,
            FlightDate5 TIMESTAMP_NS,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,
            Airport6 VARCHAR,

            CONSTRAINT uq_tripjack UNIQUE (
                BookingRef_PNR, Airline, ETicketNo,
                FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4, FlightNumber5,
                FlightDate1, FlightDate2, FlightDate3, FlightDate4, FlightDate5,
                Airport1, Airport2, Airport3, Airport4, Airport5, Airport6
            )
        )
    """)


def get_total_rows(con) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def create_clean_view(con):
    log("🧹 Creating cleaned source view")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT
            BookingId,
            PaxName,
            JourneyBucket,
            BookingRef_PNR,
            Airline,
            ETicketNo,

            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber1)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN1,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber2)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN2,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber3)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN3,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber4)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN4,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber5)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN5,

            TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) AS DT1,
            TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) AS DT2,
            TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) AS DT3,
            TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) AS DT4,
            TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) AS DT5,

            Airport1 AS AP1,
            Airport2 AS AP2,
            Airport3 AS AP3,
            Airport4 AS AP4,
            Airport5 AS AP5,
            Airport6 AS AP6
        FROM {SOURCE_TABLE}
        WHERE   
              (TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
    """)


def is_same_route(d1, d2):
    # Check for NaT (Not a Time) before calculation
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs(d2 - d1) <= timedelta(days=ROUTE_MAX_DAYS)


def is_valid_flightno(fn: str, dt: str) -> bool:
    if pd.isna(fn) or pd.isna(dt):
        return False

    fn = str(fn).strip()

    # ❌ Ignore empty strings
    if not fn:
        return False

    # ❌ Corrupted huge numeric value → drop whole row
    if fn.isdigit():
        return False
    if len(fn) > MAX_FLTNO_DIGITS:
        return False
    # ❌ Remove TK000, TK0000, 0000, etc. (Numeric part all zeros)
    stripped = fn.rstrip("0")

    # If string is empty (e.g., "0000") -> Drop
    # If string is all alpha (e.g., "TK") -> Drop
    if not stripped or stripped.isalpha():
        return False

    fn = fn.strip().upper()
    # ❌ Reject invalid short flight numbers (G8, 6P, I5, etc.)
    # Valid format: 2-3 letters + at least 1 digit (minimum length 3)
    if not re.fullmatch(r"[A-Z0-9]{2,3}\d+", fn):
        return False
    return True


def deduplicate_flights(flights):
    # Sort flights by Date ONLY (x[1]).
    flights.sort(key=lambda x: x[1])
    seen_segments = set()
    unique_flights = []

    for fn, dt, dep_ap, arr_ap in flights:
        # Deduplicate by FlightNo + FlightDate (date-level)
        key = (fn, dt.date())

        if key not in seen_segments:
            seen_segments.add(key)
            unique_flights.append((fn, dt, dep_ap, arr_ap))

    return unique_flights


def group_into_routes(flights):
    routes = []

    current = [flights[0]]
    route_start_date = flights[0][1]

    for f in flights[1:]:
        if is_same_route(f[1], route_start_date):
            current.append(f)
        else:
            routes.append(current)
            current = [f]
            route_start_date = f[1]

    # IMPORTANT: append the last route
    routes.append(current)

    return routes


def process_batch(con, offset):
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out_rows = []
    base_data = df[
        [
            "BookingId",
            "PaxName",
            "JourneyBucket",
            "BookingRef_PNR",
            "Airline",
            "ETicketNo",
        ]
    ].values

    for idx, row in enumerate(df.itertuples(index=False)):
        flights = []

        for i in range(1, 6):
            fn = getattr(row, f"FN{i}")
            dt = getattr(row, f"DT{i}")

            if not is_valid_flightno(fn, dt):
                continue

            depAp = getattr(row, f"AP{i}")
            arrAp = getattr(row, f"AP{i + 1}")

            flights.append((fn, dt, depAp, arrAp))

        # ❌ No valid (FltNo + FltDate) pairs → skip row
        if not flights:
            continue

        flights = deduplicate_flights(flights)

        routes = group_into_routes(flights)

        # Build output rows
        for route in routes:
            row_out = list(base_data[idx])
            fn_out = [None] * 5
            dt_out = [None] * 5
            ap_out = [None] * 6

            for i, (fn, dt, dep_ap, arr_ap) in enumerate(route[:6]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                if i + 1 < len(ap_out):
                    ap_out[i + 1] = arr_ap

            out_rows.append(row_out + fn_out + dt_out + ap_out)

    if not out_rows:
        return 0

    df_out = pd.DataFrame(out_rows, dtype="object")
    con.execute(f"INSERT OR IGNORE INTO {TARGET_TABLE} SELECT * FROM df_out")

    return len(out_rows)


# ==================================================
# MAIN
# ==================================================
def main():
    start = time.time()
    log(f"🚀 Start {now_str()}")

    con = connect_db()
    create_target_table(con)
    create_clean_view(con)

    result = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()
    total = result[0] if result else 0
    log(f"📊 Cleaned rows: {total:,}")

    offset = 0
    batch = 0
    processed = 0

    while offset < total:
        batch += 1
        batch_start = time.time()
        log(f"🔄 Batch {batch} | {offset:,} → {min(offset + BATCH_SIZE, total):,}")

        rows_processed = process_batch(con, offset)
        processed += rows_processed
        offset += BATCH_SIZE

        batch_time = time.time() - batch_start
        progress = (offset / total) * 100
        eta = (batch_time * (total - offset) / BATCH_SIZE) / 3600

        log(f"✅ Processed {rows_processed:,} rows | {progress:.1f}% | ETA: {eta:.2f}h")

    elapsed = time.time() - start
    log(f"📊 Total processed: {processed:,} rows")
    log(f"⏱️ Execution Time: {elapsed / 3600:.2f} hours")
    log("🎉 ETL COMPLETED")

    con.close()


if __name__ == "__main__":
    main()
