from datetime import timedelta
import duckdb
import pandas as pd
import time
from pathlib import Path
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "BLUESTAR"
TARGET_TABLE = "BLUESTAR_TARGET"

FLTNO_REGEX = r"^([A-Z]{2,3})0+([1-9][0-9]*)$"
MAX_FLTNO_DIGITS = 10

BATCH_SIZE = 100_000

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
            BillDate TIMESTAMP_NS,
            PaxName VARCHAR,
            PNRNo VARCHAR,
            AirlineName VARCHAR,
            TicketNo VARCHAR,
            
            FltNo1 VARCHAR,
            FltNo2 VARCHAR,
            FltNo3 VARCHAR,
            FltNo4 VARCHAR,

            FltDate1 TIMESTAMP_NS,
            FltDate2 TIMESTAMP_NS,
            FltDate3 TIMESTAMP_NS,
            FltDate4 TIMESTAMP_NS,

            SupplierName VARCHAR,
            PaxType VARCHAR,            

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,

            CONSTRAINT uq_bluestar UNIQUE (
                PNRNo, AirlineName, TicketNo,
                FltNo1, FltNo2, FltNo3, FltNo4,
                FltDate1, FltDate2, FltDate3, FltDate4,
                Airport1, Airport2, Airport3, Airport4, Airport5
            )
        )
    """)


def get_total_rows(con) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def create_clean_view(con):
    log("🧹 Creating cleaned source view")

    # Note: We use {{ }} for regex groups inside the python f-string
    # We use \\1\\2 for backreferences in the replacement string
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT
            BillDate,
            PaxName,
            PNRNo,
            AirlineName,
            TicketNo,
            
            CASE
                WHEN regexp_matches(
                    replace(trim(upper(FltNo1)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$'
                )
                THEN regexp_replace(
                    replace(trim(upper(FltNo1)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$',
                    '\\1\\2'
                )
                ELSE replace(trim(upper(FltNo1)), ' ', '')
            END AS FN1,

            CASE
                WHEN regexp_matches(
                    replace(trim(upper(FltNo2)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$'
                )
                THEN regexp_replace(
                    replace(trim(upper(FltNo2)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$',
                    '\\1\\2'
                )
                ELSE replace(trim(upper(FltNo2)), ' ', '')
            END AS FN2,

            CASE
                WHEN regexp_matches(
                    replace(trim(upper(FltNo3)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$'
                )
                THEN regexp_replace(
                    replace(trim(upper(FltNo3)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$',
                    '\\1\\2'
                )
                ELSE replace(trim(upper(FltNo3)), ' ', '')
            END AS FN3,

            CASE
                WHEN regexp_matches(
                    replace(trim(upper(FltNo4)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$'
                )
                THEN regexp_replace(
                    replace(trim(upper(FltNo4)), ' ', ''),
                    '^([A-Z]{{2,3}})0+([1-9][0-9]*)$',
                    '\\1\\2'
                )
                ELSE replace(trim(upper(FltNo4)), ' ', '')
            END AS FN4,

            TRY_CAST(FltDate1 AS TIMESTAMP) AS DT1,
            TRY_CAST(FltDate2 AS TIMESTAMP) AS DT2,
            TRY_CAST(FltDate3 AS TIMESTAMP) AS DT3,
            TRY_CAST(FltDate4 AS TIMESTAMP) AS DT4,

            SupplierName,
            PaxType,

            Airport1 AS AP1,
            Airport2 AS AP2,
            Airport3 AS AP3,
            Airport4 AS AP4,
            Airport5 AS AP5
        FROM {SOURCE_TABLE}
        WHERE
            (TRY_CAST(FltDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
    """)


# ==================================================
# ROUTE LOGIC
# ==================================================
def same_route(d1, d2):
    # Check for NaT (Not a Time) before calculation
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs(d2 - d1) <= timedelta(days=1)


def process_batch(con, offset):
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out_rows = []
    base_data1 = df[
        [
            "BillDate",
            "PaxName",
            "PNRNo",
            "AirlineName",
            "TicketNo",
        ]
    ].values
    base_data2 = df[
        [
            "SupplierName",
            "PaxType",
        ]
    ].values

    for idx, row in enumerate(df.itertuples(index=False)):
        flights = []

        for i in range(1, 5):
            fn = getattr(row, f"FN{i}")
            dt = getattr(row, f"DT{i}")

            # FIX: Use pd.isna to catch None, NaN, and NaT (Not a Time)
            if pd.isna(fn) or pd.isna(dt):
                continue

            fn = str(fn).strip()

            # ❌ Ignore empty strings
            if not fn:
                continue

            # ❌ Corrupted huge numeric value → drop whole row
            if fn.isdigit() and len(fn) > MAX_FLTNO_DIGITS:
                flights = []
                break

            # ❌ Remove TK000, TK0000, 0000, etc. (Numeric part all zeros)
            stripped = fn.rstrip("0")

            # If string is empty (e.g., "0000") -> Drop
            # If string is all alpha (e.g., "TK") -> Drop
            if not stripped or stripped.isalpha():
                continue

            fn = fn.strip().upper()
            # ❌ Reject invalid short flight numbers (G8, 6P, I5, etc.)
            # Valid format: 2-3 letters + at least 1 digit (minimum length 3)
            if not re.fullmatch(r"[A-Z0-9]{2,3}\d+", fn):
                continue

            depAp = getattr(row, f"AP{i}")
            arrAp = getattr(row, f"AP{i + 1}")

            flights.append((fn, dt, depAp, arrAp))

        # ❌ No valid (FltNo + FltDate) pairs → skip row
        if not flights:
            continue

        # ==================================================
        # SORTING LOGIC
        # ==================================================
        # Sort flights by Date ONLY (x[1]).
        flights.sort(key=lambda x: x[1])

        # ==================================================
        # DEDUPLICATE EXACT FLIGHT SEGMENTS (CORRECT)
        # ==================================================
        seen_segments = set()
        unique_flights = []

        for fn, dt, dep_ap, arr_ap in flights:
            # Deduplicate by FlightNo + FlightDate (date-level)
            key = (fn, dt.date())

            if key not in seen_segments:
                seen_segments.add(key)
                unique_flights.append((fn, dt, dep_ap, arr_ap))

        flights = unique_flights

        # ==================================================
        # GROUP FLIGHTS INTO ROUTES (MAX 1-DAY SPAN)
        # ==================================================
        routes = []

        current = [flights[0]]
        route_start_date = flights[0][1]

        for f in flights[1:]:
            if abs(f[1] - route_start_date) <= timedelta(days=1):
                current.append(f)
            else:
                routes.append(current)
                current = [f]
                route_start_date = f[1]

        # IMPORTANT: append the last route
        routes.append(current)

        # Build output rows
        for route in routes:
            base1 = list(base_data1[idx])
            base2 = list(base_data2[idx])
            fn_out = [None] * 4
            dt_out = [None] * 4
            ap_out = [None] * 5

            for i, (fn, dt, dep_ap, arr_ap) in enumerate(route[:4]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                if i + 1 < len(ap_out):
                    ap_out[i + 1] = arr_ap

            # out_rows.append(row_out + fn_out + dt_out + ap_out)
            out_rows.append(base1 + fn_out + dt_out + base2 + ap_out)

    if not out_rows:
        return 0

    df_out = pd.DataFrame(out_rows, dtype="object")
    df_out.replace("", None, inplace=True)

    unique_indices = [2, 3, 4] + list(range(5, 13)) + list(range(15, 20))

    initial_rows = len(df_out)
    df_out = df_out.drop_duplicates(subset=unique_indices)
    dropped_rows = initial_rows - len(df_out)

    if dropped_rows > 0:
        log(f"🗑️  Dropped {dropped_rows} duplicate rows within batch")

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
