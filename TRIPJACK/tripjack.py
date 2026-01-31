import duckdb
import pandas as pd
import time
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TRIPJACK"
TARGET_TABLE = "TRIPJACK_TARGET"

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
            Airport6 VARCHAR
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

            NULLIF(regexp_replace(trim(upper(FlightNumber1)),'^([A-Z][A-Z][A-Z]?)0*([0-9]+)$',
                    '\\1\\2'),'') AS FN1,
            NULLIF(regexp_replace(trim(upper(FlightNumber2)),'^([A-Z][A-Z][A-Z]?)0*([0-9]+)$',
                    '\\1\\2'),'') AS FN2,
            NULLIF(regexp_replace(trim(upper(FlightNumber3)),'^([A-Z][A-Z][A-Z]?)0*([0-9]+)$',
                    '\\1\\2'),'') AS FN3,
            NULLIF(regexp_replace(trim(upper(FlightNumber4)),'^([A-Z][A-Z][A-Z]?)0*([0-9]+)$',
                    '\\1\\2'),'') AS FN4,
            NULLIF(regexp_replace(trim(upper(FlightNumber5)),'^([A-Z][A-Z][A-Z]?)0*([0-9]+)$',
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


# ==================================================
# ROUTE LOGIC
# ==================================================
def same_route(d1, d2):
    return abs((d2 - d1).total_seconds()) <= 36 * 3600


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

            # ❌ Pair mismatch → ignore
            if fn is None or dt is None:
                continue

            fn = str(fn).strip()

            # ❌ Ignore flight numbers ending with "000"
            if not fn or fn.endswith("000"):
                continue

            depAp = getattr(row, f"AP{i}")
            arrAp = getattr(row, f"AP{i + 1}")

            flights.append((fn, dt, depAp, arrAp))

        # ❌ No valid (FltNo + FltDate) pairs → skip row
        if not flights:
            continue

        # Group flights into routes
        routes = []
        current = [flights[0]]

        for f in flights[1:]:
            if same_route(current[-1][1], f[1]):
                current.append(f)
            else:
                routes.append(current)
                current = [f]

        routes.append(current)

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
    con.execute(f"INSERT INTO {TARGET_TABLE} SELECT * FROM df_out")

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
