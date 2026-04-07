from datetime import timedelta
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

SOURCE_TABLE = "YATRA"
TARGET_TABLE = "YATRA_TARGET"

FLTNO_REGEX = r"^([A-Z]{2,3})0+([1-9][0-9]*)$"

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
            BookingRef VARCHAR,
            BookingDate TIMESTAMP,
            SuperPNR VARCHAR,
            PaxName VARCHAR,
            PNRNo VARCHAR,
            AirlineName VARCHAR,
            TicketNo VARCHAR,
            
            FlightNumberOriginal VARCHAR,
            DepartureDate TIMESTAMP,
            DepartureAirport VARCHAR,
            ArrivalDate TIMESTAMP,
            ArrivalAirport VARCHAR,
            matched_airline VARCHAR,
            FlightNumber VARCHAR,

            CONSTRAINT uq_yatra UNIQUE (
                PNRNo, AirlineName, TicketNo,
                FlightNumberOriginal, DepartureDate, DepartureAirport, ArrivalDate, ArrivalAirport,
                matched_airline, FlightNumber
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
            BookingRef,
            BookingDate,
            SuperPNR,
            PaxName,
            PNRNo,
            AirlineName,
            TicketNo,
            FlightNumberOriginal,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FlightNumber,

            TRY_CAST(DepartureDate AS TIMESTAMP) AS DepartureDate,
            TRY_CAST(ArrivalDate AS TIMESTAMP) AS ArrivalDate,

            DepartureAirport AS DepartureAirport,
            ArrivalAirport AS ArrivalAirport,
        FROM {SOURCE_TABLE}
        WHERE
              (TRY_CAST(DepartureDate AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(ArrivalDate AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
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
    base_data = df[
        [
            "BookingRef",
            "BookingDate",
            "SuperPNR",
            "PaxName",
            "PNRNo",
            "AirlineName",
            "TicketNo",
            "FlightNumberOriginal",
        ]
    ].values

    for idx, row in enumerate(df.itertuples(index=False)):
        flights = []

        for i in range(1, 2):
            flight_number = getattr(row, "FlightNumber")
            departure_date = getattr(row, "DepartureDate")
            arrival_date = getattr(row, "ArrivalDate")
            departure_ap = getattr(row, "DepartureAirport")
            arrival_ap = getattr(row, "ArrivalAirport")
            # FIX: Use pd.isna to catch None, NaN, and NaT (Not a Time)
            if pd.isna(flight_number) or pd.isna(departure_date):
                continue

            flight_number = str(flight_number).strip()

            # ❌ Ignore empty strings
            if not flight_number:
                continue

            # ❌ Ignore purely numeric flight numbers (e.g., "6000000000000")
            if flight_number.isdigit():
                continue

            # ❌ Remove TK000, TK0000, 0000, etc. (Numeric part all zeros)
            stripped = flight_number.rstrip("0")

            # If string is empty (e.g., "0000") -> Drop
            # If string is all alpha (e.g., "TK") -> Drop
            if not stripped or stripped.isalpha():
                continue

            flights.append(
                (flight_number, departure_date, departure_ap, arrival_date, arrival_ap)
            )

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

        for (
            flight_number,
            departure_date,
            departure_ap,
            arrival_date,
            arrival_ap,
        ) in flights:
            # Deduplicate by FlightNo + FlightDate (date-level)
            key = (flight_number, departure_date.date())

            if key not in seen_segments:
                seen_segments.add(key)
                unique_flights.append(
                    (
                        flight_number,
                        departure_date,
                        departure_ap,
                        arrival_date,
                        arrival_ap,
                    )
                )

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
            row_out = list(base_data[idx])
            flight_number_out = [None]
            departure_date_out = [None]
            departure_ap_out = [None]
            arrival_date_out = [None]
            arrival_ap_out = [None]
            matched_airline_out = [None]

            for i, (fn, dt, dep_ap, arr_dt, arr_ap) in enumerate(route[:1]):
                flight_number_out[i] = fn
                departure_date_out[i] = dt
                departure_ap_out[i] = dep_ap
                arrival_date_out[i] = arr_dt
                arrival_ap_out[i] = arr_ap

            out_rows.append(
                row_out
                + departure_date_out
                + departure_ap_out
                + arrival_date_out
                + arrival_ap_out
                + matched_airline_out
                + flight_number_out
            )

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
