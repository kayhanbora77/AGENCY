import duckdb
import pandas as pd
import time
from pathlib import Path
from datetime import timedelta

# ==================================================
# CONFIG - Easy to change settings
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TBO3_MASTER"
TARGET_TABLE = "TBO3_MASTER_TARGET"

BATCH_SIZE = 500_000  # Increased batch size for fewer iterations

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 4
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"


# ==================================================
# SIMPLE HELPER FUNCTIONS
# ==================================================
def log(msg: str):
    """Print a message with timestamp"""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connect to database with optimized settings"""
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


# ==================================================
# TABLE SETUP
# ==================================================
def create_target_table(con):
    """Create empty target table to store results"""
    log("Creating target table...")
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            PaxName TEXT,
            BookingRef TEXT,
            ETicketNo TEXT,
            ClientCode TEXT,
            Airline TEXT,
            JourneyType TEXT,

            FlightNumber1 TEXT,
            FlightNumber2 TEXT,
            FlightNumber3 TEXT,
            FlightNumber4 TEXT,
            FlightNumber5 TEXT,
            FlightNumber6 TEXT,
            FlightNumber7 TEXT,

            DepartureDateLocal1 TIMESTAMP,
            DepartureDateLocal2 TIMESTAMP,
            DepartureDateLocal3 TIMESTAMP,
            DepartureDateLocal4 TIMESTAMP,
            DepartureDateLocal5 TIMESTAMP,
            DepartureDateLocal6 TIMESTAMP,
            DepartureDateLocal7 TIMESTAMP,

            Airport1 TEXT,
            Airport2 TEXT,
            Airport3 TEXT,
            Airport4 TEXT,
            Airport5 TEXT,
            Airport6 TEXT,
            Airport7 TEXT,
            Airport8 TEXT
        )
    """)


def create_clean_view(con):
    """
    Create a view that:
    1. Cleans flight numbers (removes leading zeros)
    2. Converts dates to proper format
    3. Filters out invalid dates
    """
    log("Creating cleaned data view...")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT *
        FROM (
            SELECT
                PaxName,
                BookingRef,
                ETicketNo,
                ClientCode,
                Airline,
                JourneyType,

                regexp_replace(upper(trim(FlightNumber1)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN1,
                regexp_replace(upper(trim(FlightNumber2)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN2,
                regexp_replace(upper(trim(FlightNumber3)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN3,
                regexp_replace(upper(trim(FlightNumber4)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN4,
                regexp_replace(upper(trim(FlightNumber5)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN5,
                regexp_replace(upper(trim(FlightNumber6)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN6,
                regexp_replace(upper(trim(FlightNumber7)), '^([A-Z]{1, 3})0+([0-9]+)$', '\\1\\2') AS FN7,

                TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) AS DT1,
                TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) AS DT2,
                TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) AS DT3,
                TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) AS DT4,
                TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) AS DT5,
                TRY_CAST(DepartureDateLocal6 AS TIMESTAMP) AS DT6,
                TRY_CAST(DepartureDateLocal7 AS TIMESTAMP) AS DT7,

                Airport1 AS AP1,
                Airport2 AS AP2,
                Airport3 AS AP3,
                Airport4 AS AP4,
                Airport5 AS AP5,
                Airport6 AS AP6,
                Airport7 AS AP7,
                Airport8 AS AP8
            FROM TBO3_MASTER
        )
        WHERE
            (
                DT1 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT2 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT3 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT4 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT5 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT6 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
                OR DT7 BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31'
            )           
    """)


# ==================================================
# ROUTE SPLITTING LOGIC
# ==================================================
def is_same_route(date1, date2):
    """Check if two flights are part of the same journey (within 36 hours)"""
    if date1 is None or date2 is None:
        return False
    return abs(date2 - date1) <= timedelta(hours=36)
    # return abs((date2 - date1).days) <= 1


def split_into_routes(flights):
    """
    Split a list of flights into separate routes.
    Flights are in the same route if they're within 36 hours of each other.
    """
    if not flights:
        return []

    routes = []
    current_route = []

    for flight in flights:
        if not current_route:
            # First flight starts a new route
            current_route.append(flight)
        elif is_same_route(current_route[-1][1], flight[1]):
            # Flight is within 36h of previous - add to current route
            current_route.append(flight)
        else:
            # Flight is more than 36h later - start new route
            routes.append(current_route)
            current_route = [flight]

    # Don't forget the last route
    if current_route:
        routes.append(current_route)

    return routes


def process_batch(con, offset):
    """
    Process one batch of rows:
    1. Get cleaned data from database
    2. Split flights into separate routes
    3. Insert results into target table
    """
    # Step 1: Get data from database
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    output_rows = []

    # Step 2: Process each row
    for row in df.itertuples(index=False):
        # Get the basic info (same for all routes)
        base_info = list(row[:6])  # PaxName, BookingRef, etc.

        # Collect all valid flights from this row
        flights = []
        date_set = set()  # To avoid duplicate dates
        for i in range(1, 8):
            flight_number = getattr(row, f"FN{i}")
            departure_date = getattr(row, f"DT{i}")
            departure_airport = getattr(row, f"AP{i}")
            arrival_airport = getattr(row, f"AP{i + 1}")

            if departure_date in date_set:
                continue
            # Skip invalid flights
            if not flight_number or not departure_date or flight_number.endswith("000"):
                continue

            flights.append(
                (flight_number, departure_date, departure_airport, arrival_airport)
            )
            date_set.add(departure_date)

        if not flights:
            continue

        # Step 3: Split flights into separate routes
        routes = split_into_routes(flights)

        # Step 4: Create one output row per route
        for route in routes:
            output_row = base_info.copy()

            # Initialize empty columns
            flight_nums = [None] * 7
            dates = [None] * 7
            airports = [None] * 8

            # Fill in the data from this route (max 7 flights)
            for i, (fn, dt, dep_ap, arr_ap) in enumerate(route[:7]):
                flight_nums[i] = fn
                dates[i] = dt
                airports[i] = dep_ap
                if i + 1 < len(airports):
                    airports[i + 1] = arr_ap

            # Combine all data
            output_row.extend(flight_nums)
            output_row.extend(dates)
            output_row.extend(airports)
            output_rows.append(output_row)

    if not output_rows:
        return 0

    # Step 5: Insert into target table
    df_output = pd.DataFrame(output_rows)
    con.register("batch_data", df_output)
    con.execute(f"INSERT INTO {TARGET_TABLE} SELECT * FROM batch_data")
    con.unregister("batch_data")

    return len(output_rows)


# ==================================================
# MAIN EXECUTION
# ==================================================
def main():
    start_time = time.time()
    log("Starting ETL process...")

    # Connect and setup
    con = connect_db()
    create_target_table(con)
    create_clean_view(con)

    # Get total rows to process
    total_rows = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()[0]
    log(f"Total rows to process: {total_rows:,}")

    # Process in batches
    offset = 0
    batch_number = 0
    total_inserted = 0

    while offset < total_rows:
        batch_number += 1
        log(
            f"Processing batch {batch_number}: rows {offset:,} to {min(offset + BATCH_SIZE, total_rows):,}"
        )

        rows_inserted = process_batch(con, offset)
        total_inserted += rows_inserted

        log(f"  → Inserted {rows_inserted:,} output rows")
        offset += BATCH_SIZE

    # Finish
    elapsed_seconds = time.time() - start_time
    log("✓ ETL Complete!")
    log(f"  Total output rows: {total_inserted:,}")
    log(f"  Execution time: {elapsed_seconds / 60:.1f} minutes")

    con.close()


if __name__ == "__main__":
    main()
