import duckdb
import pandas as pd
import re
from pathlib import Path
import time

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TBO3_MASTER"
TARGET_TABLE = "TBO3_MASTER_TARGET"

BATCH_SIZE = 200_000

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def create_target_table(con: duckdb.DuckDBPyConnection) -> None:
    log("♻️ Creating target table")

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
        );
    """)


def get_total_rows(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def get_target_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]


def is_rnk(flight: str) -> bool:
    if not isinstance(flight, str):
        return False
    return flight.endswith("000")


def normalize_flight_number(fn: str) -> str | None:
    if not isinstance(fn, str):
        return None
    fn = fn.strip().upper()
    m = re.fullmatch(r"([A-Z]{2,3})(0*)(\d+)", fn)
    if not m:
        return fn
    airline, _, number = m.groups()
    return f"{airline}{int(number)}"


def normalize_date(value) -> str | None:
    d = pd.to_datetime(value, errors="coerce")
    if pd.isna(d):
        return None
    if d.year < VALID_YEAR_MIN or d.year > VALID_YEAR_MAX:
        return None
    return d.to_pydatetime()


def same_route(d1, d2):
    if d1 is None or d2 is None:
        return False
    return abs((d2 - d1).days) <= 1


def process_batch(con: duckdb.DuckDBPyConnection, offset: int) -> int:
    query = f"""
        SELECT *
        FROM {SOURCE_TABLE}
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """

    df = con.execute(query).df()
    if df.empty:
        return

    inserted = 0
    out_rows = []
    for _, row in df.iterrows():
        base_columns = {
            "PaxName": row["PaxName"],
            "BookingRef": row["BookingRef"],
            "ETicketNo": row["ETicketNo"],
            "ClientCode": row["ClientCode"],
            "Airline": row["Airline"],
            "JourneyType": row["JourneyType"],
        }

        unique_flights = set()
        cleaned_raw = []
        for i in range(1, 8):
            fn = row.get(f"FlightNumber{i}")
            dt = row.get(f"DepartureDateLocal{i}")
            ap = row.get(f"Airport{i}")

            if not fn or not dt:
                continue

            fn_clean = fn.strip().upper()
            if is_rnk(fn_clean):
                continue

            key = (fn_clean, dt)
            if key in unique_flights:
                continue
            unique_flights.add(key)
            cleaned_raw.append((fn_clean, dt, ap))

        flights = []
        for fn_clean, dt, ap in cleaned_raw:
            n_fn = normalize_flight_number(fn_clean)
            n_dt = normalize_date(dt)
            if n_fn and n_dt:
                flights.append((n_fn, n_dt, ap))

        if not flights:
            continue

        routes = []
        current = []
        for fn, dt, ap in flights:
            if not current:
                current.append((fn, dt, ap))
                continue
            prev_fn, prev_dt, _ = current[-1]
            if fn == prev_fn and dt == prev_dt:
                continue
            if same_route(prev_dt, dt):
                current.append((fn, dt, ap))
            else:
                routes.append(current)
                current = [(fn, dt, ap)]
        if current:
            routes.append(current)

        for route in routes:
            new_row = base_columns.copy()

            for i in range(1, 8):
                new_row[f"FlightNumber{i}"] = None
                new_row[f"DepartureDateLocal{i}"] = None
                new_row[f"Airport{i}"] = None
            new_row["Airport8"] = None

            seen = set()
            compacted = []
            for fn, dt, ap in route:
                key = (fn, dt)
                if key in seen:
                    continue
                seen.add(key)
                compacted.append((fn, dt, ap))

            for i, (fn, dt, ap) in enumerate(compacted, start=1):
                if i > 7:
                    break
                new_row[f"FlightNumber{i}"] = fn
                new_row[f"DepartureDateLocal{i}"] = dt
                new_row[f"Airport{i}"] = ap

            out_rows.append(new_row)

    df_out = pd.DataFrame(out_rows)
    if not df_out.empty:
        con.register("df_out_view", df_out)
        con.execute(f"""
            INSERT INTO {TARGET_TABLE} (
                PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4, FlightNumber5, FlightNumber6, FlightNumber7,
                DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3, DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6, DepartureDateLocal7,
                Airport1, Airport2, Airport3, Airport4, Airport5, Airport6, Airport7, Airport8
            )
            SELECT 
                PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4, FlightNumber5, FlightNumber6, FlightNumber7,
                DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3, DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6, DepartureDateLocal7,
                Airport1, Airport2, Airport3, Airport4, Airport5, Airport6, Airport7, Airport8
            FROM df_out_view
        """)
        con.unregister("df_out_view")
        inserted += len(df_out)

    return inserted


def main() -> None:
    start_time = time.time()
    log(f"🚀 Starting at {now_str()}")

    con = connect_db()
    create_target_table(con)

    total_rows = get_total_rows(con)
    log(f"📊 Total source records: {total_rows:,}")

    offset = 0
    batch_no = 0

    while offset < total_rows:
        batch_no += 1
        log(f"🔄 Batch {batch_no} | rows {offset:,} → {offset + BATCH_SIZE:,}")

        process_batch(con, offset)

        offset += BATCH_SIZE

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time

    # Convert to hours, minutes, seconds
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int(execution_time % 60)
    log(f"⏰ End Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    log(
        f"⏱️  Execution Time: {hours:02d} hours, {minutes:02d} minutes, {seconds:02d} seconds"
    )

    log("🎉 FINAL ETL COMPLETED SUCCESSFULLY")
    con.close()


if __name__ == "__main__":
    main()
