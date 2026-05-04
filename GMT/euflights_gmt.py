import duckdb
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TABLE_NAME = "EU_FLIGHTS"
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
CHUNK_SIZE = 100_000

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

# --------------------------------------------------
# DISCOVER ACTUAL SCHEMA  ← NEW
# --------------------------------------------------
print("=" * 60)
print(f"📋 Schema for {TABLE_NAME}:")
print("=" * 60)
schema_rows = con.execute(f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{TABLE_NAME}'
    ORDER BY ordinal_position
""").fetchall()

for col_name, col_type in schema_rows:
    print(f"  {col_name:40s} {col_type}")
print("=" * 60)

# --------------------------------------------------
# MAP CORRECT COLUMN NAMES  ← FIXED
# --------------------------------------------------
# From the error the real columns are:
#   DepartureIataCode   (not DepAirport)
#   ArrivalIataCode     (not ArrAirport)      — verify from printout above
#   FlightDate
#   GMTDeparture        (column to UPDATE for dep)
#   GMTArrival          (column to UPDATE for arr) — verify from printout above
#
# ⬇️ Adjust these 5 names to match the printed schema ⬇️

COL_ID = "Id"
COL_DEP_AIRPORT = "DepartureIataCode"
COL_ARR_AIRPORT = "ArrivalIataCode"  # ← verify
COL_DEP_TIME = "FlightDate"  # ← date source for GMT calc
COL_GMT_DEP = "GMTDeparture"  # ← column to UPDATE
COL_GMT_ARR = "GMTArrival"  # ← column to UPDATE (if exists)

# --------------------------------------------------
# LOAD AIRPORT TIMEZONES (ONCE)
# --------------------------------------------------
airports = con.execute("""
    SELECT iata, timezone
    FROM AIRPORTS_ALL
    WHERE iata IS NOT NULL AND timezone IS NOT NULL
""").df()

tz_map = dict(zip(airports["iata"].str.strip().str.upper(), airports["timezone"]))


# --------------------------------------------------
# GMT FUNCTION
# --------------------------------------------------
def calc_gmt_offset(date_val, airport):
    if pd.isna(date_val) or pd.isna(airport):
        return None

    tz_name = tz_map.get(str(airport).strip().upper())
    if not tz_name:
        return None

    try:
        # If date_val is a full datetime, extract just the date
        if isinstance(date_val, pd.Timestamp):
            date_only = date_val.date()
        elif isinstance(date_val, datetime):
            date_only = date_val.date()
        else:
            date_only = date_val  # already a date

        dt = datetime.combine(date_only, time(12, 0))
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
        offset = dt.utcoffset().total_seconds() / 3600

        if offset == int(offset):
            return f"{int(offset):+d}"  # "+2", "-3"
        else:
            return f"{offset:+.1f}"  # "+5.5", "-9.5"

    except Exception:
        return None


def main():
    # --------------------------------------------------
    # STREAM + PROCESS + UPDATE
    # --------------------------------------------------
    print("🚀 Starting chunk processing...")

    query = f"""
        SELECT "{COL_ID}"         AS id,
               "{COL_DEP_TIME}"   AS dep_time,
               "{COL_DEP_AIRPORT}" AS dep_airport,
               "{COL_ARR_AIRPORT}" AS arr_airport
        FROM {TABLE_NAME}
    """

    result = con.execute(query)

    chunk_count = 0
    total_rows = 0

    while True:
        chunk = result.fetch_df_chunk(CHUNK_SIZE)

        if chunk is None or len(chunk) == 0:
            break

        chunk.columns = chunk.columns.str.strip().str.lower().str.replace(" ", "")

        # ✅ calculate GMT
        chunk["gmt_dep"] = [
            calc_gmt_offset(dep_time, dep_air)
            for dep_time, dep_air in zip(chunk["dep_time"], chunk["dep_airport"])
        ]

        chunk["gmt_arr"] = [
            calc_gmt_offset(dep_time, arr_air)
            for dep_time, arr_air in zip(chunk["dep_time"], chunk["arr_airport"])
        ]

        update_chunk = chunk[["id", "gmt_dep", "gmt_arr"]]
        con.register("tmp_chunk", update_chunk)

        con.execute(f"""
            UPDATE {TABLE_NAME} t
            SET "{COL_GMT_DEP}" = c.gmt_dep,
                "{COL_GMT_ARR}" = c.gmt_arr
            FROM tmp_chunk c
            WHERE t."{COL_ID}" = c.id
        """)

        con.unregister("tmp_chunk")

        chunk_count += 1
        total_rows += len(chunk)
        print(f"✔ Chunk {chunk_count} processed ({len(chunk)} rows)")

    print("🎉 DONE!")
    print(f"Total rows processed: {total_rows}")
    print(f"Chunks processed:     {chunk_count}")


if __name__ == "__main__":
    main()
