import duckdb
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path

# --------------------------------------------------
# CONFIG
# ==================================================
TABLE_NAME = "AGENCY_NOTFOUND_GMT"
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
CHUNK_SIZE = 100_000

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

# --------------------------------------------------
# LOAD AIRPORT TIMEZONES (ONCE)
# --------------------------------------------------
airports = con.execute("""
SELECT iata, timezone
FROM AIRPORTS_ALL
WHERE iata IS NOT NULL AND timezone IS NOT NULL
""").df()

tz_map = dict(zip(airports["iata"], airports["timezone"]))


# --------------------------------------------------
# GMT FUNCTION
# --------------------------------------------------
def calc_gmt_offset(date_val, airport):
    if pd.isna(date_val) or pd.isna(airport):
        return None

    tz_name = tz_map.get(airport)
    if not tz_name:
        return None

    try:
        dt = datetime.combine(date_val, time(12, 0))  # DST-safe
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
        offset = dt.utcoffset().total_seconds() / 3600

        return f"{int(offset):+d}"  # "+2", "-3"

    except Exception:
        return None


def main():
    # --------------------------------------------------
    # STREAM + PROCESS + UPDATE
    # --------------------------------------------------
    print("🚀 Starting chunk processing...")

    query = f"""
    SELECT Id, DepartureDate, DepAirport
    FROM {TABLE_NAME}
    """

    result = con.execute(query)

    chunk_count = 0
    total_rows = 0

    while True:
        chunk = result.fetch_df_chunk(CHUNK_SIZE)

        if chunk is None or len(chunk) == 0:
            break

        # 🔥 normalize aggressively
        chunk.columns = chunk.columns.str.strip().str.lower().str.replace(" ", "")

        # 🔍 safety check
        required_cols = {"id", "departuredate", "depairport"}
        if not required_cols.issubset(set(chunk.columns)):
            print("❌ Missing columns in chunk:", chunk.columns.tolist())
            continue

        # ✅ calculate GMT
        chunk["gmt"] = [
            calc_gmt_offset(d, a)
            for d, a in zip(chunk["departuredate"], chunk["depairport"])
        ]

        con.register("tmp_chunk", chunk)

        con.execute(f"""
        UPDATE {TABLE_NAME} t
        SET GMT = c.gmt
        FROM tmp_chunk c
        WHERE t.Id = c.id
        """)

        print(f"✔ Chunk processed ({len(chunk)} rows)")

        chunk_count += 1
        total_rows += len(chunk)

    print("🎉 DONE!")
    print(f"Total rows processed: {total_rows}")
    print(f"Chunk processed: {chunk_count}")


if __name__ == "__main__":
    main()
