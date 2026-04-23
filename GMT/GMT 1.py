import duckdb
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
DB_PATH = r"D:\DuckDB\my_database.duckdb"
CSV_PATH = r"C:\Users\Last Airbender\Desktop\TSC_Filtered.csv"
TARGET_TABLE = "TSC_with_gmt"

MAX_SEGMENTS = 6

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

# --------------------------------------------------
# LOAD CSV
# --------------------------------------------------
df = pd.read_csv(CSV_PATH)

# --------------------------------------------------
# LOAD AIRPORT TIMEZONES
# --------------------------------------------------
airports = con.execute("""
SELECT iata, timezone
FROM airports
""").df()

tz_map = dict(zip(airports["iata"], airports["timezone"]))

# --------------------------------------------------
# GMT OFFSET FUNCTION
# --------------------------------------------------
def calc_gmt_offset(flight_date, airport_iata):

    if pd.isna(flight_date) or pd.isna(airport_iata):
        return None

    tz_name = tz_map.get(airport_iata)
    if not tz_name:
        return None

    # normalize date
    if isinstance(flight_date, str):
        flight_date = datetime.fromisoformat(flight_date).date()
    elif isinstance(flight_date, datetime):
        flight_date = flight_date.date()

    dt = datetime.combine(flight_date, time(12, 0))
    dt = dt.replace(tzinfo=ZoneInfo(tz_name))

    return dt.utcoffset().total_seconds() / 3600


# --------------------------------------------------
# PROCESS SEGMENTS
# --------------------------------------------------
for i in range(1, MAX_SEGMENTS + 1):

    date_col = f"S{i}Date"
    airport_col = f"Airport{i}"
    gmt_col = f"S{i}_GMT"

    if date_col not in df.columns or airport_col not in df.columns:
        continue

    df[gmt_col] = df.apply(
        lambda r: calc_gmt_offset(r[date_col], r[airport_col]),
        axis=1
    )

    print(f"✔ Segment {i} processed")


# --------------------------------------------------
# WRITE TO DUCKDB
# --------------------------------------------------
con.register("tmp_flights", df)

con.execute(f"""
CREATE OR REPLACE TABLE {TARGET_TABLE} AS
SELECT * FROM tmp_flights
""")

print(f"🎉 Data written to DuckDB table: {TARGET_TABLE}")
