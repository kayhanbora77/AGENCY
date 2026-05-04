import duckdb
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path

# --- CONFIG ---
TABLE_NAME = "EU_FLIGHTS"
DB_PATH = Path.home() / "my_database" / "my_db.duckdb"

COL_ID = "Id"
COL_DEP_AIRPORT = "DepartureIataCode"
COL_ARR_AIRPORT = "ArrivalIataCode"
COL_DEP_TIME = "ScheduledDepartureTime"
COL_GMT_DEP = "GMTDeparture"
COL_GMT_ARR = "GMTArrival"

con = duckdb.connect(str(DB_PATH))

# 1. LOAD TIMEZONE MAP & VALIDATE
# --------------------------------------------------
airports = con.execute(
    "SELECT iata, timezone FROM AIRPORTS_ALL WHERE iata IS NOT NULL"
).df()
# Normalize keys to stripped uppercase
tz_map = dict(zip(airports["iata"].str.strip().str.upper(), airports["timezone"]))

print(f"📊 Reference Airports loaded: {len(tz_map)}")
print(f"Sample mapping check (LHR): {tz_map.get('LHR')}")


def calc_gmt_offset(date_val, airport):
    if pd.isna(date_val) or pd.isna(airport):
        return None
    airport_key = str(airport).strip().upper()
    tz_name = tz_map.get(airport_key)
    if not tz_name:
        return None
    try:
        dt = date_val.date() if hasattr(date_val, "date") else date_val
        dt_obj = datetime.combine(dt, time(12, 0)).replace(tzinfo=ZoneInfo(tz_name))
        offset = dt_obj.utcoffset().total_seconds() / 3600
        return f"{int(offset):+d}" if offset == int(offset) else f"{offset:+.1f}"
    except:
        return None


# 2. BUILD MAPPING TABLE
# --------------------------------------------------
print("🔍 Extracting unique airport/date combinations...")
unique_pairs = con.execute(f"""
    SELECT DISTINCT CAST("{COL_DEP_TIME}" AS DATE) as d_date, TRIM("{COL_DEP_AIRPORT}") as airport FROM "{TABLE_NAME}"
    UNION
    SELECT DISTINCT CAST("{COL_DEP_TIME}" AS DATE) as d_date, TRIM("{COL_ARR_AIRPORT}") as airport FROM "{TABLE_NAME}"
""").df()

print(f"⚙ Processing {len(unique_pairs)} offsets...")
unique_pairs["offset_val"] = unique_pairs.apply(
    lambda x: calc_gmt_offset(x["d_date"], x["airport"]), axis=1
)

# --- DEBUG: CHECK IF CALCULATION WORKED ---
calculated_count = unique_pairs["offset_val"].notnull().sum()
print(f"✅ Offsets successfully calculated: {calculated_count} / {len(unique_pairs)}")

if calculated_count == 0:
    print(
        "❌ ERROR: No offsets were calculated. Check if AIRPORTS_ALL contains the IATAs from EU_FLIGHTS."
    )
    # Show what we actually have
    print("Sample unique_pairs data:")
    print(unique_pairs.head())
    exit()

con.register("map_table", unique_pairs)

# 3. EXECUTE UPDATES WITH FORCED MATCHING
# --------------------------------------------------
print("💾 Updating GMT Departure...")
con.execute(f"""
    UPDATE "{TABLE_NAME}" 
    SET "{COL_GMT_DEP}" = m.offset_val
    FROM map_table m
    WHERE CAST("{TABLE_NAME}"."{COL_DEP_TIME}" AS DATE) = CAST(m.d_date AS DATE) 
      AND TRIM("{TABLE_NAME}"."{COL_DEP_AIRPORT}") = m.airport
""")

print("💾 Updating GMT Arrival...")
con.execute(f"""
    UPDATE "{TABLE_NAME}" 
    SET "{COL_GMT_ARR}" = m.offset_val
    FROM map_table m
    WHERE CAST("{TABLE_NAME}"."{COL_DEP_TIME}" AS DATE) = CAST(m.d_date AS DATE) 
      AND TRIM("{TABLE_NAME}"."{COL_ARR_AIRPORT}") = m.airport
""")

# 4. FINAL VERIFICATION
# --------------------------------------------------
res = con.execute(f"""
    SELECT "{COL_DEP_AIRPORT}", "{COL_GMT_DEP}", "{COL_GMT_ARR}" 
    FROM "{TABLE_NAME}" 
    WHERE "{COL_GMT_DEP}" IS NOT NULL 
    LIMIT 5
""").df()

if res.empty:
    print("❌ Still Null! Checking for column content mismatch...")
    db_airport = con.execute(
        f'SELECT "{COL_DEP_AIRPORT}" FROM "{TABLE_NAME}" LIMIT 1'
    ).fetchone()[0]
    print(f"Sample Airport in DB: '{db_airport}' (Length: {len(str(db_airport))})")
else:
    print("🎉 Success! Sample of updated rows:")
    print(res)
