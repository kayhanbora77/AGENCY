import time
from pathlib import Path

import duckdb
import pandas as pd

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
EXCEL_DIR = Path("/home/kayhan/Desktop/EU_Eligible/")
TIME_TABLE_NAME = "TIME_LIMITS"
STANDARD_TABLE_NAME = "TA_Standard_Template"
AIRPORT_TABLE_NAME = "AIRPORTS"


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


def load_eu_eligible_data(con):
    try:
        data = con.execute(
            """
            SELECT 
                t.Id,
                t.ConnectionID,
                t.AirlineCode,
                t.FromAirport,
                t.ToAirport,
                depAirport.NameCountry AS FromCountry,
                arrAirport.NameCountry AS ToCountry,
                fromTimeLimits.LimitationYears AS fromCountryLimit,
                toTimeLimits.LimitationYears AS toCountryLimit,
                COUNT(*) OVER (PARTITION BY t.ConnectionID) AS LegCount
            FROM TA_STANDARD_TEMPLATE t
            LEFT JOIN AIRPORTS depAirport
                ON t.FromAirport = depAirport.CodeIataAirport
            LEFT JOIN AIRPORTS arrAirport
                ON t.ToAirport = arrAirport.CodeIataAirport
            LEFT JOIN TIME_LIMITS fromTimeLimits
                ON depAirport.NameCountry = fromTimeLimits.Country
            LEFT JOIN TIME_LIMITS toTimeLimits
                ON arrAirport.NameCountry = toTimeLimits.Country
            WHERE t.EUEligible IS TRUE
            ORDER BY t.ConnectionID;            
            """
        ).fetchall()

        return data

    except Exception as e:
        log(f"Error loading EU eligible data: {e}")
        return None


def set_time_limit(con, connection_id, L1, L2):
    try:
        con.execute(
            f"""
            UPDATE TA_STANDARD_TEMPLATE
            SET TIMELIMITL1 = {L1},
                TIMELIMITL2 = {L2}
            WHERE ConnectionID = {connection_id}
            """
        )
    except Exception as e:
        log(f"Error setting time limit: {e}")


def process_multi_leg(con, multi_leg):
    for row in multi_leg:
        leg_count = row[11]
        limit1 = 0
        limit2 = 0
        for i in range(leg_count + 1):
            if i == 0:  # first leg
                fromL1 = row[7]
                # toL1 = row[8]
                fromL2 = row[9]
                # toL2 = row[10]
            elif i == leg_count:  # last leg
                toL1 = row[8]
                toL2 = row[10]
        if fromL1 > toL1:
            limit1 = fromL1            
        else    
            limit1 = toL1
        if fromL2 > toL2:
            limit2 = fromL2
        else:
            limit2 = toL2
        set_time_limit(con, row[1], limit1, limit2)


def process_single_leg(con, single_leg):
    row = single_leg[0]
    fromL1 = row[7]
    toL1 = row[8]
    fromL2 = row[9]
    toL2 = row[10]
    if fromL1 > toL1:
        limit1 = fromL1            
    else:
        limit1 = toL1
    if fromL2 > toL2:
        limit2 = fromL2
    else:
        limit2 = toL2
    set_time_limit(con, row[1], limit1, limit2)


def main():
    log(f"Starting at {now_str()}")

    con = connect_db()
    data = load_eu_eligible_data(con)
    if not data:
        log("No data found")
        return
    # seperate flights
    single_leg = [row for row in data if row[11] == 1]
    multi_leg = [row for row in data if row[11] > 1]
    process_multi_leg(con, multi_leg)
    process_single_leg(con, single_leg)
        

    log(f"Total flights: {len(data)}")
    log(f"Single leg flights: {len(single_leg)}")
    log(f"Multi leg flights: {len(multi_leg)}")

    log(f"Connected to database at {now_str()}")


if __name__ == "__main__":
    main()
