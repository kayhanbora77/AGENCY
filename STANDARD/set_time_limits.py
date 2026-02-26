import time
from pathlib import Path

import duckdb
import pandas as pd
from typing import Any


# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"

# Global cached sets (faster lookup)
EU_AIRPORTS: set = set()

SPECIAL_NON_EU_CARRIERS = {
    "BA",  # British Airways
    "TK",  # Turkish Airlines
    "PC",  # Pegasus
    "JU",  # AirSerbian
    "H2",  # Sky Airline
    "FH",  # Freebird Airlines
    "VF",  # AJet(Anadolu jet)
    "VS",  # Virgin Atlantic
}

SPECIAL_NON_EU_TIME_LIMITS = {
    "BA": (6, 6),
    "TK": (2, 2),
    "PC": (2, 2),
    "JU": (2, 2),
    "H2": (2, 2),
    "FH": (2, 2),
    "VF": (2, 2),
    "VS": (6, 6),
}


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


def is_eu_airport(airport: Any) -> bool:
    if airport is None or pd.isna(airport):
        return False
    code = str(airport).upper().strip()
    return code in EU_AIRPORTS


def load_airports(con: duckdb.DuckDBPyConnection, force_refresh=False) -> set:
    global EU_AIRPORTS
    if EU_AIRPORTS and not force_refresh:
        return EU_AIRPORTS

    try:
        result = con.execute("SELECT CodeIataAirport FROM AIRPORTS").fetchall()
        EU_AIRPORTS = {r[0].strip().upper() for r in result if r[0]}
    except Exception as e:
        print(f"Error loading airports: {e}")
        EU_AIRPORTS = set()

    return EU_AIRPORTS


def __init__(con: duckdb.DuckDBPyConnection):
    load_airports(con)


def get_eu_eligible_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    log("Loading EU eligible data...")

    query = """
        SELECT
            t.ConnectionID,
            t.LegNo,
            t.AirlineCode,
            dep.NameCountry AS FromCountry,
            arr.NameCountry AS ToCountry,
            COALESCE(fromL1.LimitationYears, 0) AS fromL1,
            COALESCE(toL1.LimitationYears,   0) AS toL1,
            COALESCE(fromL2.LimitationYears, 0) AS fromL2,
            COALESCE(toL2.LimitationYears,   0) AS toL2
        FROM TA_STANDARD_TEMPLATE t
        LEFT JOIN AIRPORTS dep ON t.FromAirport = dep.CodeIataAirport
        LEFT JOIN AIRPORTS arr ON t.ToAirport   = arr.CodeIataAirport
        LEFT JOIN TIME_LIMITL1 fromL1 ON dep.NameCountry = fromL1.Country
        LEFT JOIN TIME_LIMITL1   toL1 ON arr.NameCountry =   toL1.Country
        LEFT JOIN TIME_LIMITL2 fromL2 ON dep.NameCountry = fromL2.Country
        LEFT JOIN TIME_LIMITL2   toL2 ON arr.NameCountry =   toL2.Country
        WHERE t.EUEligible IS TRUE
        ORDER BY t.ConnectionID, t.LegNo
    """

    df = con.execute(query).df()

    if df.empty:
        log("No EU eligible records found.")
        con.close()
        return

    log(f"Loaded {len(df):,} legs")

    return df


def get_updated_data(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure correct order (safety belt even with ORDER BY in query)
    df = df.sort_values(["ConnectionID", "LegNo"], ignore_index=True)

    # Make sure numeric (defensive)
    num_cols = ["fromL1", "toL1", "fromL2", "toL2"]
    df[num_cols] = (
        df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("int32")
    )

    log("Computing time limits per connection...")

    # Find first & last leg per group using LegNo
    first_idx = df.groupby("ConnectionID")["LegNo"].idxmin()
    last_idx = df.groupby("ConnectionID")["LegNo"].idxmax()

    first = df.loc[first_idx, ["ConnectionID", "fromL1", "fromL2"]].rename(
        columns={"fromL1": "dep_L1", "fromL2": "dep_L2"}
    )
    last = df.loc[last_idx, ["ConnectionID", "toL1", "toL2"]].rename(
        columns={"toL1": "arr_L1", "toL2": "arr_L2"}
    )
    fromCountry = df.loc[first_idx, ["ConnectionID", "FromCountry"]]
    toCountry = df.loc[last_idx, ["ConnectionID", "ToCountry"]]

    merged = first.merge(last, on="ConnectionID")

    merged["TimeLimitL1"] = merged[["dep_L1", "arr_L1"]].max(axis=1)
    merged["TimeLimitL2"] = merged[["dep_L2", "arr_L2"]].max(axis=1)

    df_updates = merged[["ConnectionID", "TimeLimitL1", "TimeLimitL2"]]

    if None in fromCountry.values and None in toCountry.values:
        airlineCode = df.loc["AirlineCode"]
        limits = SPECIAL_NON_EU_TIME_LIMITS.get(airlineCode)
        if limits:
            log("Special non-EU carrier detected")
            df_updates = merged[["ConnectionID"]].copy()
            df_updates["TimeLimitL1"] = limits[0]
            df_updates["TimeLimitL2"] = limits[1]

    log(f"Prepared {len(df_updates):,} updates")

    return df_updates


def set_time_limits(con: duckdb.DuckDBPyConnection, df_updates: pd.DataFrame):
    # ── Transactional update ───────────────────────────────
    con.begin()

    try:
        con.register("temp_updates", df_updates)

        con.execute("""
            UPDATE TA_STANDARD_TEMPLATE t
               SET TimeLimitL1 = u.TimeLimitL1,
                   TimeLimitL2 = u.TimeLimitL2
             FROM temp_updates u
            WHERE t.ConnectionID = u.ConnectionID
              AND t.EUEligible IS TRUE
        """)

        con.unregister("temp_updates")
        con.commit()
        log("Batch UPDATE completed successfully")

    except Exception as e:
        con.rollback()
        log(f"UPDATE failed: {e}")
        raise


def main():
    start_time = time.time()
    log(f"Starting at {now_str()}")

    con = get_connection()

    __init__(con)

    df = get_eu_eligible_data(con)

    df_updates = get_updated_data(df)

    set_time_limits(con, df_updates)

    elapsed = time.time() - start_time

    log("──────────── DONE ────────────")
    log(f"Updated {len(df_updates):,} connections")
    log(f"Processed {len(df):,} legs")
    log(f"Finished in {elapsed:.2f} seconds")
    log(f"Completed at {now_str()}")

    con.close()


if __name__ == "__main__":
    main()
