import time
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np
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

# Global cached set (faster lookup)
EU_AIRPORTS: set = set()

SPECIAL_NON_EU_CARRIERS = {
    "BA",  # British Airways
    "TK",  # Turkish Airlines
    "PC",  # Pegasus
    "JU",  # AirSerbian
    "H2",  # Sky Airline
    "FH",  # Freebird Airlines
    "VF",  # AJet (AnadoluJet)
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
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


def load_airports(con: duckdb.DuckDBPyConnection, force_refresh: bool = False) -> set:
    global EU_AIRPORTS
    if EU_AIRPORTS and not force_refresh:
        return EU_AIRPORTS

    try:
        result = con.execute("SELECT CodeIataAirport FROM AIRPORTS").fetchall()
        EU_AIRPORTS = {r[0].strip().upper() for r in result if r[0]}
        log(f"Loaded {len(EU_AIRPORTS):,} EU airport codes")
    except Exception as e:
        log(f"Error loading airports: {e}")
        EU_AIRPORTS = set()

    return EU_AIRPORTS


def init_db(con: duckdb.DuckDBPyConnection):
    load_airports(con)


def get_eu_eligible_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame | None:
    log("Loading EU eligible data...")

    query = """
        SELECT
            t.ConnectionID,
            t.LegNo,
            t.AirlineCode,
            dep.NameCountry AS FromCountry,
            arr.NameCountry AS ToCountry,
            COALESCE(fromL1.LimitationYears, 0) AS fromL1,
            COALESCE(toL1.LimitationYears, 0) AS toL1,
            COALESCE(fromL2.LimitationYears, 0) AS fromL2,
            COALESCE(toL2.LimitationYears, 0) AS toL2
        FROM TA_STANDARD_TEMPLATE t
        LEFT JOIN AIRPORTS dep ON t.FromAirport = dep.CodeIataAirport
        LEFT JOIN AIRPORTS arr ON t.ToAirport = arr.CodeIataAirport
        LEFT JOIN TIME_LIMITL1 fromL1 ON dep.NameCountry = fromL1.Country
        LEFT JOIN TIME_LIMITL1 toL1 ON arr.NameCountry = toL1.Country
        LEFT JOIN TIME_LIMITL2 fromL2 ON dep.NameCountry = fromL2.Country
        LEFT JOIN TIME_LIMITL2 toL2 ON arr.NameCountry = toL2.Country
        WHERE t.EUEligible IS TRUE
        ORDER BY t.ConnectionID, t.LegNo
    """

    df = con.execute(query).df()

    if df.empty:
        log("No EU eligible records found.")
        return None

    log(f"Loaded {len(df):,} legs")
    return df


def get_updated_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "TimeLimitL1", "TimeLimitL2"])

    # Ensure correct order
    df = df.sort_values(["ConnectionID", "LegNo"], ignore_index=True)

    # Safe numeric conversion
    num_cols = ["fromL1", "toL1", "fromL2", "toL2"]
    df[num_cols] = (
        df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("int32")
    )

    log("Computing time limits per connection...")

    # Aggregate per connection
    agg = df.groupby("ConnectionID", as_index=False).agg(
        dep_L1=("fromL1", "first"),
        dep_L2=("fromL2", "first"),
        arr_L1=("toL1", "last"),
        arr_L2=("toL2", "last"),
        FromCountry=("FromCountry", "first"),
        ToCountry=("ToCountry", "last"),
        AirlineCode=("AirlineCode", "first"),
    )

    # Default limits
    agg["TimeLimitL1"] = agg[["dep_L1", "arr_L1"]].max(axis=1)
    agg["TimeLimitL2"] = agg[["dep_L2", "arr_L2"]].max(axis=1)

    mask_from_missing = agg["FromCountry"].isna() & agg["ToCountry"].notna()
    mask_to_missing = agg["FromCountry"].notna() & agg["ToCountry"].isna()

    if mask_from_missing.any():
        agg.loc[mask_from_missing, "TimeLimitL1"] = agg.loc[mask_from_missing, "arr_L1"]
        agg.loc[mask_from_missing, "TimeLimitL2"] = agg.loc[mask_from_missing, "arr_L2"]

    if mask_to_missing.any():
        agg.loc[mask_to_missing, "TimeLimitL1"] = agg.loc[mask_to_missing, "dep_L1"]
        agg.loc[mask_to_missing, "TimeLimitL2"] = agg.loc[mask_to_missing, "dep_L2"]
        # Special override logic
        both_missing = agg["FromCountry"].isna() & agg["ToCountry"].isna()

    if both_missing.any():
        log(f"Found {both_missing.sum():,} connections with both countries missing")

        special_mask = both_missing & agg["AirlineCode"].isin(
            SPECIAL_NON_EU_TIME_LIMITS.keys()
        )

        if special_mask.any():
            log(
                f" → Overriding {special_mask.sum():,} connections with special carrier limits"
            )

            # Extract codes as numpy array (fast & safe)
            codes = agg.loc[special_mask, "AirlineCode"].values

            # Create numpy arrays of integers (this avoids pandas Series/index problems)
            l1_arr = np.array(
                [SPECIAL_NON_EU_TIME_LIMITS[c][0] for c in codes], dtype=np.int32
            )
            l2_arr = np.array(
                [SPECIAL_NON_EU_TIME_LIMITS[c][1] for c in codes], dtype=np.int32
            )

            # Assign using plain numpy arrays → very reliable
            agg.loc[special_mask, "TimeLimitL1"] = l1_arr
            agg.loc[special_mask, "TimeLimitL2"] = l2_arr

    df_updates = agg[["ConnectionID", "TimeLimitL1", "TimeLimitL2"]]

    log(f"Prepared {len(df_updates):,} updates")
    return df_updates


def set_time_limits(con: duckdb.DuckDBPyConnection, df_updates: pd.DataFrame):
    if df_updates.empty:
        log("No updates to apply")
        return

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
        log(f"Batch UPDATE completed successfully ({len(df_updates):,} rows)")
    except Exception as e:
        con.rollback()
        log(f"UPDATE failed: {e}")
        raise


def main():
    start_time = time.time()
    log("Starting process")

    con = connect_db()
    init_db(con)

    df = get_eu_eligible_data(con)

    if df is not None:
        df_updates = get_updated_data(df)
        set_time_limits(con, df_updates)

        elapsed = time.time() - start_time
        log("──────────── DONE ────────────")
        log(f"Updated {len(df_updates):,} connections")
        log(f"Processed {len(df):,} legs")
        log(f"Finished in {elapsed:.2f} seconds")
    else:
        log("No data to process")

    con.close()


if __name__ == "__main__":
    main()
