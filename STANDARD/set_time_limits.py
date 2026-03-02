import time
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = Path(tempfile.gettempdir()) / "duckdb_temp"

# ────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────
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


# ────────────────────────────────────────────────
# UTILITIES
# ────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_connection() -> duckdb.DuckDBPyConnection:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


# ────────────────────────────────────────────────
# DATA LOADING
# ────────────────────────────────────────────────
def get_eu_eligible_data(con: duckdb.DuckDBPyConnection) -> Optional[pd.DataFrame]:
    log("Loading EU eligible data...")

    query = """
        SELECT
            t.ConnectionID,
            t.AirlineCode,
            depAirport.NameCountry AS depCountry,
            arrAirport.NameCountry AS arrCountry,
            COALESCE(depLimits.LimitL1, 0) AS depL1,
            COALESCE(depLimits.LimitL2, 0) AS depL2,
            COALESCE(arrLimits.LimitL1, 0) AS arrL1,
            COALESCE(arrLimits.LimitL2, 0) AS arrL2,
            t.LegNo
        FROM TA_STANDARD_TEMPLATE t
        LEFT JOIN AIRPORTS depAirport
            ON t.FromAirport = depAirport.CodeIataAirport
        LEFT JOIN AIRPORTS arrAirport
            ON t.ToAirport = arrAirport.CodeIataAirport
        LEFT JOIN TIME_LIMITS depLimits
            ON depAirport.NameCountry = depLimits.Country
        LEFT JOIN TIME_LIMITS arrLimits
            ON arrAirport.NameCountry = arrLimits.Country
        WHERE t.EUEligible IS TRUE
        ORDER BY t.ConnectionID, t.LegNo
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        log(f"Error fetching data: {e}")
        return None

    if df.empty:
        log("No EU eligible records found.")
        return None

    log(f"Loaded {len(df):,} legs")
    return df


# ────────────────────────────────────────────────
# BUSINESS LOGIC
# ────────────────────────────────────────────────
def calculate_timelimits(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "TimeLimitL1", "TimeLimitL2"])

    num_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[num_cols] = (
        df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype("int32")
    )

    # -------------------------------------------------
    # STEP 1 — First dep & last arr per connection
    # -------------------------------------------------
    agg = df.groupby("ConnectionID", as_index=False).agg(
        first_dep_L1=("depL1", "first"),
        first_dep_L2=("depL2", "first"),
        last_arr_L1=("arrL1", "last"),
        last_arr_L2=("arrL2", "last"),
    )

    # Default calculation
    agg["TimeLimitL1"] = agg[["first_dep_L1", "last_arr_L1"]].max(axis=1)
    agg["TimeLimitL2"] = agg[["first_dep_L2", "last_arr_L2"]].max(axis=1)

    # -------------------------------------------------
    # STEP 2 — Identify connections needing special rule
    # -------------------------------------------------
    mask_zero = (
        (agg["first_dep_L1"] == 0)
        & (agg["last_arr_L1"] == 0)
        & (agg["first_dep_L2"] == 0)
        & (agg["last_arr_L2"] == 0)
    )

    if mask_zero.any():
        # -------------------------------------------------
        # STEP 3 — Map airlines to special limits
        # -------------------------------------------------
        special_df = df[["ConnectionID", "AirlineCode"]].drop_duplicates()

        special_df["SpecialLimits"] = special_df["AirlineCode"].map(
            SPECIAL_NON_EU_TIME_LIMITS
        )

        # Keep only airlines that exist in dict
        special_df = special_df[special_df["SpecialLimits"].notna()]

        if not special_df.empty:
            # Split tuple into columns
            special_df[["SpecL1", "SpecL2"]] = pd.DataFrame(
                special_df["SpecialLimits"].tolist(), index=special_df.index
            )

            # Get max per connection
            special_max = (
                special_df.groupby("ConnectionID")[["SpecL1", "SpecL2"]]
                .max()
                .reset_index()
            )

            # Merge into agg
            agg = agg.merge(special_max, on="ConnectionID", how="left")

            # Apply only to zero-mask rows
            agg.loc[mask_zero & agg["SpecL1"].notna(), "TimeLimitL1"] = agg["SpecL1"]
            agg.loc[mask_zero & agg["SpecL2"].notna(), "TimeLimitL2"] = agg["SpecL2"]

            agg = agg.drop(columns=["SpecL1", "SpecL2"], errors="ignore")

    return agg[["ConnectionID", "TimeLimitL1", "TimeLimitL2"]]


"""
def calculate_timelimits(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ConnectionID", "TimeLimitL1", "TimeLimitL2"])

    log("Computing time limits per connection...")

    # Ensure deterministic order before groupby
    df = df.sort_values(["ConnectionID", "LegNo"])

    # Cast numeric columns
    num_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[num_cols] = df[num_cols].astype("int32")

    # Aggregate per connection
    agg = df.groupby("ConnectionID", as_index=False).agg(
        dep_L1=("depL1", "first"),
        dep_L2=("depL2", "first"),
        arr_L1=("arrL1", "last"),
        arr_L2=("arrL2", "last"),
        DepCountry=("depCountry", "first"),
        ArrCountry=("arrCountry", "last"),
        AirlineCode=("AirlineCode", "first"),
    )

    # Default limits
    agg["TimeLimitL1"] = agg[["dep_L1", "arr_L1"]].max(axis=1)
    agg["TimeLimitL2"] = agg[["dep_L2", "arr_L2"]].max(axis=1)

    # Special carrier override (vectorized)
    mask_missing = agg["DepCountry"].isna() & agg["ArrCountry"].isna()
    mask_special = mask_missing & agg["AirlineCode"].isin(SPECIAL_NON_EU_TIME_LIMITS)

    if mask_special.any():
        mapped = agg.loc[mask_special, "AirlineCode"].map(SPECIAL_NON_EU_TIME_LIMITS)
        agg.loc[mask_special, ["TimeLimitL1", "TimeLimitL2"]] = pd.DataFrame(
            mapped.tolist(), index=agg.loc[mask_special].index
        )

    return agg[["ConnectionID", "TimeLimitL1", "TimeLimitL2"]]
"""


# ────────────────────────────────────────────────
# DATABASE UPDATE
# ────────────────────────────────────────────────
def set_time_limits(con: duckdb.DuckDBPyConnection, df_updates: pd.DataFrame) -> None:
    if df_updates.empty:
        log("No updates to apply")
        return

    log(f"Updating {len(df_updates):,} connections...")

    try:
        con.execute("BEGIN")
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
        con.execute("COMMIT")
        log("Batch UPDATE completed successfully")

    except Exception:
        con.execute("ROLLBACK")
        log("UPDATE failed — transaction rolled back")
        raise


def main():
    start_time = time.time()
    log("Starting process")

    con = get_connection()

    try:
        df = get_eu_eligible_data(con)

        if df is not None:
            df_updates = calculate_timelimits(df)
            set_time_limits(con, df_updates)

            log("──────────── DONE ────────────")
            log(f"Updated {len(df_updates):,} connections")
            log(f"Processed {len(df):,} legs")
            log(f"Finished in {time.time() - start_time:.2f} seconds")
        else:
            log("No data to process")

    finally:
        # Ensure the connection closes even if an error occurs
        con.close()


if __name__ == "__main__":
    main()
