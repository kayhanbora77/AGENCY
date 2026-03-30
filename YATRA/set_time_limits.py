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
            strftime(t.DepartureDate, '%Y-%m-%d') AS DepartureDate,
            depAirport.NameCountry AS depCountry,
            arrAirport.NameCountry AS arrCountry,
            COALESCE(depLimits.LimitL1, 0) AS depL1,
            COALESCE(depLimits.LimitL2, 0) AS depL2,
            COALESCE(arrLimits.LimitL1, 0) AS arrL1,
            COALESCE(arrLimits.LimitL2, 0) AS arrL2,
            t.LegNo
        FROM TA_STANDARD_YATRA t
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
def calculate_timelimits_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Ensure numeric columns are floats to avoid 'LossySetitemError' during assignment
    num_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[num_cols] = (
        df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(float)
    )

    # Get the max limits per connection
    agg = df.groupby("ConnectionID").agg(
        {
            "depL1": "max",
            "arrL1": "max",
            "depL2": "max",
            "arrL2": "max",
            "DepartureDate": "min",
        }
    )

    agg["limitL1"] = agg[["depL1", "arrL1"]].max(axis=1)
    agg["limitL2"] = agg[["depL2", "arrL2"]].max(axis=1)

    # 2. Identify connections with NO airport limits
    needs_special = (agg["limitL1"] == 0) & (agg["limitL2"] == 0)

    if needs_special.any():
        # Use .copy() to avoid SettingWithCopy warnings on the original df
        df_special = df[["ConnectionID", "AirlineCode"]].copy()
        df_special["spec"] = df_special["AirlineCode"].map(SPECIAL_NON_EU_TIME_LIMITS)

        # Extract values into float columns
        df_special["sL1"] = df_special["spec"].apply(
            lambda x: float(x[0]) if isinstance(x, tuple) else 0.0
        )
        df_special["sL2"] = df_special["spec"].apply(
            lambda x: float(x[1]) if isinstance(x, tuple) else 0.0
        )

        # Get the max special limit per connection
        special_max = df_special.groupby("ConnectionID")[["sL1", "sL2"]].max()

        # Update the aggregate table
        # .loc with .values ensures we are passing the raw data without index misalignment issues
        agg.loc[needs_special, "limitL1"] = special_max.loc[
            needs_special[needs_special].index, "sL1"
        ]
        agg.loc[needs_special, "limitL2"] = special_max.loc[
            needs_special[needs_special].index, "sL2"
        ]

    # 3. Final comparison
    april_target = pd.to_datetime("2026-04-01")
    # Ensure DepartureDate is datetime
    agg_dep_dates = pd.to_datetime(agg["DepartureDate"])
    diff_years = (april_target - agg_dep_dates).dt.days / 365.25

    agg["IsTimeLimitL1"] = agg["limitL1"] >= diff_years
    agg["IsTimeLimitL2"] = agg["limitL2"] >= diff_years

    return agg.reset_index()[["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"]]


def check_if_timelimit_is_met(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"])

    april_1_2026 = pd.to_datetime("2026-04-01")
    df["DepartureDate"] = pd.to_datetime(df["DepartureDate"])

    # Numeric conversion
    num_cols = ["depL1", "depL2", "arrL1", "arrL2"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    results = []

    # Group by connection
    for connection_id, group in df.groupby("ConnectionID"):
        dep_date = group["DepartureDate"].min()
        diff_years = (april_1_2026 - dep_date).days / 365.25

        timelimit_L1 = max(group["depL1"].max(), group["arrL1"].max())
        timelimit_L2 = max(group["depL2"].max(), group["arrL2"].max())

        if timelimit_L1 == 0 and timelimit_L2 == 0:
            airline_codes = group["AirlineCode"].dropna().unique()

            # Find all special airline limits in connection group
            special_L1 = None
            special_L2 = None

            for code in airline_codes:
                if code in SPECIAL_NON_EU_TIME_LIMITS:
                    l1, l2 = SPECIAL_NON_EU_TIME_LIMITS[code]

                    special_L1 = max(special_L1 or 0, l1)
                    special_L2 = max(special_L2 or 0, l2)

            # If no special airline found → fail rule
            if special_L1 is None and special_L2 is None:
                is_time_limit_L1 = False
                is_time_limit_L2 = False
            else:
                is_time_limit_L1 = special_L1 >= diff_years
                is_time_limit_L2 = special_L2 >= diff_years
        else:
            is_time_limit_L1 = timelimit_L1 >= diff_years
            is_time_limit_L2 = timelimit_L2 >= diff_years

        results.append([connection_id, is_time_limit_L1, is_time_limit_L2])

    return pd.DataFrame(
        results, columns=["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"]
    )


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
            UPDATE TA_STANDARD_YATRA t
               SET IsTimeLimitL1 = u.IsTimeLimitL1,
                   IsTimeLimitL2 = u.IsTimeLimitL2
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
            df_updates = calculate_timelimits_vectorized(df)
            # df_updates = check_if_timelimit_is_met(df)
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
