import time
from pathlib import Path

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
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


def main():
    start_time = time.time()
    log(f"Starting at {now_str()}")

    con = connect_db()

    log("Loading EU eligible data...")

    query = """
        SELECT
            t.ConnectionID,
            t.LegNo,
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

    merged = first.merge(last, on="ConnectionID")

    merged["TimeLimitL1"] = merged[["dep_L1", "arr_L1"]].max(axis=1)
    merged["TimeLimitL2"] = merged[["dep_L2", "arr_L2"]].max(axis=1)

    df_updates = merged[["ConnectionID", "TimeLimitL1", "TimeLimitL2"]]

    log(f"Prepared {len(df_updates):,} updates")

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

    elapsed = time.time() - start_time

    log("──────────── DONE ────────────")
    log(f"Updated {len(df_updates):,} connections")
    log(f"Processed {len(df):,} legs")
    log(f"Finished in {elapsed:.2f} seconds")
    log(f"Completed at {now_str()}")

    con.close()


if __name__ == "__main__":
    main()
