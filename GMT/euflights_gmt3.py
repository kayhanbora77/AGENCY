import duckdb
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path
import time as _time

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TABLE_NAME = "EU_FLIGHTS"
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

COL_ID = "Id"
COL_DEP_AIRPORT = "DepartureIataCode"
COL_ARR_AIRPORT = "ArrivalIataCode"
COL_DEP_TIME = "FlightDate"
COL_GMT_DEP = "GMTDeparture"
COL_GMT_ARR = "GMTArrival"

THREADS = 4  # lower than before to reduce memory pressure
MEMORY_LIMIT = "4GB"  # conservative
TEMP_DIR = "/tmp/duckdb_temp"

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)
con.execute(f"SET threads={THREADS}")
con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
con.execute(f"SET temp_directory='{TEMP_DIR}'")
con.execute("SET enable_progress_bar=false")


# --------------------------------------------------
# STEP 0 — discover actual column names
# --------------------------------------------------
def discover_columns():
    schema = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{TABLE_NAME}'
        ORDER BY ordinal_position
    """).fetchall()

    print("=" * 60)
    print(f"Schema for {TABLE_NAME}:")
    for col, dtype in schema:
        print(f"  {col:40s} {dtype}")
    print("=" * 60)

    # Sample 5 rows to see actual values
    row = con.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 1").fetchone()
    cols = [d[0] for d in con.description]
    print("\nSample row:")
    for c, v in zip(cols, row):
        print(f"  {c:40s} = {v!r}")
    print("=" * 60)


# --------------------------------------------------
# STEP 1 — build GMT offset cache from airport list only
# (no scan of the full 38M table yet)
# --------------------------------------------------
def load_tz_map():
    airports = con.execute("""
        SELECT iata, timezone
        FROM AIRPORTS_ALL
        WHERE iata IS NOT NULL AND timezone IS NOT NULL
    """).df()
    tz_map = dict(zip(airports["iata"].str.strip().str.upper(), airports["timezone"]))
    print(f"✅ Loaded {len(tz_map):,} airport timezones")
    return tz_map


def calc_gmt_offset(tz_name: str, date_val) -> str | None:
    """Compute GMT offset string for a timezone name + date."""
    if not tz_name or pd.isna(date_val):
        return None
    try:
        if isinstance(date_val, (pd.Timestamp, datetime)):
            date_only = date_val.date()
        else:
            date_only = pd.Timestamp(date_val).date()
        dt = datetime.combine(date_only, time(12, 0)).replace(tzinfo=ZoneInfo(tz_name))
        offset = dt.utcoffset().total_seconds() / 3600
        return f"{int(offset):+d}" if offset == int(offset) else f"{offset:+.1f}"
    except Exception:
        return None


# --------------------------------------------------
# STEP 2 — build lookup only from airports that actually
# appear in the flight table, sampled cheaply
# --------------------------------------------------
def build_airport_date_lookup(tz_map: dict) -> pd.DataFrame:
    """
    Instead of DISTINCT over 38M rows (OOM), we:
    1. Get the distinct airports from the flight table (small result).
    2. Get the distinct dates from the flight table (small result).
    3. Cross-join them in Python — max ~500 airports × 365 days = 180K rows.
    4. Compute GMT for each combo.
    """
    print("\n🔍 Collecting distinct airports from flight table...")
    dep_aps = (
        con.execute(f"""
        SELECT DISTINCT UPPER(TRIM("{COL_DEP_AIRPORT}")) AS ap
        FROM {TABLE_NAME}
        WHERE "{COL_DEP_AIRPORT}" IS NOT NULL
          AND TRIM("{COL_DEP_AIRPORT}") != ''
    """)
        .df()["ap"]
        .dropna()
        .tolist()
    )

    arr_aps = (
        con.execute(f"""
        SELECT DISTINCT UPPER(TRIM("{COL_ARR_AIRPORT}")) AS ap
        FROM {TABLE_NAME}
        WHERE "{COL_ARR_AIRPORT}" IS NOT NULL
          AND TRIM("{COL_ARR_AIRPORT}") != ''
    """)
        .df()["ap"]
        .dropna()
        .tolist()
    )

    all_aps = list(set(dep_aps + arr_aps))
    print(f"   → {len(all_aps):,} distinct airports")

    # Only keep airports we have a timezone for
    known_aps = [(ap, tz_map[ap]) for ap in all_aps if ap in tz_map]
    print(f"   → {len(known_aps):,} airports with known timezone")

    print("\n🔍 Collecting distinct flight dates...")

    # Cast to date carefully — handle both TIMESTAMP and VARCHAR columns
    try:
        dates = (
            con.execute(f"""
            SELECT DISTINCT CAST("{COL_DEP_TIME}" AS DATE) AS flt_date
            FROM {TABLE_NAME}
            WHERE "{COL_DEP_TIME}" IS NOT NULL
              AND TRY_CAST("{COL_DEP_TIME}" AS DATE) IS NOT NULL
            ORDER BY flt_date
        """)
            .df()["flt_date"]
            .dropna()
            .tolist()
        )
    except Exception as e:
        print(f"   ⚠️  Date cast failed ({e}), trying VARCHAR path...")
        dates = (
            con.execute(f"""
            SELECT DISTINCT TRY_CAST("{COL_DEP_TIME}" AS DATE) AS flt_date
            FROM {TABLE_NAME}
            WHERE TRY_CAST("{COL_DEP_TIME}" AS DATE) IS NOT NULL
            ORDER BY flt_date
        """)
            .df()["flt_date"]
            .dropna()
            .tolist()
        )

    print(f"   → {len(dates):,} distinct dates  ({min(dates)} → {max(dates)})")

    # Cross-join in Python — this is tiny (airports × dates)
    print(f"\n⚙️  Computing GMT offsets for {len(known_aps) * len(dates):,} combos...")
    rows = []
    for ap, tz_name in known_aps:
        for d in dates:
            offset = calc_gmt_offset(tz_name, d)
            rows.append((ap, d, offset))

    df = pd.DataFrame(rows, columns=["ap", "flt_date", "gmt_offset"])
    print(f"   → Done. {len(df):,} lookup rows built.")
    return df


# --------------------------------------------------
# STEP 3 — register lookup and UPDATE in one SQL pass
# --------------------------------------------------
def apply_updates(lookup_df: pd.DataFrame):
    print("\n⚡ Registering lookup table...")
    con.execute("DROP TABLE IF EXISTS _gmt_ap_lookup")
    con.register("_tmp_lookup_df", lookup_df)
    con.execute("""
        CREATE TEMP TABLE _gmt_ap_lookup AS
        SELECT ap, CAST(flt_date AS DATE) AS flt_date, gmt_offset
        FROM _tmp_lookup_df
    """)
    con.unregister("_tmp_lookup_df")

    # Index on (ap, flt_date) for fast JOIN
    con.execute("CREATE INDEX idx_dep ON _gmt_ap_lookup (ap, flt_date)")

    print(f"⚡ Running UPDATE for {COL_GMT_DEP}...")
    con.execute(f"""
        UPDATE {TABLE_NAME} t
        SET "{COL_GMT_DEP}" = g.gmt_offset
        FROM _gmt_ap_lookup g
        WHERE UPPER(TRIM(t."{COL_DEP_AIRPORT}")) = g.ap
          AND TRY_CAST(t."{COL_DEP_TIME}" AS DATE) = g.flt_date
    """)
    print(f"   → {COL_GMT_DEP} updated")

    print(f"⚡ Running UPDATE for {COL_GMT_ARR}...")
    con.execute(f"""
        UPDATE {TABLE_NAME} t
        SET "{COL_GMT_ARR}" = g.gmt_offset
        FROM _gmt_ap_lookup g
        WHERE UPPER(TRIM(t."{COL_ARR_AIRPORT}")) = g.ap
          AND TRY_CAST(t."{COL_DEP_TIME}" AS DATE) = g.flt_date
    """)
    print(f"   → {COL_GMT_ARR} updated")

    con.execute("DROP TABLE IF EXISTS _gmt_ap_lookup")


# --------------------------------------------------
# STEP 4 — verify
# --------------------------------------------------
def verify():
    total = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    filled = con.execute(
        f'SELECT COUNT(*) FROM {TABLE_NAME} WHERE "{COL_GMT_DEP}" IS NOT NULL'
    ).fetchone()[0]
    print("\n📊 Verification:")
    print(f"   Total rows     : {total:>12,}")
    print(f"   GMT dep filled : {filled:>12,}  ({filled / total * 100:.1f}%)")
    print(f"   Still NULL     : {total - filled:>12,}")

    print("\n   Sample filled rows:")
    rows = con.execute(f"""
        SELECT "{COL_DEP_AIRPORT}", "{COL_ARR_AIRPORT}",
               "{COL_DEP_TIME}", "{COL_GMT_DEP}", "{COL_GMT_ARR}"
        FROM {TABLE_NAME}
        WHERE "{COL_GMT_DEP}" IS NOT NULL
        LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"   {r}")

    if filled == 0:
        print("\n⚠️  Still 0 filled. Run discover_columns() output and check:")
        print("   1. COL_DEP_TIME actual values (may be empty string, not NULL)")
        print("   2. COL_DEP_AIRPORT / COL_ARR_AIRPORT exact column names")
        print("   3. Whether airports in flight table match AIRPORTS_ALL iata codes")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    t0 = _time.time()

    discover_columns()  # print schema + 1 sample row
    tz_map = load_tz_map()
    lookup_df = build_airport_date_lookup(tz_map)
    apply_updates(lookup_df)
    verify()

    elapsed = _time.time() - t0
    print(f"\n⏱️  Total: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    print("🎉 DONE")
    con.close()


if __name__ == "__main__":
    main()
