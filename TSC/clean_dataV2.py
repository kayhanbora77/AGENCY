from datetime import timedelta
import duckdb
import pandas as pd
import time
from pathlib import Path
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TSC"
TARGET_TABLE = "TSC_2"
REJECTION_TABLE = "TSC_REJECTIONS"

FLTNO_REGEX = r"^([A-Z]{2,3})0+([1-9][0-9]*)$"

MAX_FLTNO_DIGITS = 8
BATCH_SIZE = 100_000
ROUTE_MAX_DAYS = 1

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"


# ==================================================
# REJECTION REASONS
# ==================================================
class Reason:
    INVALID_DATE_RANGE = "INVALID_DATE_RANGE"
    DT_NULL = "FlightDate NULL"
    DT_EMPTY = "FlightDate EMPTY"
    FN_NULL = "FlightNumber NULL"
    FN_EMPTY = "FlightNumber EMPTY"
    FN_PURELY_NUMERIC = "FlightNumber PURELY_NUMERIC"
    FN_TOO_LONG = "FlightNumber TOO_LONG"
    FN_ALL_ZEROS = "FlightNumber ALL_ZEROS"
    FN_ALL_ALPHA_AFTER_STRIP = "FlightNumber ALL_ALPHA_AFTER_STRIP"
    FN_BAD_FORMAT = "FlightNumber BAD_FORMAT"
    NO_VALID_SEGMENTS = "NO VALID SEGMENTS"
    DUPLICATE_SEGMENT = "DUPLICATE SEGMENT"
    ROUTE_OVERFLOW = "ROUTE OVERFLOW"
    BATCH_DUPLICATE = "BATCH DUPLICATE"
    DB_UNIQUE_VIOLATION = "DB UNIQUE VIOLATION"


# ==================================================
# UTILS
# ==================================================
def log(msg: str):
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ==================================================
# DATABASE
# ==================================================
def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def create_target_table(con):
    log("♻️ Creating target table")
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            PaxName VARCHAR,
            PNRCRS VARCHAR,
            PNRAirline VARCHAR,
            Airlines VARCHAR,
            AirlineCode VARCHAR,
            TktNo VARCHAR,
            
            FlightNumber1 VARCHAR,
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            FlightNumber5 VARCHAR,
            FlightNumber6 VARCHAR,

            FlightDate1 TIMESTAMP,
            FlightDate2 TIMESTAMP,
            FlightDate3 TIMESTAMP,
            FlightDate4 TIMESTAMP,
            FlightDate5 TIMESTAMP,
            FlightDate6 TIMESTAMP,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,
            Airport6 VARCHAR,
            Airport7 VARCHAR
        )
    """)


def create_rejection_table(con):
    log("♻️ Creating rejection log table")
    con.execute(f"DROP TABLE IF EXISTS {REJECTION_TABLE}")
    con.execute(f"""
        CREATE TABLE {REJECTION_TABLE} (
            -- Original source columns
            PaxName VARCHAR,
            PNRCRS VARCHAR,
            PNRAirline VARCHAR,
            Airlines VARCHAR,
            AirlineCode VARCHAR,
            TktNo VARCHAR,

            FlightNumber1 VARCHAR,
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            FlightNumber5 VARCHAR,
            FlightNumber6 VARCHAR,

            FlightDate1 VARCHAR,
            FlightDate2 VARCHAR,
            FlightDate3 VARCHAR,
            FlightDate4 VARCHAR,
            FlightDate5 VARCHAR,
            FlightDate6 VARCHAR,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,   
            Airport6 VARCHAR,
            Airport7 VARCHAR,

            -- Rejection metadata
            RejectionReason VARCHAR,
            RejectionDetail VARCHAR,
            RejectedAt TIMESTAMP DEFAULT current_timestamp
        )
    """)


def get_total_rows(con) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def create_clean_view(con):
    log("🧹 Creating cleaned source view")
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT
            PaxName,
            PNRCRS,
            PNRAirline,
            Airlines,
            AirlineCode,
            TktNo,

            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber1)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN1,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber2)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN2,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber3)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN3,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber4)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN4,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber5)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN5,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber6)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN6,

            TRY_CAST(FlightDate1 AS TIMESTAMP) AS DT1,
            TRY_CAST(FlightDate2 AS TIMESTAMP) AS DT2,
            TRY_CAST(FlightDate3 AS TIMESTAMP) AS DT3,
            TRY_CAST(FlightDate4 AS TIMESTAMP) AS DT4,
            TRY_CAST(FlightDate5 AS TIMESTAMP) AS DT5,
            TRY_CAST(FlightDate6 AS TIMESTAMP) AS DT6,

            -- Keep raw originals for the rejection log
            CAST(FlightDate1 AS VARCHAR) AS RAW_FD1,
            CAST(FlightDate2 AS VARCHAR) AS RAW_FD2,
            CAST(FlightDate3 AS VARCHAR) AS RAW_FD3,
            CAST(FlightDate4 AS VARCHAR) AS RAW_FD4,
            CAST(FlightDate5 AS VARCHAR) AS RAW_FD5,
            CAST(FlightDate6 AS VARCHAR) AS RAW_FD6,

            CAST(FlightNumber1 AS VARCHAR) AS RAW_FN1,
            CAST(FlightNumber2 AS VARCHAR) AS RAW_FN2,
            CAST(FlightNumber3 AS VARCHAR) AS RAW_FN3,
            CAST(FlightNumber4 AS VARCHAR) AS RAW_FN4,
            CAST(FlightNumber5 AS VARCHAR) AS RAW_FN5,
            CAST(FlightNumber6 AS VARCHAR) AS RAW_FN6,

            Airport1 AS AP1,
            Airport2 AS AP2,
            Airport3 AS AP3,
            Airport4 AS AP4,
            Airport5 AS AP5,
            Airport6 AS AP6,
            Airport7 AS AP7
        FROM {SOURCE_TABLE}
        WHERE
              (TRY_CAST(FlightDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate5 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate6 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
    """)


def log_invalid_date_range_rows(con, rejection_rows: list):
    df = con.execute(f"""
        SELECT
            CAST(PaxName     AS VARCHAR),
            CAST(PNRCRS      AS VARCHAR),
            CAST(PNRAirline  AS VARCHAR),
            CAST(Airlines    AS VARCHAR),
            CAST(AirlineCode AS VARCHAR),
            CAST(TktNo       AS VARCHAR),
            CAST(FlightNumber1 AS VARCHAR),
            CAST(FlightNumber2 AS VARCHAR),
            CAST(FlightNumber3 AS VARCHAR),
            CAST(FlightNumber4 AS VARCHAR),
            CAST(FlightNumber5 AS VARCHAR),
            CAST(FlightNumber6 AS VARCHAR),
            CAST(FlightDate1 AS VARCHAR),
            CAST(FlightDate2 AS VARCHAR),
            CAST(FlightDate3 AS VARCHAR),
            CAST(FlightDate4 AS VARCHAR),
            CAST(FlightDate5 AS VARCHAR),
            CAST(FlightDate6 AS VARCHAR),
            CAST(Airport1    AS VARCHAR),
            CAST(Airport2    AS VARCHAR),
            CAST(Airport3    AS VARCHAR),
            CAST(Airport4    AS VARCHAR),
            CAST(Airport5    AS VARCHAR),
            CAST(Airport6    AS VARCHAR),
            CAST(Airport7    AS VARCHAR)
        FROM {SOURCE_TABLE}
        WHERE NOT (
            (TRY_CAST(FlightDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FlightDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FlightDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FlightDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FlightDate5 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FlightDate6 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
        )
    """).fetchall()

    for r in df:
        rejection_rows.append(
            list(r)
            + [
                Reason.INVALID_DATE_RANGE,
                f"All dates outside {VALID_YEAR_MIN}-{VALID_YEAR_MAX}",
            ]
        )


def is_same_route(d1, d2):
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs(d2 - d1) <= timedelta(days=ROUTE_MAX_DAYS)


def is_valid_flightno(fn, dt) -> tuple[bool, str | None, str | None]:
    if pd.isna(fn) and pd.isna(dt):
        # Return FN_NULL so the skip logic for slots > 1 works correctly
        return False, Reason.FN_NULL, f"fn={fn!r}, dt={dt!r}"

    if pd.isna(fn):
        return False, Reason.FN_NULL, f"fn={fn!r}"

    if pd.isna(dt):
        return False, Reason.DT_NULL, f"dt={dt!r}"

    dt = str(dt).strip()
    if not dt:
        return False, Reason.DT_EMPTY, f"dt={dt!r}"

    fn = str(fn).strip()
    if not fn:
        return False, Reason.FN_EMPTY, f"fn={fn!r}"

    if fn.isdigit():
        return False, Reason.FN_PURELY_NUMERIC, f"fn={fn!r}"

    if len(fn) > MAX_FLTNO_DIGITS:
        return False, Reason.FN_TOO_LONG, f"fn={fn!r} len={len(fn)}"

    stripped = fn.rstrip("0")
    if not stripped or stripped.isalpha():
        return (
            False,
            Reason.FN_ALL_ZEROS if not stripped else Reason.FN_ALL_ALPHA_AFTER_STRIP,
            f"fn={fn!r}",
        )

    fn_upper = fn.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,3}\d+", fn_upper):
        return False, Reason.FN_BAD_FORMAT, f"fn={fn_upper!r}"

    return True, None, None


def deduplicate_flights(flights):
    flights.sort(key=lambda x: x[1])
    seen_segments = set()
    unique_flights = []

    for fn, dt, dep_ap, arr_ap, slot in flights:
        key = (fn, dt.date())
        if key not in seen_segments:
            seen_segments.add(key)
            unique_flights.append((fn, dt, dep_ap, arr_ap, slot))

    return unique_flights


def group_into_routes(flights):
    routes = []
    current = [flights[0]]
    route_start_date = flights[0][1]

    for f in flights[1:]:
        if is_same_route(f[1], route_start_date):
            current.append(f)
        else:
            routes.append(current)
            current = [f]
            route_start_date = f[1]

    routes.append(current)
    return routes


def insert_target_table(con, out_rows, rej_bases, rejection_rows):
    df_out = pd.DataFrame(out_rows, dtype="object")
    df_out.replace("", None, inplace=True)

    col_names = [
        "PaxName",
        "PNRCRS",
        "PNRAirline",
        "Airlines",
        "AirlineCode",
        "TktNo",
        "FlightNumber1",
        "FlightNumber2",
        "FlightNumber3",
        "FlightNumber4",
        "FlightNumber5",
        "FlightNumber6",
        "FlightDate1",
        "FlightDate2",
        "FlightDate3",
        "FlightDate4",
        "FlightDate5",
        "FlightDate6",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
        "Airport6",
        "Airport7",
    ]
    df_out.columns = col_names

    key_cols_target = [
        "PNRCRS",
        "AirlineCode",
        "TktNo",
        "FlightNumber1",
        "FlightNumber2",
        "FlightNumber3",
        "FlightNumber4",
        "FlightNumber5",
        "FlightNumber6",
        "FlightDate1",
        "FlightDate2",
        "FlightDate3",
        "FlightDate4",
        "FlightDate5",
        "FlightDate6",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
        "Airport6",
        "Airport7",
    ]

    # --- Batch-level duplicates ---
    initial_rows = len(df_out)
    mask_first = ~df_out.duplicated(subset=key_cols_target, keep="first")
    dup_indices = df_out.index[~mask_first].tolist()
    df_out = df_out[mask_first].reset_index(drop=True)

    dropped_count = initial_rows - len(df_out)
    if dropped_count > 0:
        log(f"🗑️ Dropped {dropped_count} duplicate rows within batch")
        for i in dup_indices:
            rejection_rows.append(
                rej_bases[i] + [Reason.BATCH_DUPLICATE, "Duplicate within batch"]
            )

    kept_rej_bases = [
        rej_bases[i] for i in range(initial_rows) if i not in set(dup_indices)
    ]

    if len(df_out) == 0:
        return

    # --- Detect DB duplicates BEFORE insert ---
    con.execute("DROP TABLE IF EXISTS _batch_staging")
    con.execute(
        f"CREATE TEMP TABLE _batch_staging AS SELECT * FROM {TARGET_TABLE} WHERE 1=0"
    )
    con.execute("ALTER TABLE _batch_staging ADD COLUMN _rej_idx INTEGER")

    df_out_indexed = df_out.copy()
    df_out_indexed["_rej_idx"] = range(len(df_out_indexed))

    con.execute("INSERT INTO _batch_staging SELECT * FROM df_out_indexed")

    join_clause = " AND\n ".join(f"s.{c} = t.{c}" for c in key_cols_target)
    dup_rows = con.execute(f"""
        SELECT _rej_idx FROM _batch_staging s
        WHERE EXISTS (
            SELECT 1 FROM {TARGET_TABLE} t
            WHERE {join_clause}
        )
    """).fetchall()

    dup_idx_set = {int(r[0]) for r in dup_rows}

    for idx in sorted(dup_idx_set):
        rejection_rows.append(
            kept_rej_bases[idx]
            + [Reason.DB_UNIQUE_VIOLATION, "Already exists in target table"]
        )

    ignored = len(dup_idx_set)
    if ignored > 0:
        log(f"⚠️ {ignored} rows skipped - already exist in target")

    # Insert only non-duplicate rows
    target_cols = ", ".join(col_names)
    if dup_idx_set:
        placeholders = ",".join(str(i) for i in sorted(dup_idx_set))
        con.execute(f"""
            INSERT INTO {TARGET_TABLE} ({target_cols})
            SELECT {target_cols} FROM _batch_staging
            WHERE _rej_idx NOT IN ({placeholders})
        """)
    else:
        con.execute(f"""
            INSERT INTO {TARGET_TABLE} ({target_cols})
            SELECT {target_cols} FROM _batch_staging
        """)

    inserted = len(df_out) - ignored
    log(f"✅ Inserted {inserted:,} rows into {TARGET_TABLE}")
    con.execute("DROP TABLE IF EXISTS _batch_staging")


def flush_rejections(con, rejection_rows: list):
    if not rejection_rows:
        return
    padded = []
    for r in rejection_rows:
        if len(r) == 27:  # 25 source cols + reason + detail
            padded.append(r + [None])  # add NULL for RejectedAt
        elif len(r) == 28:  # already has RejectedAt
            padded.append(r)
        else:
            # Safety: pad or trim to 28
            padded.append((r + [None] * 28)[:28])

    df_rej = pd.DataFrame(padded, dtype="object")
    df_rej.replace("", None, inplace=True)
    con.execute(f"INSERT INTO {REJECTION_TABLE} SELECT * FROM df_rej")
    rejection_rows.clear()


def process_batch(con, offset, rejection_rows: list):
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out_rows = []
    rej_bases = []

    base_cols = ["PaxName", "PNRCRS", "PNRAirline", "Airlines", "AirlineCode", "TktNo"]
    raw_cols = [
        "RAW_FN1",
        "RAW_FN2",
        "RAW_FN3",
        "RAW_FN4",
        "RAW_FN5",
        "RAW_FN6",
        "RAW_FD1",
        "RAW_FD2",
        "RAW_FD3",
        "RAW_FD4",
        "RAW_FD5",
        "RAW_FD6",
    ]
    ap_cols = ["AP1", "AP2", "AP3", "AP4", "AP5", "AP6", "AP7"]

    base_data = df[base_cols].values
    raw_data = df[raw_cols].values
    ap_data = df[ap_cols].values

    for idx, row in enumerate(df.itertuples(index=False)):

        def make_rej_base():
            pn, pcrs, pal, als, acd, tn = base_data[idx]
            fn1, fn2, fn3, fn4, fn5, fn6, fd1, fd2, fd3, fd4, fd5, fd6 = raw_data[idx]
            ap1, ap2, ap3, ap4, ap5, ap6, ap7 = ap_data[idx]
            return [
                str(pn) if pn is not None else None,
                str(pcrs) if pcrs is not None else None,
                str(pal) if pal is not None else None,
                str(als) if als is not None else None,
                str(acd) if acd is not None else None,
                str(tn) if tn is not None else None,
                str(fn1) if fn1 is not None else None,
                str(fn2) if fn2 is not None else None,
                str(fn3) if fn3 is not None else None,
                str(fn4) if fn4 is not None else None,
                str(fn5) if fn5 is not None else None,
                str(fn6) if fn6 is not None else None,
                str(fd1) if fd1 is not None else None,
                str(fd2) if fd2 is not None else None,
                str(fd3) if fd3 is not None else None,
                str(fd4) if fd4 is not None else None,
                str(fd5) if fd5 is not None else None,
                str(fd6) if fd6 is not None else None,
                str(ap1) if ap1 is not None else None,
                str(ap2) if ap2 is not None else None,
                str(ap3) if ap3 is not None else None,
                str(ap4) if ap4 is not None else None,
                str(ap5) if ap5 is not None else None,
                str(ap6) if ap6 is not None else None,
                str(ap7) if ap7 is not None else None,
            ]

        flights = []
        slot_rejections = []

        for i in range(1, 7):
            fn = getattr(row, f"FN{i}")
            dt = getattr(row, f"DT{i}")
            valid, reason, detail = is_valid_flightno(fn, dt)

            if not valid:
                if (
                    reason == Reason.FN_NULL
                    or reason == Reason.DT_NULL
                    or reason == Reason.FN_EMPTY
                    or reason == Reason.DT_EMPTY
                ) and i > 1:
                    continue
                slot_rejections.append((i, reason, detail))
                continue

            depAp = getattr(row, f"AP{i}")
            arrAp = getattr(row, f"AP{i + 1}")
            flights.append((fn, dt, depAp, arrAp, i))

        if not flights:
            rej_base = make_rej_base()
            if slot_rejections:
                primary = next(
                    (r for r in slot_rejections if r[0] == 1), slot_rejections[0]
                )
                _, reason, detail = primary
                all_slots = ", ".join(f"FltNo{r[0]}({r[1]})" for r in slot_rejections)
                rejection_rows.append(
                    rej_base + [reason, f"No valid segments. Slots: {all_slots}"]
                )
            else:
                rejection_rows.append(
                    rej_base
                    + [Reason.NO_VALID_SEGMENTS, "All flight slots null or invalid"]
                )
            continue

        flights = deduplicate_flights(flights)
        routes = group_into_routes(flights)
        rej_base = make_rej_base()

        for route in routes:
            row_out = list(base_data[idx])
            fn_out = [None] * 6
            dt_out = [None] * 6
            ap_out = [None] * 7

            for i, (fn, dt, dep_ap, arr_ap, slot) in enumerate(route[:6]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                if i + 1 < len(ap_out):
                    ap_out[i + 1] = arr_ap

            out_rows.append(row_out + fn_out + dt_out + ap_out)
            rej_bases.append(rej_base)

    if not out_rows:
        return 0

    insert_target_table(con, out_rows, rej_bases, rejection_rows)
    return len(out_rows)


# ==================================================
# MAIN
# ==================================================
def main():
    start = time.time()
    log(f"🚀 Start {now_str()}")

    con = connect_db()
    create_target_table(con)
    create_rejection_table(con)
    create_clean_view(con)

    rejection_rows: list = []
    log("🔍 Scanning for invalid date range rows...")
    log_invalid_date_range_rows(con, rejection_rows)
    if rejection_rows:
        flush_rejections(con, rejection_rows)
        log(f"📋 {len(rejection_rows)} rows logged as INVALID_DATE_RANGE")

    result = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()
    total = result[0] if result else 0
    log(f"📊 Cleaned rows: {total:,}")

    offset = 0
    batch = 0
    processed = 0

    while offset < total:
        batch += 1
        batch_start = time.time()
        log(f"🔄 Batch {batch} | {offset:,} → {min(offset + BATCH_SIZE, total):,}")

        rejection_rows = []
        rows_processed = process_batch(con, offset, rejection_rows)
        processed += rows_processed
        offset += BATCH_SIZE

        if rejection_rows:
            flush_rejections(con, rejection_rows)

        batch_time = time.time() - batch_start
        progress = (offset / total) * 100
        eta = (batch_time * (total - offset) / BATCH_SIZE) / 3600

        log(f"✅ Processed {rows_processed:,} rows | {progress:.1f}% | ETA: {eta:.2f}h")

    log("\n📋 Rejection Summary:")
    summary = con.execute(f"""
        SELECT RejectionReason, COUNT(*) AS cnt
        FROM {REJECTION_TABLE}
        GROUP BY RejectionReason
        ORDER BY cnt DESC
    """).fetchall()
    for reason, cnt in summary:
        log(f" {reason:<30} {cnt:>10,}")

    src = con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]
    tgt = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
    rej = con.execute(f"SELECT COUNT(*) FROM {REJECTION_TABLE}").fetchone()[0]
    extra = tgt - src + rej
    log("\n📊 Reconciliation:")
    log(f" Source rows : {src:>10,}")
    log(f" Target rows : {tgt:>10,}")
    log(f" Rejection rows : {rej:>10,}")
    log(f" Route-split expansions : {extra:>10,}")

    elapsed = time.time() - start
    log(f"\n⏱️ Execution Time: {elapsed / 3600:.2f} hours")
    log("🎉 ETL COMPLETED")

    con.close()


if __name__ == "__main__":
    main()
