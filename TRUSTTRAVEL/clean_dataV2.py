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

SOURCE_TABLE = "TRUST_TRAVEL"
TARGET_TABLE = "TRUST_TRAVEL_TARGET_2"
REJECTION_TABLE = "TRUST_TRAVEL_REJECTIONS"

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
            BillDate TIMESTAMP_NS,
            PNRNo VARCHAR,
            AirlineName VARCHAR,
            TicketNo VARCHAR,

            FltNo1 VARCHAR,
            FltNo2 VARCHAR,
            FltNo3 VARCHAR,
            FltNo4 VARCHAR,

            FltDate1 TIMESTAMP_NS,
            FltDate2 TIMESTAMP_NS,
            FltDate3 TIMESTAMP_NS,
            FltDate4 TIMESTAMP_NS,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR
        )
    """)


def create_rejection_table(con):
    log("♻️ Creating rejection log table")
    con.execute(f"DROP TABLE IF EXISTS {REJECTION_TABLE}")
    con.execute(f"""
        CREATE TABLE {REJECTION_TABLE} (
            -- Original source columns
            BillDate VARCHAR,
            PNRNo VARCHAR,
            AirlineName VARCHAR,
            TicketNo VARCHAR,

            FltNo1 VARCHAR,
            FltNo2 VARCHAR,
            FltNo3 VARCHAR,
            FltNo4 VARCHAR,

            FltDate1 VARCHAR,
            FltDate2 VARCHAR,
            FltDate3 VARCHAR,
            FltDate4 VARCHAR,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,

            -- Rejection metadata
            RejectionReason VARCHAR,
            RejectionDetail VARCHAR,
            RejectedAt TIMESTAMP DEFAULT current_timestamp
        )
    """)


def get_total_rows(con) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def create_clean_view(con):
    log("🧹 Materializing cleaned source")
    con.execute("DROP TABLE IF EXISTS _cleaned_source")
    con.execute(f"""
        CREATE TEMP TABLE _cleaned_source AS
        SELECT
            row_number() OVER () AS _row_id,
            BillDate, PNRNo, AirlineName, TicketNo,
            NULLIF(regexp_replace(replace(trim(upper(FltNo1)), ' ', ''), '{FLTNO_REGEX}', '\\1\\2'), '') AS FN1,
            NULLIF(regexp_replace(replace(trim(upper(FltNo2)), ' ', ''), '{FLTNO_REGEX}', '\\1\\2'), '') AS FN2,
            NULLIF(regexp_replace(replace(trim(upper(FltNo3)), ' ', ''), '{FLTNO_REGEX}', '\\1\\2'), '') AS FN3,
            NULLIF(regexp_replace(replace(trim(upper(FltNo4)), ' ', ''), '{FLTNO_REGEX}', '\\1\\2'), '') AS FN4,
            TRY_CAST(FltDate1 AS TIMESTAMP) AS DT1,
            TRY_CAST(FltDate2 AS TIMESTAMP) AS DT2,
            TRY_CAST(FltDate3 AS TIMESTAMP) AS DT3,
            TRY_CAST(FltDate4 AS TIMESTAMP) AS DT4,
            CAST(FltDate1 AS VARCHAR) AS RAW_FD1,
            CAST(FltDate2 AS VARCHAR) AS RAW_FD2,
            CAST(FltDate3 AS VARCHAR) AS RAW_FD3,
            CAST(FltDate4 AS VARCHAR) AS RAW_FD4,
            CAST(FltNo1 AS VARCHAR) AS RAW_FN1,
            CAST(FltNo2 AS VARCHAR) AS RAW_FN2,
            CAST(FltNo3 AS VARCHAR) AS RAW_FN3,
            CAST(FltNo4 AS VARCHAR) AS RAW_FN4,
            Airport1 AS AP1, Airport2 AS AP2, Airport3 AS AP3, Airport4 AS AP4, Airport5 AS AP5
        FROM {SOURCE_TABLE}
        WHERE
              (TRY_CAST(FltDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FltDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FltDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FltDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
    """)
    con.execute("CREATE INDEX idx_cleaned_row_id ON _cleaned_source (_row_id)")


def log_invalid_date_range_rows(con, rejection_rows: list):
    df = con.execute(f"""
        SELECT
            CAST(BillDate AS VARCHAR),
            CAST(PNRNo AS VARCHAR),
            CAST(AirlineName AS VARCHAR),
            CAST(TicketNo AS VARCHAR),
            CAST(FltNo1 AS VARCHAR),
            CAST(FltNo2 AS VARCHAR),
            CAST(FltNo3 AS VARCHAR),
            CAST(FltNo4 AS VARCHAR),
            CAST(FltDate1 AS VARCHAR),
            CAST(FltDate2 AS VARCHAR),
            CAST(FltDate3 AS VARCHAR),
            CAST(FltDate4 AS VARCHAR),
            CAST(Airport1 AS VARCHAR),
            CAST(Airport2 AS VARCHAR),
            CAST(Airport3 AS VARCHAR),
            CAST(Airport4 AS VARCHAR),
            CAST(Airport5 AS VARCHAR)
        FROM {SOURCE_TABLE}
        WHERE NOT (
            (TRY_CAST(FltDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
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
        return False, Reason.FN_NULL + "-" + Reason.DT_NULL, f"fn={fn!r}, dt={dt!r}"

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
        "BillDate",
        "PNRNo",
        "AirlineName",
        "TicketNo",
        "FltNo1",
        "FltNo2",
        "FltNo3",
        "FltNo4",
        "FltDate1",
        "FltDate2",
        "FltDate3",
        "FltDate4",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
    ]
    df_out.columns = col_names

    key_cols_target = [
        "PNRNo",
        "AirlineName",
        "TicketNo",
        "FltNo1",
        "FltNo2",
        "FltNo3",
        "FltNo4",
        "FltDate1",
        "FltDate2",
        "FltDate3",
        "FltDate4",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
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
        if len(r) == 19:  # 17 source cols + reason + detail
            padded.append(r + [None])
        elif len(r) == 20:
            padded.append(r)
        else:
            padded.append((r + [None] * 20)[:20])

    df_rej = pd.DataFrame(padded, dtype="object")
    df_rej.replace("", None, inplace=True)
    con.execute(f"INSERT INTO {REJECTION_TABLE} SELECT * FROM df_rej")
    rejection_rows.clear()


def process_batch(con, row_id_start, rejection_rows: list):
    df = con.execute(f"""
        SELECT * FROM _cleaned_source
        WHERE _row_id BETWEEN {row_id_start} AND {row_id_start + BATCH_SIZE - 1}
    """).df()

    if df.empty:
        return 0

    out_rows = []
    rej_bases = []

    base_cols = ["BillDate", "PNRNo", "AirlineName", "TicketNo"]
    raw_cols = [
        "RAW_FN1",
        "RAW_FN2",
        "RAW_FN3",
        "RAW_FN4",
        "RAW_FD1",
        "RAW_FD2",
        "RAW_FD3",
        "RAW_FD4",
    ]
    ap_cols = ["AP1", "AP2", "AP3", "AP4", "AP5"]

    base_data = df[base_cols].values
    raw_data = df[raw_cols].values
    ap_data = df[ap_cols].values

    for idx, row in enumerate(df.itertuples(index=False)):

        def make_rej_base():
            bd, pnr, al, tn = base_data[idx]
            fn1, fn2, fn3, fn4, fd1, fd2, fd3, fd4 = raw_data[idx]
            ap1, ap2, ap3, ap4, ap5 = ap_data[idx]
            return [
                str(bd) if bd is not None else None,
                str(pnr) if pnr is not None else None,
                str(al) if al is not None else None,
                str(tn) if tn is not None else None,
                str(fn1) if fn1 is not None else None,
                str(fn2) if fn2 is not None else None,
                str(fn3) if fn3 is not None else None,
                str(fn4) if fn4 is not None else None,
                str(fd1) if fd1 is not None else None,
                str(fd2) if fd2 is not None else None,
                str(fd3) if fd3 is not None else None,
                str(fd4) if fd4 is not None else None,
                str(ap1) if ap1 is not None else None,
                str(ap2) if ap2 is not None else None,
                str(ap3) if ap3 is not None else None,
                str(ap4) if ap4 is not None else None,
                str(ap5) if ap5 is not None else None,
            ]

        flights = []
        slot_rejections = []

        for i in range(1, 5):
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
            fn_out = [None] * 4
            dt_out = [None] * 4
            ap_out = [None] * 5

            for i, (fn, dt, dep_ap, arr_ap, slot) in enumerate(route[:4]):
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

    row_id = 1
    batch = 0

    while row_id <= total:
        batch += 1
        batch_start = time.time()
        log(
            f"🔄 Batch {batch} | rows {row_id:,} → {min(row_id + BATCH_SIZE - 1, total):,}"
        )

        rejection_rows = []
        rows_processed = process_batch(con, row_id, rejection_rows)
        processed += rows_processed
        row_id += BATCH_SIZE

        if rejection_rows:
            flush_rejections(con, rejection_rows)

        batch_time = time.time() - batch_start
        progress = (min(row_id, total) / total) * 100
        eta = (batch_time * max(total - row_id, 0) / BATCH_SIZE) / 3600
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
