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

SOURCE_TABLE = "BLUESTAR"
TARGET_TABLE = "BLUESTAR_TARGET_2"
REJECTION_TABLE = "BLUESTAR_REJECTIONS"

FLTNO_REGEX = r"^([A-Z]{2,3}|\d[A-Z])0+([1-9][0-9]*)$"

MAX_FLTNO_DIGITS = 8

BATCH_SIZE = 100_000

ROUTE_MAX_DAYS = 1

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"
KEY_COLS = [
    "PNRNo",
    "AirlineName",
    "TicketNo",
    "FltNo1",
    "FltDate1",
    "Airport1",
    "Airport2",
]


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
    # con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def create_target_table(con):
    log("♻️ Creating target table")
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            BillDate TIMESTAMP_NS,
            PaxName VARCHAR,
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

            SupplierName VARCHAR,
            PaxType VARCHAR,

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
            PaxName VARCHAR,
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
            SupplierName VARCHAR,
            PaxType VARCHAR,
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
    log("🧹 Creating cleaned source view")
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT
            BillDate,
            PaxName,
            PNRNo,
            AirlineName,
            TicketNo,

            NULLIF(
                regexp_replace(
                    replace(trim(upper(FltNo1)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN1,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FltNo2)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN2,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FltNo3)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN3,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FltNo4)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN4,

            TRY_CAST(FltDate1 AS TIMESTAMP) AS DT1,
            TRY_CAST(FltDate2 AS TIMESTAMP) AS DT2,
            TRY_CAST(FltDate3 AS TIMESTAMP) AS DT3,
            TRY_CAST(FltDate4 AS TIMESTAMP) AS DT4,

            -- Keep raw originals for the rejection log
            CAST(FltDate1 AS VARCHAR) AS RAW_FD1,
            CAST(FltDate2 AS VARCHAR) AS RAW_FD2,
            CAST(FltDate3 AS VARCHAR) AS RAW_FD3,
            CAST(FltDate4 AS VARCHAR) AS RAW_FD4,

            CAST(FltNo1 AS VARCHAR) AS RAW_FN1,
            CAST(FltNo2 AS VARCHAR) AS RAW_FN2,
            CAST(FltNo3 AS VARCHAR) AS RAW_FN3,
            CAST(FltNo4 AS VARCHAR) AS RAW_FN4,

            SupplierName,
            PaxType,

            Airport1 AS AP1,
            Airport2 AS AP2,
            Airport3 AS AP3,
            Airport4 AS AP4,
            Airport5 AS AP5
        FROM {SOURCE_TABLE}
        WHERE
            (TRY_CAST(FltDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            OR (TRY_CAST(FltDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
            ORDER BY PNRNo, AirlineName, FltNo1, FltDate1
    """)


def log_invalid_date_range_rows(con, rejection_rows: list):
    df = con.execute(f"""
        SELECT
            CAST(BillDate AS VARCHAR),
            CAST(PaxName AS VARCHAR),
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
            CAST(SupplierName AS VARCHAR),
            CAST(PaxType AS VARCHAR),
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


def insert_target_table(con, paired_rows, rejection_rows):
    if not paired_rows:
        return

    col_names = [
        "BillDate",
        "PaxName",
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
        "SupplierName",
        "PaxType",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
    ]

    out_rows = [p[0] for p in paired_rows]
    rej_bases = [p[1] for p in paired_rows]  # index-safe: same list, never drifts

    df_out = pd.DataFrame(out_rows, columns=col_names, dtype="object")
    df_out.replace("", None, inplace=True)

    for col in [
        "PNRNo",
        "AirlineName",
        "TicketNo",
        "FltNo1",
        "FltNo2",
        "FltNo3",
        "FltNo4",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
    ]:
        df_out[col] = df_out[col].astype("string").str.strip()

    for dc in ["FltDate1", "FltDate2", "FltDate3", "FltDate4"]:
        df_out[dc] = pd.to_datetime(df_out[dc], errors="coerce")

    # ── Stage 1: batch-level dedup ──────────────────────────────────────────
    is_dup = df_out.duplicated(subset=KEY_COLS, keep="first")
    dup_idxs = df_out.index[is_dup].tolist()

    for i in dup_idxs:
        rejection_rows.append(
            rej_bases[i] + [Reason.BATCH_DUPLICATE, "Duplicate within batch"]
        )

    df_out = df_out[~is_dup].reset_index(drop=True)
    rej_bases = [
        rej_bases[i] for i in range(len(paired_rows)) if i not in set(dup_idxs)
    ]

    if dropped := len(dup_idxs):
        log(f"🗑️ Dropped {dropped} duplicate rows within batch")

    if df_out.empty:
        return

    # ── Stage 2: DB conflict check via staging table ─────────────────────────
    con.execute("DROP TABLE IF EXISTS _batch_staging")
    con.execute(
        f"CREATE TEMP TABLE _batch_staging AS SELECT * FROM {TARGET_TABLE} WHERE 1=0"
    )
    con.execute("ALTER TABLE _batch_staging ADD COLUMN _rej_idx INTEGER")

    df_staging = df_out.copy()
    df_staging["_rej_idx"] = range(len(df_staging))  # 0..N, lines up with rej_bases
    con.execute("INSERT INTO _batch_staging SELECT * FROM df_staging")

    # NULL-safe JOIN — IS NOT DISTINCT FROM treats NULL == NULL as TRUE
    conflict_idxs = {
        int(r[0])
        for r in con.execute(f"""
            SELECT s._rej_idx
            FROM _batch_staging s
            WHERE EXISTS (
                SELECT 1 FROM {TARGET_TABLE} t
                WHERE TRIM(s.PNRNo)       = TRIM(t.PNRNo)
                  AND TRIM(s.AirlineName) = TRIM(t.AirlineName)
                  AND TRIM(s.TicketNo)    = TRIM(t.TicketNo)
              AND (s.FltNo1 IS NOT DISTINCT FROM t.FltNo1)
              AND s.FltDate1 IS NOT DISTINCT FROM t.FltDate1
              AND TRIM(s.Airport1)    = TRIM(t.Airport1)
              AND TRIM(s.Airport2)    = TRIM(t.Airport2)
              AND TRIM(s.Airport3)    = TRIM(t.Airport3)
              AND TRIM(s.Airport4)    = TRIM(t.Airport4)
              AND TRIM(s.Airport5)    = TRIM(t.Airport5)
              AND (s.FltNo2 IS NOT DISTINCT FROM t.FltNo2)
              AND (s.FltNo3 IS NOT DISTINCT FROM t.FltNo3)
              AND (s.FltNo4 IS NOT DISTINCT FROM t.FltNo4)
              AND (s.FltDate2 IS NOT DISTINCT FROM t.FltDate2)
              AND (s.FltDate3 IS NOT DISTINCT FROM t.FltDate3)
              AND (s.FltDate4 IS NOT DISTINCT FROM t.FltDate4)
        )
    """).fetchall()
    }
    for i in sorted(conflict_idxs):
        rejection_rows.append(
            rej_bases[i]
            + [Reason.DB_UNIQUE_VIOLATION, "Composite key already in target"]
        )

    if ignored := len(conflict_idxs):
        log(f"⚠️ {ignored} rows skipped — already exist in target")

    # Insert only non-conflicting rows
    target_cols = ", ".join(col_names)
    if conflict_idxs:
        placeholders = ", ".join(str(i) for i in sorted(conflict_idxs))
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

    log(f"✅ Inserted {len(df_out) - len(conflict_idxs):,} rows into {TARGET_TABLE}")
    con.execute("DROP TABLE IF EXISTS _batch_staging")


def flush_rejections(con, rejection_rows: list):
    if not rejection_rows:
        return
    padded = []
    for r in rejection_rows:
        if len(r) == 22:  # 20 source cols + reason + detail
            padded.append(r + [None])
        elif len(r) == 23:
            padded.append(r)
        else:
            padded.append((r + [None] * 23)[:23])

    df_rej = pd.DataFrame(padded, dtype="object")
    df_rej.replace("", None, inplace=True)
    con.execute(f"INSERT INTO {REJECTION_TABLE} SELECT * FROM df_rej")
    rejection_rows.clear()


def process_batch(con, offset, rejection_rows: list):
    df = con.execute(f"""
        SELECT * FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    paired_rows = []  # each entry is (out_row, rej_base) — never drifts apart

    base_cols1 = ["BillDate", "PaxName", "PNRNo", "AirlineName", "TicketNo"]
    base_cols2 = ["SupplierName", "PaxType"]
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

    base_data1 = df[base_cols1].values
    base_data2 = df[base_cols2].values
    raw_data = df[raw_cols].values
    ap_data = df[ap_cols].values

    for idx, row in enumerate(df.itertuples(index=False)):

        def make_rej_base(idx=idx):
            # Use the named columns from the dataframe directly to avoid index errors
            curr_row = df.iloc[idx]

            return [
                str(curr_row["BillDate"]) if pd.notna(curr_row["BillDate"]) else None,
                str(curr_row["PaxName"]) if pd.notna(curr_row["PaxName"]) else None,
                str(curr_row["PNRNo"]) if pd.notna(curr_row["PNRNo"]) else None,
                str(curr_row["AirlineName"])
                if pd.notna(curr_row["AirlineName"])
                else None,
                str(curr_row["TicketNo"]) if pd.notna(curr_row["TicketNo"]) else None,
                str(curr_row["RAW_FN1"]) if pd.notna(curr_row["RAW_FN1"]) else None,
                str(curr_row["RAW_FN2"]) if pd.notna(curr_row["RAW_FN2"]) else None,
                str(curr_row["RAW_FN3"]) if pd.notna(curr_row["RAW_FN3"]) else None,
                str(curr_row["RAW_FN4"]) if pd.notna(curr_row["RAW_FN4"]) else None,
                str(curr_row["RAW_FD1"]) if pd.notna(curr_row["RAW_FD1"]) else None,
                str(curr_row["RAW_FD2"]) if pd.notna(curr_row["RAW_FD2"]) else None,
                str(curr_row["RAW_FD3"]) if pd.notna(curr_row["RAW_FD3"]) else None,
                str(curr_row["RAW_FD4"]) if pd.notna(curr_row["RAW_FD4"]) else None,
                str(curr_row["SupplierName"])
                if pd.notna(curr_row["SupplierName"])
                else None,
                str(curr_row["PaxType"]) if pd.notna(curr_row["PaxType"]) else None,
                str(curr_row["AP1"]) if pd.notna(curr_row["AP1"]) else None,
                str(curr_row["AP2"]) if pd.notna(curr_row["AP2"]) else None,
                str(curr_row["AP3"]) if pd.notna(curr_row["AP3"]) else None,
                str(curr_row["AP4"]) if pd.notna(curr_row["AP4"]) else None,
                str(curr_row["AP5"]) if pd.notna(curr_row["AP5"]) else None,
            ]

        flights = []
        slot_rejections = []

        for i in range(1, 5):
            fn = getattr(row, f"FN{i}")
            dt = getattr(row, f"DT{i}")
            valid, reason, detail = is_valid_flightno(fn, dt)

            if not valid:
                if (
                    reason
                    in (
                        Reason.FN_NULL,
                        Reason.DT_NULL,
                        Reason.FN_EMPTY,
                        Reason.DT_EMPTY,
                    )
                    and i > 1
                ):
                    continue
                slot_rejections.append((i, reason, detail))
                continue

            dep_ap = getattr(row, f"AP{i}")
            arr_ap = getattr(row, f"AP{i + 1}")
            flights.append((fn, dt, dep_ap, arr_ap, i))

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
            base1 = list(base_data1[idx])
            base2 = list(base_data2[idx])
            fn_out = [None] * 4
            dt_out = [None] * 4
            ap_out = [None] * 5

            for i, (fn, dt, dep_ap, arr_ap, slot) in enumerate(route[:4]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                if i + 1 < len(ap_out):
                    ap_out[i + 1] = arr_ap

            # Explicitly build the list to ensure index alignment
            out_row = [
                base1[0],
                base1[1],
                base1[2],
                base1[3],
                base1[4],  # BillDate to TicketNo
                fn_out[0],
                fn_out[1],
                fn_out[2],
                fn_out[3],  # FltNos
                dt_out[0],
                dt_out[1],
                dt_out[2],
                dt_out[3],  # FltDates
                base2[0],
                base2[1],  # Supplier, PaxType
                ap_out[0],
                ap_out[1],
                ap_out[2],
                ap_out[3],
                ap_out[4],  # Airports
            ]
            paired_rows.append((out_row, rej_base))  # ✅ kept together

    if not paired_rows:
        return 0

    insert_target_table(con, paired_rows, rejection_rows)
    return len(paired_rows)


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
