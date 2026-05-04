import duckdb
import pandas as pd
import time
from pathlib import Path
import re
from datetime import timedelta

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TRIPJACK"
TARGET_TABLE = "TRIPJACK_TARGET_2"
REJECTION_TABLE = "TRIPJACK_REJECTIONS"

BATCH_SIZE = 100_000
MAX_FLTNO_DIGITS = 8
FLTNO_REGEX = r"^([A-Z]{2,3}|\d[A-Z])0+([1-9][0-9]*)$"
ROUTE_MAX_DAYS = 1

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"

KEY_COLS = [
    "BookingId",
    "PaxName",
    "FlightNumber1",
    "FlightDate1",
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
    DT_INVALID = "FlightDate INVALID"
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
            BookingId VARCHAR,
            PaxName VARCHAR,
            JourneyBucket VARCHAR,
            BookingRef_PNR VARCHAR,
            Airline VARCHAR,
            ETicketNo VARCHAR,

            FlightNumber1 VARCHAR,
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            FlightNumber5 VARCHAR,

            FlightDate1 TIMESTAMP,
            FlightDate2 TIMESTAMP,
            FlightDate3 TIMESTAMP,
            FlightDate4 TIMESTAMP,
            FlightDate5 TIMESTAMP,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,
            Airport6 VARCHAR
        )
    """)


def create_rejection_table(con):
    log("♻️ Creating rejection log table")
    con.execute(f"DROP TABLE IF EXISTS {REJECTION_TABLE}")
    con.execute(f"""
        CREATE TABLE {REJECTION_TABLE} (
            BookingId VARCHAR,
            PaxName VARCHAR,
            JourneyBucket VARCHAR,
            BookingRef_PNR VARCHAR,
            Airline VARCHAR,
            ETicketNo VARCHAR,

            FlightNumber1 VARCHAR,
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            FlightNumber5 VARCHAR,

            FlightDate1 VARCHAR,
            FlightDate2 VARCHAR,
            FlightDate3 VARCHAR,
            FlightDate4 VARCHAR,
            FlightDate5 VARCHAR,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,
            Airport6 VARCHAR,

            RejectionReason VARCHAR,
            RejectionDetail VARCHAR,
            RejectedAt TIMESTAMP DEFAULT current_timestamp
        )
    """)


def create_clean_view(con):
    log("🧹 Materializing cleaned source")
    con.execute("DROP TABLE IF EXISTS _cleaned_source")

    regex_pattern = FLTNO_REGEX.replace("\\", "\\\\")

    con.execute(f"""
        CREATE TEMP TABLE _cleaned_source AS
        SELECT
            row_number() OVER () AS _row_id,
            BookingId, PaxName, JourneyBucket, BookingRef_PNR, Airline, ETicketNo,
            NULLIF(regexp_replace(replace(trim(upper(FlightNumber1)), ' ', ''),'{regex_pattern}','\\1\\2'), '') AS FN1,
            NULLIF(regexp_replace(replace(trim(upper(FlightNumber2)), ' ', ''),'{regex_pattern}','\\1\\2'), '') AS FN2,
            NULLIF(regexp_replace(replace(trim(upper(FlightNumber3)), ' ', ''),'{regex_pattern}','\\1\\2'), '') AS FN3,
            NULLIF(regexp_replace(replace(trim(upper(FlightNumber4)), ' ', ''),'{regex_pattern}','\\1\\2'), '') AS FN4,
            NULLIF(regexp_replace(replace(trim(upper(FlightNumber5)), ' ', ''),'{regex_pattern}','\\1\\2'), '') AS FN5,
            TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) AS DT1,
            TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) AS DT2,
            TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) AS DT3,
            TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) AS DT4,
            TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) AS DT5,
            CAST(DepartureDateLocal1 AS VARCHAR) AS RAW_FD1,
            CAST(DepartureDateLocal2 AS VARCHAR) AS RAW_FD2,
            CAST(DepartureDateLocal3 AS VARCHAR) AS RAW_FD3,
            CAST(DepartureDateLocal4 AS VARCHAR) AS RAW_FD4,
            CAST(DepartureDateLocal5 AS VARCHAR) AS RAW_FD5,
            CAST(FlightNumber1 AS VARCHAR) AS RAW_FN1,
            CAST(FlightNumber2 AS VARCHAR) AS RAW_FN2,
            CAST(FlightNumber3 AS VARCHAR) AS RAW_FN3,
            CAST(FlightNumber4 AS VARCHAR) AS RAW_FN4,
            CAST(FlightNumber5 AS VARCHAR) AS RAW_FN5,
            Airport1 AS AP1, Airport2 AS AP2, Airport3 AS AP3, Airport4 AS AP4, Airport5 AS AP5, Airport6 AS AP6
        FROM {SOURCE_TABLE}
        WHERE
           COALESCE(TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
    """)
    con.execute("CREATE INDEX idx_cleaned_row_id ON _cleaned_source (_row_id)")


def log_invalid_date_range_rows(con, rejection_rows: list):
    df = con.execute(f"""
        SELECT
            CAST(BookingId AS VARCHAR), CAST(PaxName AS VARCHAR), CAST(JourneyBucket AS VARCHAR),
            CAST(BookingRef_PNR AS VARCHAR), CAST(Airline AS VARCHAR), CAST(ETicketNo AS VARCHAR),
            CAST(FlightNumber1 AS VARCHAR), CAST(FlightNumber2 AS VARCHAR), CAST(FlightNumber3 AS VARCHAR),
            CAST(FlightNumber4 AS VARCHAR), CAST(FlightNumber5 AS VARCHAR),
            CAST(DepartureDateLocal1 AS VARCHAR), CAST(DepartureDateLocal2 AS VARCHAR),
            CAST(DepartureDateLocal3 AS VARCHAR), CAST(DepartureDateLocal4 AS VARCHAR),
            CAST(DepartureDateLocal5 AS VARCHAR),
            CAST(Airport1 AS VARCHAR), CAST(Airport2 AS VARCHAR), CAST(Airport3 AS VARCHAR),
            CAST(Airport4 AS VARCHAR), CAST(Airport5 AS VARCHAR), CAST(Airport6 AS VARCHAR)
        FROM {SOURCE_TABLE}
        WHERE NOT (
           COALESCE(TRY_CAST(DepartureDateLocal1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        OR COALESCE(TRY_CAST(DepartureDateLocal5 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31', FALSE)
        )
    """).fetchall()

    for r in df:
        rejection_rows.append(
            list(r)
            + [
                Reason.INVALID_DATE_RANGE,
                f"All dates NULL or outside {VALID_YEAR_MIN}-{VALID_YEAR_MAX}",
            ]
        )


def is_same_route(d1, d2):
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs(d2 - d1) <= timedelta(days=ROUTE_MAX_DAYS)


def is_valid_flightno(fn, dt):
    if pd.isna(fn) and pd.isna(dt):
        return False, Reason.FN_NULL + "-" + Reason.DT_NULL, f"fn={fn!r}, dt={dt!r}"
    if pd.isna(fn):
        return False, Reason.FN_NULL, f"fn={fn!r}"
    if pd.isna(dt):
        return False, Reason.DT_NULL, f"dt={dt!r}"

    dt_str = str(dt).strip()
    if not dt_str:
        return False, Reason.DT_EMPTY, f"dt={dt_str!r}"

    fn_str = str(fn).strip()
    if not fn_str:
        return False, Reason.FN_EMPTY, f"fn={fn_str!r}"
    if fn_str.isdigit():
        return False, Reason.FN_PURELY_NUMERIC, f"fn={fn_str!r}"
    if len(fn_str) > MAX_FLTNO_DIGITS:
        return False, Reason.FN_TOO_LONG, f"fn={fn_str!r} len={len(fn_str)}"

    stripped = fn_str.rstrip("0")
    if not stripped or stripped.isalpha():
        return (
            False,
            (Reason.FN_ALL_ZEROS if not stripped else Reason.FN_ALL_ALPHA_AFTER_STRIP),
            f"fn={fn_str!r}",
        )

    fn_upper = fn_str.upper()
    if not re.fullmatch(r"[A-Z0-9]{2,3}\d+", fn_upper):
        return False, Reason.FN_BAD_FORMAT, f"fn={fn_upper!r}"

    return True, None, None


def deduplicate_flights(flights):
    flights.sort(key=lambda x: x[1])
    seen = set()
    out = []
    for fn, dt, dep_ap, arr_ap, slot in flights:
        k = (fn, dt.date())
        if k not in seen:
            seen.add(k)
            out.append((fn, dt, dep_ap, arr_ap, slot))
    return out


def group_into_routes(flights):
    routes = []
    current = [flights[0]]
    route_start = flights[0][1]

    for f in flights[1:]:
        if is_same_route(f[1], route_start):
            current.append(f)
        else:
            routes.append(current)
            current = [f]
            route_start = f[1]
    routes.append(current)
    return routes


COL_NAMES = [
    "BookingId",
    "PaxName",
    "JourneyBucket",
    "BookingRef_PNR",
    "Airline",
    "ETicketNo",
    "FlightNumber1",
    "FlightNumber2",
    "FlightNumber3",
    "FlightNumber4",
    "FlightNumber5",
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
    "FlightDate5",
    "Airport1",
    "Airport2",
    "Airport3",
    "Airport4",
    "Airport5",
    "Airport6",
]


def insert_target_table(con, paired_rows, rejection_rows):
    """
    paired_rows: list of (out_row_data, rej_base) tuples.
    Deduplicates within batch, then against DB, logging rejections at each step.
    """
    if not paired_rows:
        return

    out_rows = [p[0] for p in paired_rows]
    rej_bases = [p[1] for p in paired_rows]  # index-safe: built from same list

    df_out = pd.DataFrame(out_rows, columns=COL_NAMES, dtype="object")
    df_out.replace("", None, inplace=True)

    for col in [
        "BookingId",
        "PaxName",
        "ETicketNo",
        "FlightNumber1",
        "FlightNumber2",
        "FlightNumber3",
        "FlightNumber4",
        "FlightNumber5",
        "Airport1",
        "Airport2",
        "Airport3",
        "Airport4",
        "Airport5",
        "Airport6",
    ]:
        df_out[col] = df_out[col].astype("string").str.strip()

    for dc in [
        "FlightDate1",
        "FlightDate2",
        "FlightDate3",
        "FlightDate4",
        "FlightDate5",
    ]:
        df_out[dc] = pd.to_datetime(df_out[dc], errors="coerce")

    # ── Stage 1: batch-level dedup ──────────────────────────────────────────────
    is_dup = df_out.duplicated(subset=KEY_COLS, keep="first")
    dup_idxs = df_out.index[is_dup].tolist()

    for i in dup_idxs:
        rejection_rows.append(
            rej_bases[i] + [Reason.BATCH_DUPLICATE, "Duplicate within batch"]
        )

    df_out = df_out[~is_dup].reset_index(drop=True)
    # Rebuild rej_bases to match the surviving rows (same order, no gaps)
    rej_bases = [
        rej_bases[i] for i in range(len(paired_rows)) if i not in set(dup_idxs)
    ]

    if df_out.empty:
        return

    # ── Stage 2: DB conflict check via staging table ────────────────────────────
    con.execute("DROP TABLE IF EXISTS _batch_staging")
    con.execute("""
        CREATE TEMP TABLE _batch_staging AS
        SELECT
            CAST(NULL AS VARCHAR)   AS BookingId,
            CAST(NULL AS VARCHAR)   AS PaxName,
            CAST(NULL AS VARCHAR)   AS JourneyBucket,
            CAST(NULL AS VARCHAR)   AS BookingRef_PNR,
            CAST(NULL AS VARCHAR)   AS Airline,
            CAST(NULL AS VARCHAR)   AS ETicketNo,
            CAST(NULL AS VARCHAR)   AS FlightNumber1,
            CAST(NULL AS VARCHAR)   AS FlightNumber2,
            CAST(NULL AS VARCHAR)   AS FlightNumber3,
            CAST(NULL AS VARCHAR)   AS FlightNumber4,
            CAST(NULL AS VARCHAR)   AS FlightNumber5,
            CAST(NULL AS TIMESTAMP) AS FlightDate1,
            CAST(NULL AS TIMESTAMP) AS FlightDate2,
            CAST(NULL AS TIMESTAMP) AS FlightDate3,
            CAST(NULL AS TIMESTAMP) AS FlightDate4,
            CAST(NULL AS TIMESTAMP) AS FlightDate5,
            CAST(NULL AS VARCHAR)   AS Airport1,
            CAST(NULL AS VARCHAR)   AS Airport2,
            CAST(NULL AS VARCHAR)   AS Airport3,
            CAST(NULL AS VARCHAR)   AS Airport4,
            CAST(NULL AS VARCHAR)   AS Airport5,
            CAST(NULL AS VARCHAR)   AS Airport6,
            CAST(NULL AS INTEGER)   AS _rej_idx  -- matches position in rej_bases
        WHERE 1=0
    """)

    df_staging = df_out.copy()
    df_staging["_rej_idx"] = range(len(df_staging))  # 0..N, lines up with rej_bases
    con.execute("INSERT INTO _batch_staging SELECT * FROM df_staging")

    # Find staging rows whose composite key already exists in the target
    conflict_idxs = {
        int(r[0])
        for r in con.execute(f"""
            SELECT s._rej_idx
            FROM _batch_staging s
            WHERE EXISTS (
                SELECT 1 FROM {TARGET_TABLE} t
                WHERE TRIM(s.BookingId)  = TRIM(t.BookingId)
                  AND TRIM(s.PaxName)    = TRIM(t.PaxName)
                  AND s.FlightNumber1    = t.FlightNumber1
                  AND CAST(s.FlightDate1 AS TIMESTAMP) = CAST(t.FlightDate1 AS TIMESTAMP)
                  AND s.Airport1         = t.Airport1
                  AND s.Airport2         = t.Airport2
            )
        """).fetchall()
    }

    for i in sorted(conflict_idxs):
        rejection_rows.append(
            rej_bases[i]
            + [Reason.DB_UNIQUE_VIOLATION, "Composite key already in target"]
        )

    # Insert only non-conflicting rows
    cols_str = ", ".join(COL_NAMES)
    if conflict_idxs:
        placeholders = ", ".join(str(i) for i in sorted(conflict_idxs))
        con.execute(f"""
            INSERT INTO {TARGET_TABLE} ({cols_str})
            SELECT {cols_str} FROM _batch_staging
            WHERE _rej_idx NOT IN ({placeholders})
        """)
    else:
        con.execute(f"""
            INSERT INTO {TARGET_TABLE} ({cols_str})
            SELECT {cols_str} FROM _batch_staging
        """)

    con.execute("DROP TABLE IF EXISTS _batch_staging")


def flush_rejections(con, rejection_rows):
    if not rejection_rows:
        return

    expected_cols = 25
    padded = []
    for r in rejection_rows:
        if len(r) < expected_cols:
            padded.append(r + [None] * (expected_cols - len(r)))
        else:
            padded.append(r[:expected_cols])

    df_rej = pd.DataFrame(padded, dtype="object")
    df_rej.replace("", None, inplace=True)
    con.execute(f"INSERT INTO {REJECTION_TABLE} SELECT * FROM df_rej")
    rejection_rows.clear()


def process_batch(con, row_id_start, rejection_rows):
    df = con.execute(f"""
        SELECT * FROM _cleaned_source
        WHERE _row_id BETWEEN {row_id_start} AND {row_id_start + BATCH_SIZE - 1}
    """).df()

    if df.empty:
        return 0

    # Each entry is (out_row_data, rej_base) — kept together, never drift apart
    paired_rows = []

    base_cols = [
        "BookingId",
        "PaxName",
        "JourneyBucket",
        "BookingRef_PNR",
        "Airline",
        "ETicketNo",
    ]
    raw_cols = [
        "RAW_FN1",
        "RAW_FN2",
        "RAW_FN3",
        "RAW_FN4",
        "RAW_FN5",
        "RAW_FD1",
        "RAW_FD2",
        "RAW_FD3",
        "RAW_FD4",
        "RAW_FD5",
    ]
    ap_cols = ["AP1", "AP2", "AP3", "AP4", "AP5", "AP6"]

    base_data = df[base_cols].values
    raw_data = df[raw_cols].values
    ap_data = df[ap_cols].values

    for idx, row in enumerate(df.itertuples(index=False)):

        def make_rej_base(idx=idx):
            bd, pax, jb, ref, al, etn = base_data[idx]
            fn1, fn2, fn3, fn4, fn5, fd1, fd2, fd3, fd4, fd5 = raw_data[idx]
            ap1, ap2, ap3, ap4, ap5, ap6 = ap_data[idx]
            return [
                str(bd) if bd is not None else None,
                str(pax) if pax is not None else None,
                str(jb) if jb is not None else None,
                str(ref) if ref is not None else None,
                str(al) if al is not None else None,
                str(etn) if etn is not None else None,
                str(fn1) if fn1 is not None else None,
                str(fn2) if fn2 is not None else None,
                str(fn3) if fn3 is not None else None,
                str(fn4) if fn4 is not None else None,
                str(fn5) if fn5 is not None else None,
                str(fd1) if fd1 is not None else None,
                str(fd2) if fd2 is not None else None,
                str(fd3) if fd3 is not None else None,
                str(fd4) if fd4 is not None else None,
                str(fd5) if fd5 is not None else None,
                str(ap1) if ap1 is not None else None,
                str(ap2) if ap2 is not None else None,
                str(ap3) if ap3 is not None else None,
                str(ap4) if ap4 is not None else None,
                str(ap5) if ap5 is not None else None,
                str(ap6) if ap6 is not None else None,
            ]

        flights = []
        slot_rejections = []

        for i in range(1, 6):
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
                rejection_rows.append(
                    rej_base
                    + [
                        primary[1],
                        f"No valid segments. Slots: {', '.join(f'FltNo{r[0]}({r[1]})' for r in slot_rejections)}",
                    ]
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
            fn_out, dt_out, ap_out = [None] * 5, [None] * 5, [None] * 6
            for i, (fn, dt, dep_ap, arr_ap, slot) in enumerate(route[:5]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                ap_out[i + 1] = arr_ap

            # ✅ Store data and its rej_base together — index alignment is guaranteed
            paired_rows.append((row_out + fn_out + dt_out + ap_out, rej_base))

    insert_target_table(con, paired_rows, rejection_rows)
    return len(paired_rows)


def main():

    log(f"🚀 Start {now_str()}")

    con = connect_db()
    create_target_table(con)
    create_rejection_table(con)
    create_clean_view(con)

    rejection_rows = []
    log_invalid_date_range_rows(con, rejection_rows)
    invalid_count = len(rejection_rows)
    if rejection_rows:
        flush_rejections(con, rejection_rows)
    log(f"📋 {invalid_count:,} rows logged as INVALID_DATE_RANGE")

    total = con.execute("SELECT COUNT(*) FROM _cleaned_source").fetchone()[0]
    log(f"📊 Cleaned rows: {total:,}")

    row_id = 1
    batch = 0

    while row_id <= total:
        batch += 1
        batch_start = time.time()
        log(
            f"🔄 Batch {batch} | rows {row_id:,} → {min(row_id + BATCH_SIZE - 1, total):,}"
        )

        batch_rejections = []
        rows_processed = process_batch(con, row_id, batch_rejections)
        if batch_rejections:
            flush_rejections(con, batch_rejections)
        row_id += BATCH_SIZE

        batch_time = time.time() - batch_start
        progress = (min(row_id, total) / total) * 100
        eta = (batch_time * max(total - row_id, 0) / BATCH_SIZE) / 3600
        log(
            f"✅ Batch {batch} | {rows_processed:,} rows | {progress:.1f}% | ETA {eta:.2f}h"
        )

    # Final reconciliation
    src = con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]
    tgt = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
    rej = con.execute(f"SELECT COUNT(*) FROM {REJECTION_TABLE}").fetchone()[0]
    log("\n📊 Reconciliation:")
    log(f"  Source      : {src:>10,}")
    log(f"  Target      : {tgt:>10,}")
    log(f"  Rejections  : {rej:>10,}")
    log(f"  Accounted   : {tgt + rej:>10,}")
    log(
        f"  Delta       : {src - tgt - rej:>10,}  ← should be 0 or route-split expansions"
    )

    log("🎉 ETL COMPLETED")
    con.close()


if __name__ == "__main__":
    main()
