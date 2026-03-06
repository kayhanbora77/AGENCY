import pandas as pd
import duckdb
import os
import math
import pyarrow as pa
from pathlib import Path

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

FILE_PATH = "/home/kayhan/Desktop/Gelen_Datalar/AKBAR/RAW/JAN to DEC 2024 for C2R.xlsx"
TABLE_NAME = "AKBAR_2024"
CHUNK_SIZE = 100_000

COLUMNS_TO_DELETE = ["WEEK", "BspFileYear_NB", "BspFileMonth_NB", "BspFileWeek_NB"]
# ──────────────────────────────────────────────────────────────────────────────


# ─── VECTORIZED TRANSFORMS ────────────────────────────────────────────────────
def split_flightno(df: pd.DataFrame) -> pd.DataFrame:
    if "FlightNo" not in df.columns:
        return df
    split = df["FlightNo"].astype(str).str.strip().str.split(r"\s+", expand=True)
    split.columns = [f"FlightNumber{i + 1}" for i in range(split.shape[1])]
    idx = df.columns.get_loc("FlightNo")
    return pd.concat([df.iloc[:, :idx], split, df.iloc[:, idx + 1 :]], axis=1)


def split_sector(df: pd.DataFrame) -> pd.DataFrame:
    if "Sector" not in df.columns:
        return df
    split = df["Sector"].astype(str).str.strip().str.split(r"\s+", expand=True)
    split.columns = [f"SectorSplit{i + 1}" for i in range(split.shape[1])]
    idx = df.columns.get_loc("Sector")
    return pd.concat([df.iloc[:, :idx], split, df.iloc[:, idx + 1 :]], axis=1)


def split_ftda(df: pd.DataFrame) -> pd.DataFrame:
    if "FTDA" not in df.columns or "DAIS" not in df.columns:
        return df
    years = (
        pd.to_datetime(df["DAIS"], errors="coerce").dt.year.astype("Int64").astype(str)
    )
    split = df["FTDA"].astype(str).str.strip().str.split(r"\s+", expand=True)
    for col in split.columns:
        split[col] = pd.to_datetime(
            split[col] + years, format="%d%b%Y", errors="coerce"
        ).dt.date
    split.columns = [f"FlightDate{i + 1}" for i in range(split.shape[1])]
    idx = df.columns.get_loc("FTDA")
    return pd.concat([df.iloc[:, :idx], split, df.iloc[:, idx + 1 :]], axis=1)


def extract_airports(df: pd.DataFrame) -> pd.DataFrame:
    candidates = ["SectorSplit1", "SectorSplit2", "SectorSplit3", "SectorSplit4"]
    fallback = ["Sector1", "Sector2", "Sector3", "Sector4"]
    sector_cols = [c for c in candidates if c in df.columns] or [
        c for c in fallback if c in df.columns
    ]

    if not sector_cols:
        return df

    origins, dests = [], []
    for col in sector_cols:
        s = df[col].astype(str).str.strip().str.upper()
        mask = s.str.contains("/", na=False) & ~s.isin(["NAN", "NONE", ""])
        split2 = s.where(mask).str.split("/", n=1, expand=True)
        origins.append(split2[0].str.strip())
        dests.append(
            split2[1].str.strip()
            if 1 in split2.columns
            else pd.Series("", index=df.index)
        )

    first_origin = pd.Series("", index=df.index)
    for o in origins:
        first_origin = first_origin.where(first_origin != "", o.fillna(""))

    airport_parts = [first_origin.fillna("")] + [d.fillna("") for d in dests]
    while len(airport_parts) < 5:
        airport_parts.append(pd.Series("", index=df.index))
    airport_parts = airport_parts[:5]

    airport_df = pd.concat(airport_parts, axis=1)
    airport_df.columns = [f"Airport{i + 1}" for i in range(5)]

    df = df.drop(columns=sector_cols, errors="ignore")
    df = pd.concat([df, airport_df], axis=1)

    airport_col_names = [f"Airport{i + 1}" for i in range(5)]
    if "LastSectordate" in df.columns:
        cols = [c for c in df.columns if c not in airport_col_names]
        idx = cols.index("LastSectordate") + 1
        df = df[cols[:idx] + airport_col_names + cols[idx:]]

    return df


def transform_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk.columns = chunk.columns.str.strip()
    chunk.drop(columns=COLUMNS_TO_DELETE, errors="ignore", inplace=True)
    chunk = split_ftda(chunk)
    chunk = split_flightno(chunk)
    chunk = split_sector(chunk)
    chunk = extract_airports(chunk)
    return chunk


# ─── SCHEMA DETECTION ─────────────────────────────────────────────────────────
def detect_full_schema(raw_df: pd.DataFrame) -> list[str]:
    """
    Transform a small sample from the START, MIDDLE and END of the file
    so we see the maximum number of dynamic columns (FlightNumber1..N,
    SectorSplit1..N, FlightDate1..N) that can ever appear, then return
    the authoritative ordered column list.
    """
    total = len(raw_df)
    # Sample indices: first 500, middle 500, last 500 rows
    sample_idx = list(range(min(500, total)))
    mid = total // 2
    sample_idx += list(range(max(0, mid - 250), min(total, mid + 250)))
    sample_idx += list(range(max(0, total - 500), total))
    sample_idx = sorted(set(sample_idx))

    sample = raw_df.iloc[sample_idx].copy()
    transformed = transform_chunk(sample)
    return list(transformed.columns)


def align_chunk_to_schema(chunk: pd.DataFrame, schema_cols: list[str]) -> pd.DataFrame:
    """
    Add any missing columns (as empty string) and reorder to match schema exactly.
    Extra columns not in schema are dropped.
    """
    for col in schema_cols:
        if col not in chunk.columns:
            chunk[col] = ""
    return chunk[schema_cols]


# ─── DUCKDB HELPERS ───────────────────────────────────────────────────────────
DATE_COLUMNS = {
    "DAIS",
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
    "FirstSectordate",
    "LastSectordate",
}


def create_table(con: duckdb.DuckDBPyConnection, schema_cols: list[str]):
    """Create table — DATE for known date columns, VARCHAR for everything else."""
    col_defs = []
    for c in schema_cols:
        col_type = "DATE" if c in DATE_COLUMNS else "VARCHAR"
        col_defs.append(f'"{c}" {col_type}')
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"CREATE TABLE {TABLE_NAME} ({', '.join(col_defs)})")
    date_count = sum(1 for c in schema_cols if c in DATE_COLUMNS)
    print(
        f"  Table '{TABLE_NAME}' created with {len(schema_cols)} columns ({date_count} DATE columns)."
    )


def parse_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DATE_COLUMNS to datetime.date objects (YYYY-MM-DD). Invalid values become None → NULL."""
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def insert_chunk(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """Bulk-insert via PyArrow — fastest path into DuckDB."""
    df = parse_date_columns(df)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM arrow_table")


# ─── MAIN PIPELINE ────────────────────────────────────────────────────────────
def process_to_duckdb(file_path: str, db_path: str):
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[{pd.Timestamp.now()}] Loading Excel file...")
    raw_df = pd.read_excel(file_path, engine="openpyxl", dtype=str)
    total_rows = len(raw_df)
    total_chunks = math.ceil(total_rows / CHUNK_SIZE)
    print(
        f"[{pd.Timestamp.now()}] {total_rows:,} rows → {total_chunks} chunks of {CHUNK_SIZE:,}"
    )

    # ── Detect full schema ONCE before processing ──────────────────────────
    print(f"[{pd.Timestamp.now()}] Detecting full column schema from sample rows...")
    schema_cols = detect_full_schema(raw_df)
    print(f"  Schema has {len(schema_cols)} columns: {schema_cols}")

    # ── Connect and create table ───────────────────────────────────────────
    print(f"[{pd.Timestamp.now()}] Opening DuckDB at: {db_path}")
    con = duckdb.connect(str(db_path))
    create_table(con, schema_cols)

    # ── Process and insert chunk by chunk ──────────────────────────────────
    for chunk_num, start in enumerate(range(0, total_rows, CHUNK_SIZE), 1):
        end = min(start + CHUNK_SIZE, total_rows)
        chunk = raw_df.iloc[start:end].copy()
        print(
            f"[{pd.Timestamp.now()}] Chunk {chunk_num}/{total_chunks} (rows {start:,}–{end:,})...",
            end=" ",
        )

        chunk = transform_chunk(chunk)
        chunk = align_chunk_to_schema(
            chunk, schema_cols
        )  # ← guarantees exact column match
        insert_chunk(con, chunk)
        print(f"✓ inserted {len(chunk):,} rows")

    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"\n[{pd.Timestamp.now()}] ✅ Done! {count:,} rows in '{TABLE_NAME}'")
    print(f"  DB: {db_path}")
    con.close()


def main():
    if not os.path.exists(FILE_PATH):
        print(f"File not found: {FILE_PATH}")
        return
    process_to_duckdb(FILE_PATH, DB_PATH)


if __name__ == "__main__":
    main()
