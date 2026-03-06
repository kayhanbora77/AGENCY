import pandas as pd
import os

# CONFIGURATION
file_path = "/home/kayhan/Desktop/Gelen_Datalar/AKBAR/raw_data.xlsx"
ROWS_PER_FILE = 100000
COLUMNS_TO_DELETE = ["WEEK", "BspFileYear_NB", "BspFileMonth_NB", "BspFileWeek_NB"]


def extract_airports(df):
    """
    Reads SectorSplit1-4 (or Sector1-4) columns AFTER they have been populated,
    and returns a DataFrame with exactly 5 Airport columns.
    """
    sector_split_cols = ["SectorSplit1", "SectorSplit2", "SectorSplit3", "SectorSplit4"]
    sector_cols = ["Sector1", "Sector2", "Sector3", "Sector4"]

    if any(c in df.columns for c in sector_split_cols):
        all_sector_cols = [c for c in sector_split_cols if c in df.columns]
    elif any(c in df.columns for c in sector_cols):
        all_sector_cols = [c for c in sector_cols if c in df.columns]
    else:
        print("  WARNING: No sector columns found for airport extraction.")
        return df

    print(f"  Extracting airports from: {all_sector_cols}")

    def get_airport_chain(row):
        chain = []
        for col in all_sector_cols:
            raw = row[col]
            if pd.isna(raw):
                continue
            val = str(raw).strip().upper()
            if val in ("", "NAN", "NONE"):
                continue
            if "/" not in val:
                continue
            parts = [p.strip() for p in val.split("/")]
            if len(parts) < 2:
                continue
            if not chain:
                chain.append(parts[0])  # first origin only once
            chain.append(parts[1])  # every destination
        return chain

    airport_data = df.apply(get_airport_chain, axis=1).tolist()

    # Build exactly 5 Airport columns
    airport_df = pd.DataFrame(airport_data, index=df.index)
    for i in range(5):
        if i not in airport_df.columns:
            airport_df[i] = ""
        else:
            airport_df[i] = airport_df[i].fillna("")
    airport_df = airport_df.iloc[:, :5]
    airport_df.columns = [f"Airport{i + 1}" for i in range(5)]

    print(f"  Sample airport extraction (first 5 rows):\n{airport_df.head()}\n")

    # Step 1: Drop SectorSplit columns and add Airport columns at the end temporarily
    df_no_sectors = df.drop(columns=all_sector_cols, errors="ignore")
    df_with_airports = pd.concat([df_no_sectors, airport_df], axis=1)

    # Step 2: Explicitly reorder so Airport1-5 appear right after LastSectordate
    airport_cols = [f"Airport{i + 1}" for i in range(5)]
    if "LastSectordate" in df_with_airports.columns:
        all_cols = list(df_with_airports.columns)
        # Remove airport cols from wherever they are
        remaining_cols = [c for c in all_cols if c not in airport_cols]
        # Find LastSectordate position in remaining cols
        last_sector_idx = remaining_cols.index("LastSectordate") + 1
        # Insert airport cols right after LastSectordate
        final_col_order = (
            remaining_cols[:last_sector_idx]
            + airport_cols
            + remaining_cols[last_sector_idx:]
        )
        df = df_with_airports[final_col_order]
    else:
        df = df_with_airports

    return df


def process_and_split(file_path, rows_per_chunk):
    print(f"[{pd.Timestamp.now()}] Step 1: Loading file...")
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
    except Exception as e:
        print(f"Error reading Excel: {e}")
        return

    # 1. Clean header names immediately (removes spaces/hidden chars)
    df.columns = df.columns.str.strip()

    # 2. Delete unwanted columns
    df.drop(columns=COLUMNS_TO_DELETE, errors="ignore", inplace=True)

    # --- STEP 3: FTDA Date Logic ---
    if "FTDA" in df.columns and "DAIS" in df.columns:
        print(f"[{pd.Timestamp.now()}] Step 3: Processing FTDA dates...")
        years = pd.to_datetime(df["DAIS"]).dt.year.astype(str)
        ftda_split = df["FTDA"].astype(str).str.strip().str.split(r"\s+", expand=True)
        for col in ftda_split.columns:
            combined = ftda_split[col] + years
            ftda_split[col] = pd.to_datetime(
                combined, format="%d%b%Y", errors="coerce"
            ).dt.date
        ftda_split.columns = [f"FlightDate{i + 1}" for i in range(ftda_split.shape[1])]
        f_idx = df.columns.get_loc("FTDA")
        df = df.drop(columns=["FTDA"])
        df = pd.concat([df.iloc[:, :f_idx], ftda_split, df.iloc[:, f_idx:]], axis=1)

    # --- STEP 4: FlightNo Split ---
    if "FlightNo" in df.columns:
        print(f"[{pd.Timestamp.now()}] Step 4: Splitting FlightNo...")
        fno_split = (
            df["FlightNo"].astype(str).str.strip().str.split(r"\s+", expand=True)
        )
        fno_split.columns = [f"FlightNumber{i + 1}" for i in range(fno_split.shape[1])]
        n_idx = df.columns.get_loc("FlightNo")
        df = df.drop(columns=["FlightNo"])
        df = pd.concat([df.iloc[:, :n_idx], fno_split, df.iloc[:, n_idx:]], axis=1)

    # --- STEP 5: Sector Split -> SectorSplit1/2/3/4 ---
    # This MUST happen before airport extraction
    if "Sector" in df.columns:
        print(f"[{pd.Timestamp.now()}] Step 5: Splitting Sector column...")
        s_split = df["Sector"].astype(str).str.strip().str.split(r"\s+", expand=True)
        s_split.columns = [f"SectorSplit{i + 1}" for i in range(s_split.shape[1])]
        s_idx = df.columns.get_loc("Sector")
        df = df.drop(columns=["Sector"])
        df = pd.concat([df.iloc[:, :s_idx], s_split, df.iloc[:, s_idx:]], axis=1)

    # --- STEP 6: AIRPORT EXTRACTION (runs AFTER SectorSplit columns are populated) ---
    print(f"[{pd.Timestamp.now()}] Step 6: Extracting Airport1-Airport5...")
    df = extract_airports(df)

    # --- STEP 7: SAVE ---
    total_rows = len(df)
    base_name = os.path.splitext(file_path)[0]
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i : i + rows_per_chunk]
        part_num = (i // rows_per_chunk) + 1
        output_file = f"{base_name}_part{part_num}.xlsx"
        print(f"Saving {output_file}...")
        chunk.to_excel(output_file, index=False)

    print(f"[{pd.Timestamp.now()}] Done!")


def main():
    if os.path.exists(file_path):
        process_and_split(file_path, ROWS_PER_FILE)
    else:
        print("File not found.")


if __name__ == "__main__":
    main()
