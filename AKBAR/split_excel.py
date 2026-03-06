import pandas as pd
import os

file_path = "/home/kayhan/Desktop/Gelen_Datalar/AKBAR/JAN to DEC 2020 for C2R.xlsx"
ROWS_PER_FILE = 100000


def split_excel_by_rows(file_path, rows_per_chunk):
    print(f"[{pd.Timestamp.now()}] Reading large file (this may take a minute)...")

    # Load the entire dataframe
    df = pd.read_excel(file_path)
    total_rows = len(df)

    print(f"Total rows found: {total_rows:,}")

    base_name = os.path.splitext(file_path)[0]

    # Loop through the dataframe in steps of 100,000
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i : i + rows_per_chunk]

        # Calculate part number (1, 2, 3...)
        part_num = (i // rows_per_chunk) + 1
        output_file = f"{base_name}_part{part_num}.xlsx"

        print(f"Saving {output_file} ({len(chunk):,} rows)...")
        chunk.to_excel(output_file, index=False)

    print(f"[{pd.Timestamp.now()}] Done! Created {part_num} files.")


def main():
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    split_excel_by_rows(file_path, ROWS_PER_FILE)


if __name__ == "__main__":
    main()
