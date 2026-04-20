import pandas as pd

# Read all sheets from the Excel file
file_path = "/home/kayhan/Desktop/Gelen_Datalar/NEW_DATA_MURAT/GCC/GCC 2025 (C2R).xlsx"
sheets = pd.ExcelFile(file_path).sheet_names

# Concatenate all sheets into one DataFrame
df_list = [pd.read_excel(file_path, sheet_name=sheet) for sheet in sheets]
df = pd.concat(df_list, ignore_index=True)

# Split into chunks of 200,000 rows
chunk_size = 200_000
num_chunks = (len(df) + chunk_size - 1) // chunk_size  # ceiling division

for i in range(num_chunks):
    start = i * chunk_size
    end = start + chunk_size
    chunk = df.iloc[start:end]
    chunk.to_excel(
        f"/home/kayhan/Desktop/Gelen_Datalar/NEW_DATA_MURAT/GCC/output_part_{i + 1}.xlsx",
        index=False,
    )

print(f"Created {num_chunks} Excel files.")
