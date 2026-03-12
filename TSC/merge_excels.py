import pandas as pd
from pathlib import Path

input_folder = Path("/home/kayhan/Desktop/Gelen_Datalar/TSC/TEMP")
output_file = Path("/home/kayhan/Desktop/Gelen_Datalar/TSC/TEMP/TSC_RAW_MERGED.xlsx")

dfs = []

for file in input_folder.glob("*.xls"):
    print(f"⏰ Loading {file.name}")

    try:
        tables = pd.read_html(file)

        for df in tables:
            if not df.empty:
                dfs.append(df)

    except Exception as e:
        print(f"❌ Failed to read {file.name}: {e}")

if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_excel(output_file, index=False)

    print("✅ All files merged successfully")
else:
    print("❌ No data loaded")
