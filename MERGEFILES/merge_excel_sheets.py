import pandas as pd

# Input file
input_file = (
    "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/TRUST_TRAVEL_RAW_DATA.xlsx"
)

# Read all sheets
all_sheets = pd.read_excel(input_file, sheet_name=None)

# Concatenate all sheets into one DataFrame
combined_df = pd.concat(all_sheets.values(), ignore_index=True)

# Save to new Excel file
combined_df.to_excel(
    "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/TRUSTTRAVEL_RAW_MERGED.xlsx",
    index=False,
)

print("✅ All sheets merged into one file successfully!")
