import pandas as pd
import glob

# Get all Excel files
files = glob.glob("/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/PROCEED/*.xlsx")

# Read and concatenate
df = pd.concat((pd.read_excel(file) for file in files), ignore_index=True)

# Save to new Excel file
df.to_excel(
    "/home/kayhan/Desktop/Gelen_Datalar/TCG/RAW/TCG_RAW_MERGED.xlsx", index=False
)

print("✅ All Excel files merged successfully!")
