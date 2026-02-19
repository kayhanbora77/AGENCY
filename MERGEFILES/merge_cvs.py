import pandas as pd
import glob

# Path to all CSV files (example: all csv files in a folder)
files = glob.glob("/home/kayhan/Desktop/Gelen_Datalar/TBO/RAW/TBO3/*.csv")

# Read and concatenate
df = pd.concat((pd.read_csv(file) for file in files), ignore_index=True)

# Save to new CSV
df.to_csv(
    "/home/kayhan/Desktop/Gelen_Datalar/TBO/RAW/TBO3/TBO_RAW_MERGED.csv", index=False
)

print("✅ All CSV files merged successfully!")
