import pandas as pd

# load file
df = pd.read_excel(
    "/home/kayhan/Desktop/Gelen_Datalar/AKBAR/PROCEED/V1/JAN to DEC 2020 for C2R_part1.xlsx",
    dtype=str,  # Read everything as string to avoid Excel auto-conversion issues
)

# get year from main date column
df["year"] = pd.to_datetime(df["FirstSectordate"]).dt.year


# function to fix JAN style dates (e.g. "06JAN" -> "06JAN2020")
def fix_date(val, year):
    if pd.isna(val) or str(val).strip() in ("", "nan", "NaT", "NaN"):
        return None

    val_str = str(val).strip()

    # Handle DDMMM format (e.g. "10JAN", "01FEB")
    if len(val_str) == 5 and val_str[:2].isdigit() and val_str[2:].isalpha():
        try:
            return pd.to_datetime(f"{val_str}{int(year)}", format="%d%b%Y")
        except Exception:
            pass

    # Try parsing as normal date
    try:
        return pd.to_datetime(val_str)
    except Exception:
        return val


# columns containing JAN format
cols = [
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
    "FlightDate5",
    "FlightDate6",
    "FlightDate7",
    "FlightDate8",
]

for c in cols:
    if c in df.columns:
        df[c] = df.apply(lambda r: fix_date(r[c], r["year"]), axis=1)

df.drop(columns=["year"], inplace=True)

df.to_excel(
    "/home/kayhan/Desktop/Gelen_Datalar/AKBAR/PROCEED/FIXED/JAN to DEC 2020 for C2R_part1_fixed.xlsx",
    index=False,
)

print("Done!")
