import pandas as pd

df_all = pd.read_csv(
    "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/MATCHED/all_trusttravel_flights.csv"
)
df_matched = pd.read_csv(
    "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/MATCHED/matched_trusttravel_with_euflights.csv"
)

df_all["FlightDate"] = pd.to_datetime(df_all["FlightDate"])
df_matched["FlightDate"] = pd.to_datetime(df_matched["FlightDate"])

diff_all_not_in_matched = (
    df_all.merge(df_matched, how="left", indicator=True)
    .query('_merge == "left_only"')
    .drop("_merge", axis=1)
)
# diff_all_not_in_matched["source"] = "all_trusttravel_flights"


diff_all_not_in_matched.to_csv(
    "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/MATCHED/not_matched_trusttravel.csv",
    index=False,
)
