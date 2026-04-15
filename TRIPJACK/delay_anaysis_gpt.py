import duckdb
import pandas as pd
from pathlib import Path
from datetime import timedelta

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TRIPJACK_DELAYED"

LOG_FILE = Path(
    "/home/kayhan/Desktop/Gelen_Datalar/API/TRIPJACK/TRIPJACK_DELAYED/log.txt"
)


# ==================================================
# LOGGING (VECTORIZED)
# ==================================================
def log_null_rows(df: pd.DataFrame):
    mask = (
        df["ArrivalScheduledTimeLocal"].isna()
        | df["ArrivalRevisedTimeLocal"].isna()
        | df["ArrivalRunwayTimeLocal"].isna()
    )

    bad_rows = df[mask]

    if not bad_rows.empty:
        with open(LOG_FILE, "a") as f:
            for _, r in bad_rows.iterrows():
                f.write(
                    f"{r.get('Id', '')},{r.get('ConnectionID', '')},{r.get('BookingId', '')},"
                    f"{r.get('PaxName', '')},{r.get('ETicketNo', '')},{r.get('FlightNumber', '')},"
                    f"{r.get('DepartureDate', '')},{r.get('ArrivalDate', '')},"
                    f"{r.get('DepartureAirport', '')},{r.get('ArrivalAirport', '')}\n"
                )

    return mask


# ==================================================
# ELIGIBILITY LOGIC
# ==================================================
def is_single_eligible(df: pd.DataFrame) -> bool:
    row = df.iloc[0]

    actual_arrival = row[["ArrivalRevisedTimeLocal", "ArrivalRunwayTimeLocal"]].max()

    delay = actual_arrival - row["ArrivalScheduledTimeLocal"]

    return delay >= timedelta(minutes=165)


def is_multi_eligible(df: pd.DataFrame) -> bool:
    df = df.sort_values("DepartureScheduledTimeLocal")

    for i in range(len(df)):
        row = df.iloc[i]

        actual_arrival = row[
            ["ArrivalRevisedTimeLocal", "ArrivalRunwayTimeLocal"]
        ].max()

        # If not last flight
        if i < len(df) - 1:
            next_row = df.iloc[i + 1]

            gap = next_row["DepartureScheduledTimeLocal"] - actual_arrival

            if gap <= timedelta(minutes=45):
                return True
        else:
            # Last segment → fallback to single logic
            return is_single_eligible(df.iloc[[i]])

    return False


# ==================================================
# MAIN PROCESS (CHUNKED + MEMORY SAFE)
# ==================================================
def process_data():
    con = duckdb.connect(DB_PATH)

    eligible_groups = []

    query = f"SELECT * FROM {SOURCE_TABLE}"

    con.execute(query)

    total_rows = 0

    while True:
        chunk = con.fetch_df_chunk(100_000)
        if chunk is None or chunk.empty:
            break

        total_rows += len(chunk)
        print(f"Processing chunk... total rows so far: {total_rows}")

        # --------------------------------------------------
        # Remove null rows (and log them)
        # --------------------------------------------------
        null_mask = log_null_rows(chunk)
        chunk = chunk[~null_mask]

        if chunk.empty:
            continue

        # --------------------------------------------------
        # Group processing
        # --------------------------------------------------
        for cid, group in chunk.groupby("ConnectionID"):
            if len(group) == 1:
                if is_single_eligible(group):
                    eligible_groups.append(group)
            else:
                if is_multi_eligible(group):
                    eligible_groups.append(group)

    con.close()

    print(f"\nTotal processed rows: {total_rows}")
    print(f"Eligible groups found: {len(eligible_groups)}")

    return eligible_groups


# ==================================================
# ENTRY POINT
# ==================================================
def main():
    eligible = process_data()

    # Avoid printing full data (can be huge)
    print(f"\nFinal eligible group count: {len(eligible)}")


if __name__ == "__main__":
    main()
