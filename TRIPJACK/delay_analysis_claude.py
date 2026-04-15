import duckdb
import pandas as pd
from pathlib import Path
from datetime import timedelta
import logging

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TRIPJACK_DELAYED"

LOG_PATH = Path.home() / "Desktop/Gelen_Datalar/API/TRIPJACK/TRIPJACK_DELAYED/log.txt"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATETIME_COLS = [
    "ArrivalScheduledTimeLocal",
    "ArrivalRevisedTimeLocal",
    "ArrivalRunwayTimeLocal",
    "ArrivalScheduledTimeUtc",
    "ArrivalRevisedTimeUtc",
    "ArrivalRunwayTimeUtc",
    "DepartureScheduledTimeLocal",
    "DepartureRevisedTimeLocal",
    "DepartureRunwayTimeLocal",
    "DepartureScheduledTimeUtc",
    "DepartureRevisedTimeUtc",
    "DepartureRunwayTimeUtc",
    "DepartureDate",
]


# ==================================================
# NULL CHECKS & LOGGING
# ==================================================
def log_null_row(row: pd.Series) -> None:
    with open(LOG_PATH, "a") as f:
        f.write(
            f"{row.Id} {row.ConnectionID} {row.BookingRef} {row.PaxName} "
            f"{row.ETicketNo} {row.FlightNumber} {row.DepartureDate} "
            f"{row.FromAirport} {row.ToAirport}\n"
        )


def has_null_times(row: pd.Series) -> bool:
    # RunwayTime is often missing — we only strictly need Scheduled + at least one actual
    revised_missing = pd.isna(row.ArrivalRevisedTimeLocal)
    runway_missing = pd.isna(row.ArrivalRunwayTimeLocal)
    return (
        pd.isna(row.ArrivalScheduledTimeLocal)
        or (revised_missing and runway_missing)  # need at least one actual time
    )


def best_actual_arrival(row: pd.Series) -> pd.Timestamp:
    revised = row.ArrivalRevisedTimeLocal
    runway = row.ArrivalRunwayTimeLocal
    if pd.isna(revised) and pd.isna(runway):
        return pd.NaT
    if pd.isna(runway):
        return revised
    if pd.isna(revised):
        return runway
    return max(revised, runway)


def arrival_delay(row: pd.Series) -> timedelta:
    return best_actual_arrival(row) - row.ArrivalScheduledTimeLocal


def group_has_nulls(group: pd.DataFrame) -> bool:
    for _, row in group.iterrows():
        if has_null_times(row):
            log_null_row(row)
            return True
    return False


# ==================================================
# DATA LOADING
# ==================================================


def load_groups() -> tuple[list[pd.DataFrame], list[pd.DataFrame]]:
    with duckdb.connect(DB_PATH) as con:
        df = con.execute(f"SELECT * FROM {SOURCE_TABLE}").df()

    if df.empty:
        logger.info("No data found in %s", SOURCE_TABLE)
        return [], []

    for col in DATETIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col].replace("NULL", pd.NaT), errors="coerce")

    grouped = df.groupby("ConnectionID")

    single_list: list[pd.DataFrame] = []
    multi_list: list[pd.DataFrame] = []

    for _, group in grouped:
        if group_has_nulls(group):
            continue
        if len(group) == 1:
            single_list.append(group)
        else:
            multi_list.append(group)

    return single_list, multi_list


# ==================================================
# ELIGIBILITY CHECKS
# ==================================================


def arrival_delay(row: pd.Series) -> timedelta:
    return (
        max(row.ArrivalRevisedTimeLocal, row.ArrivalRunwayTimeLocal)
        - row.ArrivalScheduledTimeLocal
    )


def is_single_eligible(group: pd.DataFrame) -> bool:
    return arrival_delay(group.iloc[0]) >= timedelta(minutes=165)


def is_multi_eligible(group: pd.DataFrame) -> bool:
    for i in range(len(group) - 1):
        row = group.iloc[i]
        next_row = group.iloc[i + 1]
        missed_connection_buffer = next_row.DepartureScheduledTimeLocal - max(
            row.ArrivalRevisedTimeLocal, row.ArrivalRunwayTimeLocal
        )
        if missed_connection_buffer <= timedelta(minutes=45):
            return True
    # Check whether the final leg itself had a long delay
    return is_single_eligible(group.iloc[[-1]])


# ==================================================
# MAIN
# ==================================================


def main() -> None:
    single_list, multi_list = load_groups()

    eligible: list[pd.DataFrame] = []

    for group in single_list:
        if is_single_eligible(group):
            eligible.append(group)

    for group in multi_list:
        if is_multi_eligible(group):
            eligible.append(group)

    logger.info("Single groups: %d", len(single_list))
    logger.info("Multi groups:  %d", len(multi_list))
    logger.info("Eligible:      %d", len(eligible))
    print(eligible)


if __name__ == "__main__":
    main()
