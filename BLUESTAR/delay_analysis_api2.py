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

SOURCE_TABLE = "BLUESTAR_DELAYED_API"

LOG_PATH = (
    Path.home()
    / "/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/FILTER-3(API)/DELAYED/logApi.txt"
)

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
    revised_missing = pd.isna(row.ArrivalRevisedTimeLocal)
    runway_missing = pd.isna(row.ArrivalRunwayTimeLocal)
    return pd.isna(row.ArrivalScheduledTimeLocal) or (
        revised_missing and runway_missing
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

    df = df.sort_values(["ConnectionID", "LegNo"])

    grouped = df.groupby("ConnectionID")

    single_list: list[pd.DataFrame] = []
    multi_list: list[pd.DataFrame] = []

    for _, group in grouped:
        if group_has_nulls(group):
            continue
        if len(group) == 1:
            single_list.append(group.copy())
        else:
            multi_list.append(group.copy())
    return single_list, multi_list


# ==================================================
# ELIGIBILITY CHECKS
# ==================================================
def is_single_eligible(group: pd.DataFrame) -> bool:
    last_row = group.iloc[-1]
    delay = pd.Timedelta(arrival_delay(last_row))
    group["DelayTime"] = pd.NA
    group["DelayRowId"] = pd.NA
    group.loc[group.index[-1], "DelayTime"] = to_seconds(delay)
    group.loc[group.index[-1], "DelayRowId"] = str(
        last_row["Id"]
    )  # ✅ store Id of delayed row
    group["Eligible"] = delay >= timedelta(minutes=165)
    return bool(group["Eligible"].iloc[-1])


def is_multi_eligible(group: pd.DataFrame) -> bool:
    for i in range(len(group) - 1):
        row = group.iloc[i]
        next_row = group.iloc[i + 1]
        actual_arrival = best_actual_arrival(row)
        if pd.isna(actual_arrival):
            continue
        missed_connection_buffer = next_row.DepartureScheduledTimeLocal - actual_arrival
        if missed_connection_buffer <= timedelta(minutes=45):
            col_name = f"LayOverTime{i + 1}"
            seconds = to_seconds(missed_connection_buffer)
            if col_name not in group.columns:
                group[col_name] = pd.NA
            group.loc[group.index[i], col_name] = seconds
            if "DelayTime" not in group.columns:
                group["DelayTime"] = pd.NA
            if "DelayRowId" not in group.columns:
                group["DelayRowId"] = pd.NA
            group.loc[group.index[i], "DelayTime"] = seconds
            group.loc[group.index[i], "DelayRowId"] = str(
                row["Id"]
            )  # ✅ store Id of missed connection row
            group["Eligible"] = True
            return True
    return is_single_eligible(group)


# ==================================================
# DB UPDATE
# ==================================================
def to_seconds(val) -> int | None:
    """Convert timedelta, pd.Timedelta, or numpy int64 (nanoseconds) to integer seconds."""
    try:
        if val is None or pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass  # pd.isna() can throw on some types — if so, it's not NaT
    if isinstance(val, (pd.Timedelta, timedelta)):
        return int(val.total_seconds())
    if hasattr(val, "item"):  # numpy scalar → python int (nanoseconds)
        return int(val.item()) // 1_000_000_000
    return int(val) // 1_000_000_000


def set_eligible_status(eligible_groups: list[pd.DataFrame]) -> None:
    with duckdb.connect(DB_PATH) as con:
        for group in eligible_groups:
            connection_id = str(group["ConnectionID"].iloc[0])

            # ✅ read DelayRowId set by eligibility functions
            delay_rows = group[group["DelayRowId"].notna()]
            if not delay_rows.empty:
                delay_seconds = int(delay_rows["DelayTime"].iloc[0])
                row_id = str(delay_rows["DelayRowId"].iloc[0])
                con.execute(
                    f"UPDATE {SOURCE_TABLE} "
                    f"SET IsDelayEligible = 1, DelayTime = ? "
                    f"WHERE Id = ?",
                    [delay_seconds, row_id],
                )

            layover_cols = [c for c in group.columns if c.startswith("LayOverTime")]
            for col in layover_cols:
                for idx in group.index:
                    val = group.loc[idx, col]
                    if pd.isna(val):
                        continue
                    row_id = str(group.loc[idx, "Id"])  # ✅ exact row Id
                    con.execute(
                        f'UPDATE {SOURCE_TABLE} SET "{col}" = ? WHERE Id = ?',
                        [int(val), row_id],
                    )


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

    if eligible:
        set_eligible_status(eligible)

    print(eligible)


if __name__ == "__main__":
    main()
