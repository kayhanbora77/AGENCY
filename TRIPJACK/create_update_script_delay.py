import pandas as pd


def parse_datetime(value):
    """Convert datetime string to SQL Server format"""
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        dt = pd.to_datetime(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


def get_max_datetime(dt1, dt2):
    """Return the maximum of two datetime values"""
    dt1_parsed = parse_datetime(dt1)
    dt2_parsed = parse_datetime(dt2)

    if dt1_parsed is None and dt2_parsed is None:
        return None
    if dt1_parsed is None:
        return dt2_parsed
    if dt2_parsed is None:
        return dt1_parsed
    return max(dt1_parsed, dt2_parsed)


def format_sql_value(value):
    """Format value for SQL Server"""
    if value is None or pd.isna(value) or str(value).strip() == "":
        return "NULL"
    return f"'{value}'"


def generate_bulk_update_sql(csv_file_path, output_file_path=None, batch_size=1000):
    """
    Read CSV and generate set-based UPDATE statements using SQL Server VALUES constructor.
    Automatically chunks into batches of 1000 to respect SQL Server's VALUES limit.
    """
    df = pd.read_csv(csv_file_path)
    value_tuples = []

    for _, row in df.iterrows():
        record_id = row.get("Id")
        delay_time = row.get("DelayTime")
        arrival_scheduled = parse_datetime(row.get("ArrivalScheduledTimeLocal"))
        arrival_revised = row.get("ArrivalRevisedTimeLocal")
        arrival_runway = row.get("ArrivalRunwayTimeLocal")
        departure_scheduled = parse_datetime(row.get("DepartureScheduledTimeLocal"))
        departure_revised = row.get("DepartureRevisedTimeLocal")
        departure_runway = row.get("DepartureRunwayTimeLocal")

        # Calculate MAX for Actual times
        actual_arrival = get_max_datetime(arrival_revised, arrival_runway)
        actual_departure = get_max_datetime(departure_revised, departure_runway)

        # Build tuple string matching exact requested format
        t = (
            f"({format_sql_value(record_id)},"
            f"{format_sql_value(delay_time)},"
            f"{format_sql_value(arrival_scheduled)},"
            f"{format_sql_value(actual_arrival)},"
            f"{format_sql_value(departure_scheduled)},"
            f"{format_sql_value(actual_departure)})"
        )
        value_tuples.append(t)

    # SQL Server limits VALUES constructor to 1000 rows
    chunks = [
        value_tuples[i : i + batch_size]
        for i in range(0, len(value_tuples), batch_size)
    ]
    full_statements = []

    for i, chunk in enumerate(chunks, 1):
        values_block = ",\n    ".join(chunk)
        sql = f"""UPDATE t
SET t.DelayInSecond = u.DelayInSecond,
    t.ScheduledArrivalTimeLocal = u.ScheduledArrivalTimeLocal,
    t.ActualArrivalTimeLocal = u.ActualArrivalTimeLocal,
    t.ScheduledDepartureTimeLocal = u.ScheduledDepartureTimeLocal,
    t.ActualDepartureTimeLocal = u.ActualDepartureTimeLocal
FROM dbo.TA9_STANDARD_TRIPJACK01 t
JOIN (
    VALUES
    {values_block}
) AS u(Id, DelayInSecond, ScheduledArrivalTimeLocal, ActualArrivalTimeLocal, ScheduledDepartureTimeLocal, ActualDepartureTimeLocal)
ON t.Id = u.Id;"""
        full_statements.append(sql)

    output_content = "\n\n".join(full_statements)

    if output_file_path:
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write(output_content)
        print(
            f"✅ Generated {len(full_statements)} batch statement(s) ({len(df)} rows total) → {output_file_path}"
        )

    return full_statements


def main():
    csv_file = "/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/FILTER-3(API)/DELAYED/TRIPJACK_Delayed_Checked.csv"
    output_file = "/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/FILTER-3(API)/DELAYED/delay_update_script.txt"

    statements = generate_bulk_update_sql(csv_file, output_file)

    # Print preview of first batch
    if statements:
        print("\n=== Preview of First Batch ===\n")
        print(statements[0][:450] + " ...\n")  # Truncated for console readability


if __name__ == "__main__":
    main()
