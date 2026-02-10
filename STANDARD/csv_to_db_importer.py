import duckdb
import pandas as pd
from datetime import datetime
import uuid
from typing import Dict, List, Any
from pathlib import Path
import os

# ==================================================
# DUCKDB CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"

DATABASE_DIR.mkdir(parents=True, exist_ok=True)


class CSVToDBImporter:
    """
    Import CSV flight data into DuckDB with intelligent flight-leg expansion.
    """

    def __init__(self):
        pass

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(DB_PATH)
        con.execute(f"SET threads={THREADS}")
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        con.execute(f"SET temp_directory='{TEMP_DIR}'")
        return con

    def parse_date(self, value):
        if pd.isna(value) or value == "":
            return None

        if isinstance(value, datetime):
            return value

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                pass

        return None

    def read_csv(self, csv_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(csv_path, sep="\t", encoding="utf-8")
            if len(df.columns) > 1:
                return df
        except Exception:
            pass

        return pd.read_csv(csv_path, sep=",", encoding="utf-8")

    def parse_flight_legs(
        self, row: pd.Series, connection_id: str
    ) -> List[Dict[str, Any]]:
        legs = []

        flight_no_cols = [c for c in row.index if c.startswith("FlightNo")]
        flight_no_cols.sort(key=lambda x: int(x.replace("FlightNo", "")))

        if not flight_no_cols:
            return legs

        for i, flight_no_col in enumerate(flight_no_cols, start=1):
            flight_no = row.get(flight_no_col)

            if pd.isna(flight_no) or str(flight_no).strip() == "":
                break

            flight_date = self.parse_date(row.get(f"FlightDate{i}"))
            from_airport = row.get(f"Airport{i}")
            to_airport = row.get(f"Airport{i + 1}")

            legs.append(
                {
                    "Id": str(uuid.uuid4()),
                    "ConnectionID": connection_id,
                    "PaxName": row.get("PaxName"),
                    "AgencyRefNumber": None,
                    "ETicketNo": str(row.get("TicketNumber"))
                    if pd.notna(row.get("TicketNumber"))
                    else None,
                    "FlightNumber": str(flight_no).replace(" ", "").upper(),
                    "DepartureDate": flight_date,
                    "FileName": None,
                    "BookingRef": row.get("PNR"),
                    "AirlineCode": row.get("IATA"),
                    "FromAirport": from_airport,
                    "ToAirport": to_airport,
                    "LastLegAirport": None,
                    "EUEligible": False,
                    "EUEligibleDuration": 0,
                    "ExtraNote": row.get("AirlineName"),
                    "FlightFound": True,
                }
            )

        if legs:
            final_airport = legs[-1]["ToAirport"]
            for leg in legs:
                leg["LastLegAirport"] = final_airport

        return legs

    def insert_records(self, records: List[Dict[str, Any]]):
        if not records:
            print("⚠️ No records to insert")
            return

        table_name = "TA_Standard_Template"

        columns = [
            "Id",
            "ConnectionID",
            "PaxName",
            "AgencyRefNumber",
            "ETicketNo",
            "FlightNumber",
            "DepartureDate",
            "FileName",
            "BookingRef",
            "AirlineCode",
            "FromAirport",
            "ToAirport",
            "LastLegAirport",
            "EUEligible",
            "EUEligibleDuration",
            "ExtraNote",
            "FlightFound",
        ]

        df = pd.DataFrame(records, columns=columns)

        con = self.get_connection()
        try:
            con.register("tmp_df", df)
            con.execute(f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                SELECT {", ".join(columns)}
                FROM tmp_df
            """)
            print(f"✅ Inserted {len(df)} records into {table_name}")
        finally:
            con.close()

    def import_csv(self, csv_path: str):
        print(f"📄 Reading CSV: {csv_path}")
        df = self.read_csv(csv_path)
        print(f"📊 Rows found: {len(df)}")

        file_name = os.path.basename(csv_path)
        connection_id = str(uuid.uuid4())

        all_records = []

        for _, row in df.iterrows():
            legs = self.parse_flight_legs(row, connection_id)
            for leg in legs:
                leg["FileName"] = file_name
            all_records.extend(legs)

        print(f"✈️ Flight legs parsed: {len(all_records)}")
        self.insert_records(all_records)


if __name__ == "__main__":
    importer = CSVToDBImporter()

    CSV_FILE = "/home/kayhan/Desktop/Gelen_Datalar/FLYCREATIVE/PROCEED/EXPORTS/FLYCREATIVE_TARGET.csv"

    try:
        importer.import_csv(CSV_FILE)
        print("🎉 Import completed successfully")
    except Exception as e:
        print(f"❌ Import failed: {e}")
