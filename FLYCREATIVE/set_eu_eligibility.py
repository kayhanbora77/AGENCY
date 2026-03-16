import duckdb
import pandas as pd
from datetime import datetime
import uuid
from typing import List, Dict, Any, Optional
from pathlib import Path
import os
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"

CSV_FILE_PATH = "/home/kayhan/Desktop/Gelen_Datalar/FLYCREATIVE/PROCEED/EXPORTS/FLYCREATIVE_TARGET_202603111557.csv"

BATCH_SIZE = 50_000

SPECIAL_NON_EU_CARRIERS = {"BA", "TK", "PC", "JU", "H2", "FH", "VF", "VS"}

DATABASE_DIR.mkdir(parents=True, exist_ok=True)
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)


# ==================================================
# REFERENCE DATA
# ==================================================
class ReferenceData:
    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports = self._load_airports(con)
        self.eu_carriers = self._load_carriers(con)

    def _load_airports(self, con):
        rows = con.execute("SELECT CodeIataAirport FROM AIRPORTS").fetchall()
        return {r[0].strip().upper() for r in rows if r[0]}

    def _load_carriers(self, con):
        rows = con.execute(
            "SELECT ICAOCode FROM AIRLINES WHERE IsInUnion = 1"
        ).fetchall()
        return {r[0].strip().upper() for r in rows if r[0]}

    def is_eu_airport(self, code: str) -> bool:
        return (code or "").upper() in self.eu_airports

    def is_eu_carrier(self, code: str) -> bool:
        return (code or "").upper() in self.eu_carriers


# ==================================================
# IMPORTER
# ==================================================
class CSVToDBImporter:
    def __init__(self):
        self.con = duckdb.connect(DB_PATH)
        self.con.execute(f"SET threads={THREADS}")
        self.con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        self.con.execute("SET preserve_insertion_order=false")
        self.con.execute("SET enable_progress_bar=false")
        self.con.execute(f"SET temp_directory='{TEMP_DIR}'")

        self.ref = ReferenceData(self.con)

    # -----------------------------
    # Utilities
    # -----------------------------
    @staticmethod
    def parse_date(value: Any) -> Optional[datetime]:
        if pd.isna(value) or value == "":
            return None

        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None

        return dt.to_pydatetime()

    @staticmethod
    def extract_airline_code(flight_number: str) -> str:
        if not flight_number:
            print("no flight number")
            return ""
        match = re.match(r"^([A-Z0-9]{2})", str(flight_number).upper())
        return match.group(1) if match else ""

    def determine_eligibility(self, legs: List[Dict[str, Any]]) -> bool:
        if not legs:
            print("no legs")
            return False

        leg_dep_airport = legs[0]["FromAirport"]
        leg_arr_airport = legs[-1]["ToAirport"]
        eu_departure = self.ref.is_eu_airport(leg_dep_airport)
        eu_arrival = self.ref.is_eu_airport(leg_arr_airport)

        if eu_departure is True:
            return True
        if eu_departure is False and eu_arrival is False:
            for leg in legs:
                if leg["AirlineCode"] in SPECIAL_NON_EU_CARRIERS:
                    return True
            return False
        if eu_departure is False:  # and eu_arrival is True:
            for leg in legs:
                dep_airport = leg["FromAirport"]
                arr_airport = leg["ToAirport"]
                if self.ref.is_eu_airport(dep_airport):
                    return True
                if (
                    self.ref.is_eu_airport(dep_airport) is False
                    and self.ref.is_eu_airport(arr_airport) is True
                    and (
                        self.ref.is_eu_carrier(
                            leg["AirlineCode"]
                            or leg["AirlineCode"] in SPECIAL_NON_EU_CARRIERS
                        )
                    )
                ):
                    return True
                if leg["AirlineCode"] in SPECIAL_NON_EU_CARRIERS:
                    return True
            return False

    # -----------------------------
    # CSV Processing
    # -----------------------------
    def import_csv(self, path: str):

        df = self._read_csv(path)
        filename = os.path.basename(path)

        flight_cols = [
            c
            for c in df.columns
            if re.match(r"^Flight(?:No|Number)\d+$", c, re.IGNORECASE)
        ]

        batch = []
        total = 0

        for row in df.itertuples(index=False):
            legs = self._parse_row(row, flight_cols)

            for leg in legs:
                leg["FileName"] = filename

            batch.extend(legs)

            if len(batch) >= BATCH_SIZE:
                self._insert(batch)
                total += len(batch)
                batch.clear()

        if batch:
            self._insert(batch)
            total += len(batch)

        print(f"✓ Total inserted: {total:,}")

    # -----------------------------
    # Row Parsing
    # -----------------------------
    def _parse_row(self, row, flight_cols):

        legs = []

        for i, col in enumerate(flight_cols, 1):
            flight_no = getattr(row, col, None)
            if not flight_no:
                continue

            flight_date = self.parse_date(
                getattr(row, f"FlightDate{i}", None)
                or getattr(row, f"DepartureDateLocal{i}", None)
                or getattr(row, f"DepartureDate{i}", None)
            )
            if flight_date is None:
                continue
            from_airport = getattr(row, f"Airport{i}", None)
            to_airport = getattr(row, f"Airport{i + 1}", None)

            if not from_airport or not to_airport:
                print("No from or to airport found for row")
                continue

            flight_number = str(flight_no).replace(" ", "").upper()
            airline = getattr(row, "IATA", None) or self.extract_airline_code(
                flight_number
            )

            legs.append(
                {
                    "FlightNumber": flight_number,
                    "DepartureDate": flight_date,
                    "FromAirport": str(from_airport).upper().strip(),
                    "ToAirport": str(to_airport).upper().strip(),
                    "AirlineCode": airline,
                    "LegNo": i,
                }
            )

        if not legs:
            return []

        eligible = self.determine_eligibility(legs)
        connection_id = str(uuid.uuid4())
        final_airport = legs[-1]["ToAirport"]
        eticket = str(
            getattr(row, "TicketNumber", None) or getattr(row, "ETicketNo", None) or ""
        )
        pax_name = str(
            getattr(row, "PaxName", None)
            or getattr(row, "PassengerName", None)
            or getattr(row, "Name", None)
            or ""
        )
        booking_ref = (
            getattr(row, "PNR", None)
            or getattr(row, "PNRNo", None)
            or getattr(row, "BookingRef", None)
        )
        records = []
        for leg in legs:
            records.append(
                {
                    "Id": str(uuid.uuid4()),
                    "ConnectionID": connection_id,
                    "PaxName": pax_name,
                    "AgencyRefNumber": None,
                    "ETicketNo": eticket,
                    "FlightNumber": leg["FlightNumber"],
                    "DepartureDate": leg["DepartureDate"],
                    "FileName": None,  # filled later
                    "BookingRef": booking_ref,
                    "AirlineCode": leg["AirlineCode"],
                    "FromAirport": leg["FromAirport"],
                    "ToAirport": leg["ToAirport"],
                    "LastLegAirport": final_airport,
                    "EUEligible": eligible,
                    "EUEligibleDuration": 0,
                    "ExtraNote": None,
                    "FlightFound": False,
                    "LegNo": leg["LegNo"],
                }
            )

        return records

    # -----------------------------
    # Insert
    # -----------------------------
    def _insert(self, records):

        if not records:
            print("⚠️  No records to insert")
            return

        table_name = "TA_STANDARD_FLYCREATIVE"
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
            "LegNo",
        ]

        df = pd.DataFrame(records, columns=columns)

        try:
            self.con.register("tmp_df", df)
            self.con.execute(f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                SELECT {", ".join(columns)} FROM tmp_df
            """)
            print(f"✓ Inserted {len(df)} records")
        except Exception as e:
            print(f"Insert error: {e}")

    # -----------------------------
    # CSV Reader
    # -----------------------------
    @staticmethod
    def _read_csv(path):

        for sep in ["\t", ",", ";"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False)
                if len(df.columns) > 1:
                    return df
            except:
                continue

        raise ValueError("Could not detect CSV separator")


# ==================================================
# MAIN
# ==================================================
def main():
    importer = CSVToDBImporter()
    try:
        importer.import_csv(CSV_FILE_PATH)
    finally:
        importer.con.close()


if __name__ == "__main__":
    main()
