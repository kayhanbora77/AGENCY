import duckdb
import pandas as pd
from datetime import datetime
import uuid
from typing import Dict, List, Any, Optional
from pathlib import Path
import os
import re

# ==================================================
# CONFIG & CONSTANTS
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"

DATABASE_DIR.mkdir(parents=True, exist_ok=True)

SPECIAL_NON_EU_CARRIERS = {
    "BA",  # British Airways
    "TK",  # Turkish Airlines
    "PC",  # Pegasus
    "JU",  # AirSerbian
    "H2",  # Sky Airline
    "FH",  # Freebird Airlines
    "VF",  # AJet(Anadolu jet)
}

# Global cached sets (faster lookup)
EU_AIRPORTS: set = set()
EU_CARRIERS: set = set()


def load_airports(force_refresh=False) -> set:
    global EU_AIRPORTS
    if EU_AIRPORTS and not force_refresh:
        return EU_AIRPORTS

    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            result = con.execute("SELECT CodeIataAirport FROM AIRPORTS").fetchall()
            EU_AIRPORTS = {r[0].strip().upper() for r in result if r[0]}
    except Exception as e:
        print(f"Error loading airports: {e}")
        EU_AIRPORTS = set()

    return EU_AIRPORTS


def load_eu_carriers(force_refresh=False) -> set:
    global EU_CARRIERS
    if EU_CARRIERS and not force_refresh:
        return EU_CARRIERS

    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            result = con.execute(
                "SELECT ICAOCode FROM AIRLINES WHERE IsInUnion = 1"
            ).fetchall()
            EU_CARRIERS = {r[0].strip().upper() for r in result if r[0]}
    except Exception as e:
        print(f"Error loading EU carriers: {e}")
        EU_CARRIERS = set()

    return EU_CARRIERS


def is_eu_airport(airport: Any) -> bool:
    if airport is None or pd.isna(airport):
        return False
    code = str(airport).upper().strip()
    return code in EU_AIRPORTS


def is_eu_carrier(airline: Any) -> bool:
    if airline is None or pd.isna(airline):
        return False
    code = str(airline).upper().strip()
    return code in EU_CARRIERS


def extract_airline_from_flight_number(flight_number: str) -> str:
    """
    Extract IATA airline code (letters + digits).
    Examples:
      G31767 -> G3
      U21234 -> U2
      4U123  -> 4U
    """
    if not flight_number:
        return ""

    match = re.match(r"^([A-Z0-9]{2})", str(flight_number).upper())
    return match.group(1) if match else ""


class CSVToDBImporter:
    """Import CSV flight data → DuckDB with EU eligibility rules"""

    def __init__(self):
        # Preload reference data
        load_airports()
        load_eu_carriers()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(DB_PATH)
        con.execute(f"SET threads={THREADS}")
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        con.execute(f"SET temp_directory='{TEMP_DIR}'")
        return con

    def parse_date(self, value: Any) -> Optional[datetime]:
        if pd.isna(value) or value == "":
            return None
        if isinstance(value, datetime):
            return value
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%d.%m.%Y",
        ):
            try:
                return datetime.strptime(str(value).strip(), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def read_csv(csv_path: str) -> pd.DataFrame:
        for sep in ["\t", ",", ";"]:
            try:
                df = pd.read_csv(csv_path, sep=sep, encoding="utf-8", low_memory=False)
                if len(df.columns) > 1:
                    return df
            except:
                continue
        raise ValueError(f"Could not read CSV file with common separators: {csv_path}")

    def determine_eligibility(self, temp_legs: List[Dict[str, Any]]) -> bool:
        if not temp_legs:
            return False

        dep_eu = is_eu_airport(temp_legs[0]["FromAirport"])
        arr_eu = is_eu_airport(temp_legs[-1]["ToAirport"])

        # Rule 1: Departure from EU → always eligible
        if dep_eu:
            return True

        # Departure = NON-EU
        if arr_eu:
            # Rule 2: NON-EU → EU : at least one EU carrier in the itinerary
            return any(is_eu_carrier(leg["AirlineCode"]) for leg in temp_legs)

        # Rule 3: NON-EU → NON-EU : only if at least one special carrier
        return any(leg["AirlineCode"] in SPECIAL_NON_EU_CARRIERS for leg in temp_legs)

    def parse_flight_legs(self, row: pd.Series) -> List[Dict[str, Any]]:

        flight_no_cols = [
            c
            for c in row.index
            if re.match(r"^Flight(?:No|Number)(\d+)$", str(c), re.IGNORECASE)
        ]
        # flight_no_cols.sort(key=lambda c: int(re.search(r"\d+$", c).group()))

        if not flight_no_cols:
            return []

        temp_legs = []

        for i, flight_col in enumerate(flight_no_cols, 1):
            flight_no = row.get(flight_col)
            if pd.isna(flight_no) or str(flight_no).strip() == "":
                break

            flight_date = self.parse_date(
                row.get(f"FlightDate{i}")
                or row.get(f"DepartureDateLocal{i}")
                or row.get(f"DepartureDate{i}")
            )

            from_airport = row.get(f"Airport{i}")
            to_airport = row.get(f"Airport{i + 1}")

            if not from_airport or not to_airport:
                continue

            flight_number_clean = str(flight_no).replace(" ", "").upper()

            # Try to get airline from specific column, otherwise extract from flight number
            airline_code = str(row.get("IATA") or "").strip().upper()
            if not airline_code:
                airline_code = extract_airline_from_flight_number(flight_number_clean)

            temp_legs.append(
                {
                    "FlightNumber": flight_number_clean,
                    "DepartureDate": flight_date,
                    "FromAirport": str(from_airport).upper().strip(),
                    "ToAirport": str(to_airport).upper().strip(),
                    "AirlineCode": airline_code,
                    "LegNo": i,
                }
            )

        if not temp_legs:
            return []

        eligible = self.determine_eligibility(temp_legs)

        connection_id = str(uuid.uuid4())
        final_airport = temp_legs[-1]["ToAirport"]
        eticket = str(row.get("TicketNumber") or row.get("ETicketNo") or "")
        pax_name = str(
            row.get("PaxName") or row.get("PassengerName") or row.get("Name") or ""
        )
        final_legs = []

        for leg in temp_legs:
            record = {
                "Id": str(uuid.uuid4()),
                "ConnectionID": connection_id,
                "PaxName": pax_name,
                "AgencyRefNumber": None,
                "ETicketNo": eticket,
                "FlightNumber": leg["FlightNumber"],
                "DepartureDate": leg["DepartureDate"],
                "FileName": None,  # filled later
                "BookingRef": row.get("PNR"),
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
            final_legs.append(record)

        return final_legs

    def insert_records(self, records: List[Dict[str, Any]]):
        if not records:
            print("⚠️  No records to insert")
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
            "LegNo",
        ]

        df = pd.DataFrame(records, columns=columns)

        con = self.get_connection()
        try:
            con.register("tmp_df", df)
            con.execute(f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                SELECT {", ".join(columns)} FROM tmp_df
            """)
            print(f"✓ Inserted {len(df)} records")
        except Exception as e:
            print(f"Insert error: {e}")
        finally:
            con.close()

    def import_csv(self, csv_path: str):
        print(f"Reading: {csv_path}")
        df = self.read_csv(csv_path)
        print(f"Rows: {len(df):,}")

        filename = os.path.basename(csv_path)
        all_records = []

        for _, row in df.iterrows():
            legs = self.parse_flight_legs(row)
            for leg in legs:
                leg["FileName"] = filename
            all_records.extend(legs)

        print(f"Flight legs parsed: {len(all_records):,}")
        self.insert_records(all_records)


if __name__ == "__main__":
    importer = CSVToDBImporter()

    CSV_FILE = "/home/kayhan/Desktop/Gelen_Datalar/TRUST_TRAVEL/PROCEED/TRUST_TRAVEL_TARGET_4.csv"

    try:
        importer.import_csv(CSV_FILE)
        print("✓ Import completed.")
    except Exception as e:
        print(f"❌ Import failed: {e}")
