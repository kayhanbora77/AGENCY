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

CSV_FILE_PATH = "/home/kayhan/Desktop/Gelen_Datalar/TRIPJACK/PROCEED/EXPORTS/TRIPJACK_TARGET_UPDATED_202603061631.csv"

BATCH_SIZE = 100_000

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
# COLUMN DETECTION
# ==================================================
def detect_flight_indices(columns: List[str]) -> List[int]:
    """
    Detect numeric indices for flight number columns.
    Supports: FlitNo1, FltNo1, FlightNo1, FlightNumber1 (case-insensitive).
    Returns sorted list of found indices e.g. [1, 2, 3, 4].
    """
    pattern = re.compile(r"^(?:Flit|Flt|Flight)(?:No|Number)?(\d+)$", re.IGNORECASE)
    indices = []
    for col in columns:
        m = pattern.match(col)
        if m:
            indices.append(int(m.group(1)))
    return sorted(set(indices))


def find_col(row, *candidates):
    """Return first non-empty value from a list of candidate attribute names."""
    for name in candidates:
        val = getattr(row, name, None)
        if val is not None and str(val).strip() not in ("", "nan", "None"):
            return val
    return None


# ==================================================
# IMPORTER
# ==================================================
class CSVToDBImporter:
    def __init__(self):
        self.con = duckdb.connect(str(DB_PATH))
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
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        if str(value).strip() in ("", "nan", "None", "NaT"):
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
    def extract_airline_code(flight_number: str) -> str:
        if not flight_number:
            return ""
        match = re.match(r"^([A-Z0-9]{2})", str(flight_number).upper())
        return match.group(1) if match else ""

    def determine_eligibility(self, legs: List[Dict[str, Any]]) -> bool:
        if not legs:
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
        if eu_departure is False:
            for leg in legs:
                dep_airport = leg["FromAirport"]
                arr_airport = leg["ToAirport"]
                if self.ref.is_eu_airport(dep_airport):
                    return True
                if (
                    self.ref.is_eu_airport(dep_airport) is False
                    and self.ref.is_eu_airport(arr_airport) is True
                    and (
                        self.ref.is_eu_carrier(leg["AirlineCode"])
                        or leg["AirlineCode"] in SPECIAL_NON_EU_CARRIERS
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

        # Print detected columns for debugging
        print(f"  Columns found: {list(df.columns)}")

        # Detect flight number indices dynamically
        flight_indices = detect_flight_indices(list(df.columns))
        print(f"  Flight indices detected: {flight_indices}")

        if not flight_indices:
            print("❌ No flight number columns detected. Check column names.")
            return

        # Build a map: index → actual column name in the dataframe
        flt_col_map = {}
        date_col_map = {}
        pattern_flt = re.compile(
            r"^(?:Flit|Flt|Flight)(?:No|Number)?(\d+)$", re.IGNORECASE
        )
        pattern_date = re.compile(r"^(?:Flt|Flight)?(?:Date|Dt)(\d+)$", re.IGNORECASE)

        for col in df.columns:
            m = pattern_flt.match(col)
            if m:
                flt_col_map[int(m.group(1))] = col
            m = pattern_date.match(col)
            if m:
                date_col_map[int(m.group(1))] = col

        print(f"  FlightNo col map : {flt_col_map}")
        print(f"  FlightDate col map: {date_col_map}")

        batch = []
        total = 0
        skipped = 0

        for row in df.itertuples(index=False):
            records = self._parse_row(
                row, flight_indices, flt_col_map, date_col_map, filename
            )
            batch.extend(records)

            if len(batch) >= BATCH_SIZE:
                self._insert(batch)
                total += len(batch)
                batch.clear()

        if batch:
            self._insert(batch)
            total += len(batch)

        print(f"✓ Total inserted: {total:,}  |  Skipped legs: {skipped:,}")

    # -----------------------------
    # Row Parsing
    # -----------------------------
    def _parse_row(self, row, flight_indices, flt_col_map, date_col_map, filename):
        legs = []

        for i in flight_indices:
            # Get flight number from the dynamically detected column
            flight_no = getattr(row, flt_col_map[i], None) if i in flt_col_map else None
            if not flight_no or str(flight_no).strip() in ("", "nan", "None"):
                continue

            # Get flight date from the dynamically detected date column
            raw_date = (
                getattr(row, date_col_map[i], None) if i in date_col_map else None
            )
            flight_date = self.parse_date(raw_date)
            if flight_date is None:
                continue

            # Airport columns are always Airport{i} and Airport{i+1}
            from_airport = getattr(row, f"Airport{i}", None)
            to_airport = getattr(row, f"Airport{i + 1}", None)

            if not from_airport or not to_airport:
                continue
            if str(from_airport).strip() in ("", "nan", "None"):
                continue
            if str(to_airport).strip() in ("", "nan", "None"):
                continue

            flight_number = str(flight_no).replace(" ", "").upper()
            airline = find_col(
                row, "IATA", "AirlineCode", "Airline"
            ) or self.extract_airline_code(flight_number)

            legs.append(
                {
                    "FlightNumber": flight_number,
                    "DepartureDate": flight_date,
                    "FromAirport": str(from_airport).upper().strip(),
                    "ToAirport": str(to_airport).upper().strip(),
                    "AirlineCode": str(airline).upper().strip() if airline else "",
                    "LegNo": i,
                }
            )

        if not legs:
            return []

        eligible = self.determine_eligibility(legs)
        connection_id = str(uuid.uuid4())
        final_airport = legs[-1]["ToAirport"]

        eticket = str(find_col(row, "TicketNo", "TicketNumber", "ETicketNo") or "")
        pax_name = str(find_col(row, "PaxName", "PassengerName", "Name") or "")
        booking_ref = find_col(row, "PNRNo", "PNR", "BookingRef")

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
                    "FileName": filename,
                    "BookingRef": str(booking_ref) if booking_ref else None,
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

        table_name = "TA_STANDARD_BLUESTAR"
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
            print(f"  ✓ Inserted {len(df):,} records")
        except Exception as e:
            print(f"  ❌ Insert error: {e}")
            print(f"     Sample record: {records[0]}")

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
            except Exception:
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
