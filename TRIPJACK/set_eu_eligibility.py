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
        rows = con.execute(
            "SELECT CodeIataAirport FROM AIRPORTS WHERE CodeIso2Country NOT IN ('TR','MA')"
        ).fetchall()
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
    Supports: FltNo1, FlightNo1, FlightNumber1 (case-insensitive).
    Returns sorted list of found indices e.g. [1, 2, 3, 4].
    """
    pattern = re.compile(r"^(?:Flt|Flight)(?:No|Number)?(\d+)$", re.IGNORECASE)
    indices = []
    for col in columns:
        m = pattern.match(col)
        if m:
            indices.append(int(m.group(1)))
    return sorted(set(indices))


def find_col(row, *candidates):
    """Return first non-empty value from a list of candidate attribute names."""
    for name in candidates:
        if hasattr(row, name):
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

        # Debug counters
        self.debug_counts = {
            "total_rows": 0,
            "no_flight_number": 0,
            "invalid_date": 0,
            "missing_airport": 0,
            "successful_legs": 0,
        }

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
        print(f"  Total rows in CSV: {len(df):,}")

        # Detect flight number indices dynamically
        flight_indices = detect_flight_indices(list(df.columns))
        print(f"  Flight indices detected: {flight_indices}")

        if not flight_indices:
            print("❌ No flight number columns detected. Check column names.")
            return

        # Build a map: index → actual column name in the dataframe
        flt_col_map = {}
        date_col_map = {}
        pattern_flt = re.compile(r"^(?:Flt|Flight)(?:No|Number)?(\d+)$", re.IGNORECASE)
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

        # Sample first few rows for debugging
        print("\n🔍 DEBUG: First 3 rows sample:")
        sample_rows = []
        for idx, row in enumerate(df.itertuples(index=False)):
            if idx < 3:
                sample_rows.append(row)
                print(f"\n  Row {idx + 1}:")
                print(f"    BookingId: {getattr(row, 'BookingId', 'N/A')}")
                print(f"    PaxName: {getattr(row, 'PaxName', 'N/A')}")
                print(f"    FlightNumber1: {getattr(row, 'FlightNumber1', 'N/A')}")
                print(f"    FlightDate1: {getattr(row, 'FlightDate1', 'N/A')}")
                print(f"    Airport1: {getattr(row, 'Airport1', 'N/A')}")
                print(f"    Airport2: {getattr(row, 'Airport2', 'N/A')}")

        print("\n📊 Processing all rows:")

        for row_idx, row in enumerate(df.itertuples(index=False)):
            self.debug_counts["total_rows"] += 1

            if row_idx < 5:  # Show detailed debug for first 5 rows
                print(f"\n  Row {row_idx + 1} detailed breakdown:")

            records, row_skipped = self._parse_row_debug(
                row, flight_indices, flt_col_map, date_col_map, filename, row_idx < 5
            )

            if records:
                batch.extend(records)
                if row_idx < 5:
                    print(f"    ✅ Generated {len(records)} leg records")
            else:
                skipped += 1
                if row_idx < 5:
                    print("    ❌ No legs generated for this row")

            if len(batch) >= BATCH_SIZE:
                self._insert(batch)
                total += len(batch)
                batch.clear()
                print(
                    f"  Progress: {row_idx + 1:,} rows processed, {total:,} legs inserted"
                )

        if batch:
            self._insert(batch)
            total += len(batch)

        print("\n" + "=" * 60)
        print("📊 DEBUG STATISTICS:")
        print("=" * 60)
        print(f"Total rows processed: {self.debug_counts['total_rows']:,}")
        print(f"Rows with no flight number: {self.debug_counts['no_flight_number']:,}")
        print(f"Rows with invalid date: {self.debug_counts['invalid_date']:,}")
        print(f"Rows with missing airports: {self.debug_counts['missing_airport']:,}")
        print(f"Successful leg records: {self.debug_counts['successful_legs']:,}")
        print("=" * 60)
        print(
            f"✓ Total inserted: {total:,} leg records  |  Rows with no legs: {skipped:,}"
        )

    # -----------------------------
    # Row Parsing with Debug
    # -----------------------------
    def _parse_row_debug(
        self, row, flight_indices, flt_col_map, date_col_map, filename, debug=False
    ):
        legs = []
        row_has_leg = False

        for i in flight_indices:
            # Get flight number from the dynamically detected column
            flight_no = getattr(row, flt_col_map[i], None) if i in flt_col_map else None

            if debug:
                print(f"    Leg {i}: FlightNo={flight_no}")

            if not flight_no or str(flight_no).strip() in ("", "nan", "None"):
                if debug:
                    print("      ❌ No flight number")
                self.debug_counts["no_flight_number"] += 1
                continue

            # Get flight date from the dynamically detected date column
            raw_date = (
                getattr(row, date_col_map[i], None) if i in date_col_map else None
            )

            if debug:
                print(f"      Raw date: {raw_date}")

            flight_date = self.parse_date(raw_date)
            if flight_date is None:
                if debug:
                    print("      ❌ Invalid date")
                self.debug_counts["invalid_date"] += 1
                continue

            # Airport columns are always Airport{i} and Airport{i+1}
            from_airport = getattr(row, f"Airport{i}", None)
            to_airport = getattr(row, f"Airport{i + 1}", None)

            if debug:
                print(f"      From: {from_airport}, To: {to_airport}")

            if not from_airport or not to_airport:
                if debug:
                    print("      ❌ Missing airport(s)")
                self.debug_counts["missing_airport"] += 1
                continue

            if str(from_airport).strip() in ("", "nan", "None"):
                if debug:
                    print("      ❌ From airport empty")
                self.debug_counts["missing_airport"] += 1
                continue

            if str(to_airport).strip() in ("", "nan", "None"):
                if debug:
                    print("      ❌ To airport empty")
                self.debug_counts["missing_airport"] += 1
                continue

            flight_number = str(flight_no).replace(" ", "").upper()

            # Try to find airline code from various columns
            airline = find_col(row, "IATA", "AirlineCode", "Airline")

            if not airline:
                airline = self.extract_airline_code(flight_number)

            if debug:
                print(f"      Airline: {airline}")

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

            row_has_leg = True
            self.debug_counts["successful_legs"] += 1

        if not row_has_leg:
            return [], True

        eligible = self.determine_eligibility(legs)
        connection_id = str(uuid.uuid4())
        final_airport = legs[-1]["ToAirport"]

        eticket = str(find_col(row, "TicketNo", "ETicketNo") or "")
        pax_name = str(find_col(row, "PaxName", "PassengerName", "Name") or "")
        booking_ref = find_col(row, "PNRNo", "BookingRef_PNR", "BookingRef")

        if debug:
            print(
                f"    Connection details: Pax={pax_name}, Booking={booking_ref}, Eligible={eligible}"
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

        return records, False

    # -----------------------------
    # Insert
    # -----------------------------
    def _insert(self, records):
        if not records:
            print("⚠️  No records to insert")
            return

        table_name = "TA_STANDARD_TRIPJACK"
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
            if records:
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
