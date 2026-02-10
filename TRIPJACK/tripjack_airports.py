import duckdb
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME


def separate_and_inserts(con: duckdb.DuckDBPyConnection) -> None:
    insert_sql = """
        INSERT INTO TRIPJACK_TARGET (
            BookingId,
            PaxName,
            JourneyBucket,
            BookingRef_PNR,
            Airline,
            ETicketNo,
            FlightNumber1,
            FlightNumber2,
            FlightNumber3,
            FlightNumber4,
            FlightNumber5,
            FlightDate1,
            FlightDate2,
            FlightDate3,
            FlightDate4,
            FlightDate5,
            Airport1,
            Airport2,
            Airport3,
            Airport4,
            Airport5,
            Airport6
        )
        SELECT
            BookingId,
            PaxName,
            'OUTBOUND',
            BookingRef_PNR,
            Airline,
            ETicketNo,
            FlightNumber1,
            NULL, NULL, NULL, NULL,
            FlightDate1,
            NULL, NULL, NULL, NULL,
            Airport1,
            Airport2,
            NULL, NULL, NULL, NULL
        FROM TRIPJACK_TARGET
        WHERE Airport1 = Airport3
          AND Airport4 IS NULL
          AND Airport5 IS NULL          

        UNION ALL

        SELECT
            BookingId,
            PaxName,
            'INBOUND',
            BookingRef_PNR,
            Airline,
            ETicketNo,
            FlightNumber2,
            NULL, NULL, NULL, NULL,
            FlightDate2,
            NULL, NULL, NULL, NULL,
            Airport2,
            Airport3,
            NULL, NULL, NULL, NULL
        FROM TRIPJACK_TARGET
        WHERE Airport1 = Airport3
          AND Airport4 IS NULL
          AND Airport5 IS NULL          
        """
    return insert_sql


def delete_full_rows(con: duckdb.DuckDBPyConnection) -> None:
    delete_sql = """
        DELETE FROM TRIPJACK_TARGET
        WHERE Airport1 = Airport3
          AND Airport4 IS NULL
          AND Airport5 IS NULL;             
        """
    return delete_sql


# ==================================================
# CONNECT
# ==================================================
def process_airports():
    con = duckdb.connect(DB_PATH)

    try:
        con.execute("BEGIN TRANSACTION;")

        con.execute(separate_and_inserts(con))
        con.execute(delete_full_rows(con))

        con.execute("COMMIT;")
        print("✅ Split completed and original FULL rows removed")

    except Exception as e:
        con.execute("ROLLBACK;")
        raise e

    finally:
        con.close()


if __name__ == "__main__":
    process_airports()
