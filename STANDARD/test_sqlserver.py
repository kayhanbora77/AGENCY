import pyodbc

SERVER = "172.210.241.56"
DATABASE = "FlightDelay"
USERNAME = "C2RFlightDataUser"
PASSWORD = "C2RFlightDataServerP0ss!"

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT @@VERSION")
row = cursor.fetchone()
print(row)

conn.close()
