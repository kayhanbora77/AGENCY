import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=172.210.241.56;"
    "DATABASE=your_database;"
    "UID=C2RFlightDataUser;"
    "PWD=C2RFlightDataServerP0ss!;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
print(cursor.fetchone())
