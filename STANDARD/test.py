import pyodbc

SERVER = "c2rflightdata.eastus2.cloudapp.azure.com"
DATABASE = "C2RFlightDataUser"
USERNAME = "sa"
PASSWORD = "[PASSWORD]"

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
