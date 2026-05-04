import duckdb
from pathlib import Path

DB_PATH = Path.home() / "my_database" / "my_db.duckdb"

def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute("SET threads = 4")
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET temp_directory = '/tmp/duckdb_temp'")
    con.execute("SET max_temp_directory_size = '50GB'")  # Allow spilling to disk
    return con

def main():
    conn = connect_db()
    
    # Get table list
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
          AND table_name LIKE 'export_202%'
        ORDER BY table_name
    """).fetchall()
    
    table_names = [t[0] for t in tables]
    print(f"Found {len(table_names)} tables to merge")
    
    if not table_names:
        print("No tables found")
        return

    # Get schema from first table
    first_table = table_names[0]
    print(f"Creating PROTON table with schema from {first_table}...")
    
    # Create empty table (schema only)
    conn.execute(f'CREATE TABLE PROTON AS SELECT * FROM "{first_table}" WHERE 1=0')
    
    # Insert table by table (memory efficient)
    total_rows = 0
    for i, table in enumerate(table_names, 1):
        print(f"[{i}/{len(table_names)}] Inserting from {table}...")
        
        # Insert in batches if table is huge, or all at once if manageable
        conn.execute(f'INSERT INTO PROTON SELECT * FROM "{table}"')
        
        # Get row count for this table (optional, for progress tracking)
        count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        total_rows += count
        print(f"  → Added {count:,} rows (Total: {total_rows:,})")
        
        # Force checkpoint to free memory
        conn.execute("CHECKPOINT")

    print(f"\n✅ Success! Created 'PROTON' with {total_rows:,} total rows")
    conn.close()

if __name__ == "__main__":
    main()