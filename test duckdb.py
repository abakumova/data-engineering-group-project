import duckdb

# Path to the DuckDB database file
db_path = "datasets/analytics.duckdb"

# Connect to the DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

try:
    # Fetch all table names
    tables = conn.execute("SHOW TABLES;").fetchall()
    print("Available tables before deletion:", tables)

    # Delete all data from each table
    for table in tables:
        table_name = table[0]  # Extract the table name from the tuple
        conn.execute(f"DELETE FROM {table_name}")
        print(f"Deleted all data from table '{table_name}'.")

    # Verify that tables are now empty
    for table in tables:
        table_name = table[0]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Table '{table_name}' now has {row_count} rows.")

    for table in tables:
        table_name = table[0]  # Extract the table name from the tuple
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Drop all table '{table_name}'.")

finally:
    # Close the connection
    conn.close()