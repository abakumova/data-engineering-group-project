import duckdb

# Path to the Parquet file
parquet_file = r"llm\unified_health_model_20241208170934.parquet"

# Connect to DuckDB
conn = duckdb.connect()

# Load the Parquet file into a virtual view
try:
    conn.execute(f"CREATE VIEW data_view AS SELECT * FROM read_parquet('{parquet_file}');")
    print("Parquet file loaded successfully.")
except duckdb.IOException as e:
    print(f"Error loading Parquet file: {e}")
    exit()

# Test SQL Queries
try:
    # Example query: Check schema
    schema = conn.execute("DESCRIBE data_view").fetchdf()
    print("\nSchema:")
    print(schema)

    # Example query: Find the country with the biggest GDP in 2018
    query = """
    SELECT country, gdp_in_usd
FROM data_view
WHERE year = 2018
ORDER BY gdp_in_usd DESC
LIMIT 1;

    """
    result = conn.execute(query).fetchdf()
    print("\nQuery Result:")
    print(result)
except Exception as e:
    print(f"Error executing query: {e}")
