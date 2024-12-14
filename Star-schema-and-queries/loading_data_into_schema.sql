import pandas as pd
import duckdb  # Replace with sqlite3 if you're using SQLite

# Connect to the DuckDB database (or SQLite)
con = duckdb.connect(database='star_schema.db')  # Change this to match your DB name
cur = con.cursor()

# Function to load CSV and insert into a table
def load_csv_to_table(csv_file, table_name):
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Insert data into the table
    con.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM read_csv_auto('{csv_file}');
    """)

# Load data into each table
load_csv_to_table('mental-illnesses-prevalence.csv', 'Fact_HealthMetrics')  # Example for a fact table
load_csv_to_table('Beds for mental health in general hospitals (per 100,000).csv', 'Fact_HealthMetrics')
load_csv_to_table('Beds in community residential facilities (per 100,000).csv', 'Fact_HealthMetrics')
load_csv_to_table('WITS-Country-GDP.csv', 'Dimension_GDP')
load_csv_to_table('Suicide rates, age-standardized (per 100 000 population).csv', 'Fact_HealthMetrics')

