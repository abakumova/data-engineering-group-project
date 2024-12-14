import streamlit as st
import duckdb
import pandas as pd

# Connect to the DuckDB database
conn = duckdb.connect("star_schema.db")

# Query for Suicide Rates vs. Mental Hospital Beds
query = """
SELECT 
    loc.LocationName AS Country,
    p.Year AS Year,
    f1.FactValueNumeric AS SuicideRates,
    f2.FactValueNumeric AS BedsInMentalHospitals
FROM Fact_HealthMetrics f1
JOIN Fact_HealthMetrics f2 ON f1.LocationKey = f2.LocationKey AND f1.PeriodKey = f2.PeriodKey
JOIN Dimension_Location loc ON f1.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON f1.PeriodKey = p.PeriodKey
JOIN Dimension_Indicator i1 ON f1.IndicatorKey = i1.IndicatorKey
JOIN Dimension_Indicator i2 ON f2.IndicatorKey = i2.IndicatorKey
WHERE i1.IndicatorCode = 'MH_12' -- Suicide Rates
  AND i2.IndicatorCode = 'MH_16' -- Beds in Mental Hospitals
  AND p.Year = 2017; -- Example: Filter for the year 2017
"""

# Load data into a DataFrame
df = pd.read_sql_query(query, conn)

# Streamlit app title and description
st.title("Suicide Rates vs. Mental Hospital Beds")
st.write("This visualization shows the relationship between suicide rates and the availability of mental hospital beds in different countries.")

# Display data in Streamlit
st.dataframe(df)

# Visualization using Streamlit
st.bar_chart(df.set_index('Country')[['SuicideRates', 'BedsInMentalHospitals']])

