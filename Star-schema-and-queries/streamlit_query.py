import streamlit as st
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("health_metrics.db")

# Example Query: Suicide Rates vs. Mental Hospital Beds
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
WHERE i1.IndicatorCode = 'MH_12' AND i2.IndicatorCode = 'MH_16';
"""

# Load data into a DataFrame
df = pd.read_sql_query(query, conn)

# Display data in Streamlit
st.title("Suicide Rates vs. Mental Hospital Beds")
st.write("This visualization shows the relationship between suicide rates and the availability of mental hospital beds.")
st.dataframe(df)

# Create visualization
st.bar_chart(df.set_index('Country'))
