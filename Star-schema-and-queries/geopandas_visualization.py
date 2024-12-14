import geopandas as gpd
import pandas as pd
import duckdb
import matplotlib.pyplot as plt

# Connecting to the DuckDB database
conn = duckdb.connect("star_schema.db")

# Query to fetch suicide rates and beds in mental hospitals
query = """
SELECT 
    loc.LocationName AS Country,
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
  AND p.Year = 2017; -- Filter for the year 2017
"""

data = pd.read_sql_query(query, conn)

# Loading geographic data for countries
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Merging geographic data with the fact data
merged = world.merge(data, left_on="name", right_on="Country", how="left")

# Suicide rates on a world map
fig, ax = plt.subplots(1, 1, figsize=(15, 10))
merged.plot(column="SuicideRates", ax=ax, legend=True, cmap="OrRd", edgecolor="black")
plt.title("Suicide Rates per Country (2017)", fontsize=16)
plt.xlabel("")
plt.ylabel("")
plt.show()

# Beds in mental hospitals on a world map
fig, ax = plt.subplots(1, 1, figsize=(15, 10))
merged.plot(column="BedsInMentalHospitals", ax=ax, legend=True, cmap="Blues", edgecolor="black")
plt.title("Beds in Mental Hospitals per Country (2017)", fontsize=16)
plt.xlabel("")
plt.ylabel("")
plt.show()
