import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# Load geographic data for countries
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Load fact data (replace with your database connection and query if needed)
data = pd.DataFrame({
    "Country": ["Afghanistan", "United Kingdom", "Venezuela"],
    "SuicideRates": [0.32, 0.49, 0.27],
    "BedsInMentalHospitals": [0.703, 11.09, 0.53]
})

# Merge geographic data with fact data
merged = world.merge(data, left_on="name", right_on="Country")

# Plot suicide rates on a map
fig, ax = plt.subplots(1, 1, figsize=(15, 10))
merged.plot(column="SuicideRates", ax=ax, legend=True, cmap="OrRd", edgecolor="black")
plt.title("Suicide Rates per Country")
plt.show()
