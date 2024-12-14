# Transforming the GDP data into long format before loading into the schema

import pandas as pd

# Load GDP data
gdp_df = pd.read_csv('WITS-Country-GDP.csv')

# Melt wide format to long format
gdp_long = gdp_df.melt(id_vars=['Country Name'], 
                       var_name='Year', 
                       value_name='GDP_USD')

# Clean and prepare columns for loading
gdp_long['Year'] = gdp_long['Year'].astype(int)
gdp_long.rename(columns={'Country Name': 'LocationName'}, inplace=True)

# Save the transformed data
gdp_long.to_csv('WITS-Country-GDP-Long.csv', index=False)
