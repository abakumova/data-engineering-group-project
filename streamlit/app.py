import streamlit as st
import duckdb
import pandas as pd
import geopandas as gpd

# Connect to DuckDB
DB_PATH = "/datasets/analytics.duckdb"
TABLE_NAME = "analytics.unified_health_model"  # The table in DuckDB

def get_duckdb_connection(db_path):
    """Establish a connection to DuckDB."""
    return duckdb.connect(db_path, read_only=True)

def query_table(conn, query):
    """Execute a query and return the result as a Pandas DataFrame."""
    return conn.execute(query).df()

def fetch_data_from_db(conn, table_name):
    """Fetch data from the DuckDB table."""
    query = f"SELECT country, gdp_in_usd, suicide_rate FROM {table_name} WHERE year=2017;"
    return conn.execute(query).df()

def load_geospatial_data(data):
    """Merge geospatial boundaries with the database data."""
    # Load country geometries from Natural Earth GeoJSON
    gdf = gpd.read_file("ne_110m_admin_0_countries.geojson")

    # Merge geometries with the database data
    merged_gdf = gdf.merge(data, how="left", left_on="name", right_on="country")
    return merged_gdf

def normalize_country_names(data):
    """Normalize country names in the DuckDB data to match GeoDataFrame."""
    country_mapping = {
        "United States": "United States of America",
        "Russia": "Russian Federation",
        "UK": "United Kingdom",
        # Add other mappings as needed
    }
    data["country"] = data["country"].replace(country_mapping)
    return data

# Streamlit App
def main():
    st.title("Unified Health Model Data Viewer (DuckDB)")

    # Connect to DuckDB
    conn = get_duckdb_connection(DB_PATH)

    # Query data overview
    query_overview = f"""
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT column_name) AS column_count
        FROM information_schema.columns
        WHERE table_name = '{TABLE_NAME.split('.')[-1]}';
    """
    overview = query_table(conn, query_overview)

    # Display dataset overview
    st.subheader("Dataset Overview")
    st.write(f"Rows: {overview['row_count'][0]}")
    st.write(f"Columns: {overview['column_count'][0]}")

    # Fetch the column names
    query_columns = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{TABLE_NAME.split('.')[-1]}';
    """
    columns = query_table(conn, query_columns)["column_name"].tolist()
    st.write("Columns in the dataset:", columns)

    # Filter by a column
    st.subheader("Filter Data")
    column_to_filter = st.selectbox("Select column to filter by:", columns)

    # Fetch unique values for the selected column
    query_unique_values = f"SELECT DISTINCT {column_to_filter} FROM {TABLE_NAME} ORDER BY {column_to_filter};"
    unique_values = query_table(conn, query_unique_values)[column_to_filter].tolist()
    selected_value = st.selectbox(f"Select value from '{column_to_filter}':", unique_values)

    # Apply filter
    query_filtered_data = f"""
        SELECT * 
        FROM {TABLE_NAME}
        WHERE {column_to_filter} = '{selected_value}';
    """
    filtered_data = query_table(conn, query_filtered_data)

    st.write(f"Filtered Data (where `{column_to_filter}` is `{selected_value}`):")
    st.dataframe(filtered_data)

    # Plot data
    st.subheader("Data Visualization")
    numeric_columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{TABLE_NAME.split('.')[-1]}' 
        AND data_type IN ('INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'REAL', 'NUMERIC');
    """
    numeric_columns = query_table(conn, numeric_columns_query)["column_name"].tolist()

    if numeric_columns:
        x_axis = st.selectbox("Select X-axis for the plot:", numeric_columns)
        y_axis = st.selectbox("Select Y-axis for the plot:", numeric_columns)

        # Query data for the selected columns
        query_plot_data = f"""
            SELECT {x_axis}, {y_axis}
            FROM {TABLE_NAME};
        """
        plot_data = query_table(conn, query_plot_data)

        # Plot data using Streamlit
        st.line_chart(plot_data.set_index(x_axis))
    else:
        st.write("No numeric columns available for visualization.")


    st.title("Unified Health Model - Geospatial Viewer")

    # Connect to DuckDB
    conn = get_duckdb_connection(DB_PATH)

    # Fetch data from the DuckDB table
    data = fetch_data_from_db(conn, TABLE_NAME)

    # Display the fetched data
    st.subheader("Fetched Data from Database")
    st.write(data)

    # Load and merge geospatial data
    gdf = load_geospatial_data(data)

    # Display merged geospatial data
    st.subheader("Geospatial Data")
    st.write(gdf)

    # Visualize the data
    st.subheader("Geospatial Visualization")
    st.map(gdf.set_geometry("geometry"))

# Run the app
if __name__ == "__main__":
    main()
