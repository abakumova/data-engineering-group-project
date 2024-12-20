import os
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.operators.dummy import DummyOperator

# Paths
DATASETS = [
    {"name": "suicide_rates", "path": "/opt/airflow/dags/data/Suicide rates, age-standardized (per 100 000 population).csv"},
    {"name": "mental_illnesses", "path": "/opt/airflow/dags/data/1- mental-illnesses-prevalence.csv"},
    {"name": "beds_in_general_hospitals", "path": "/opt/airflow/dags/data/Beds for mental health in general hospitals (per 100,000).csv"},
    {"name": "beds_in_community", "path": "/opt/airflow/dags/data/Beds in community residential facilities (per 100,000).csv"},
    {"name": "beds_in_mental_hospitals", "path": "/opt/airflow/dags/data/Beds in mental hospitals (per 100,000).csv"},
    {"name": "nurses", "path": "/opt/airflow/dags/data/Nurses working in mental health sector (per 100 000).csv"},
    {"name": "psychiatrists", "path": "/opt/airflow/dags/data/Psychiatrists working in mental health sector (per 100,000).csv"},
    {"name": "psychologists", "path": "/opt/airflow/dags/data/Psychologists working in mental health sector (per 100,000).csv"},
    {"name": "gdp", "path": "/opt/airflow/dags/data/WITS-Country-GDP.csv"},
]
DUCKDB_PATH = "/opt/airflow/dags/data/analytics.duckdb"
DBT_PROJECT_DIR = "/usr/app/dbt_project"
DBT_PROFILES_DIR = "/root/.dbt"

# Functions for ETL
def extract_data(dataset, **kwargs):
    try:
        data = pd.read_csv(dataset["path"])
        print(f"[Extract] Extracted {len(data)} rows from {dataset['name']}")
        return data.to_json()
    except Exception as e:
        print(f"[Extract] Failed to extract data from {dataset['path']}: {e}")
        raise e

def transform_data(data_json, **kwargs):
    try:
        data = pd.read_json(data_json)
        data.columns = [col.lower().replace(" ", "_") for col in data.columns]
        print(f"[Transform] Transformed dataset with {len(data)} rows and columns: {list(data.columns)}")
        return data.to_json()
    except Exception as e:
        print(f"[Transform] Failed to transform data: {e}")
        raise e

def load_to_duckdb(data_json, dataset, **kwargs):
    try:
        data = pd.read_json(data_json)
        table_name = dataset["name"]

        with duckdb.connect(DUCKDB_PATH) as con:
            # Check existing tables
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"[DuckDB] Existing tables: {tables}")

            # Create table if it doesn't exist
            con.register("data", data)  # Register the dataframe explicitly
            if table_name not in [table[0] for table in tables]:
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data")
                print(f"[DuckDB] Created table: {table_name}")
            else:
                con.execute(f"INSERT INTO {table_name} SELECT * FROM data")
                print(f"[DuckDB] Inserted data into existing table: {table_name}")

            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[DuckDB] Table {table_name} now contains {row_count} rows")
    except Exception as e:
        print(f"[DuckDB] Failed to load data into table {table_name}: {e}")
        raise e


def create_unified_table(**kwargs):
    with duckdb.connect(DUCKDB_PATH) as con:
        try:
            # +) Attempt to create the table only if it doesn't exist
            con.execute("""
                CREATE TABLE IF NOT EXISTS analytics.unified_health_model AS 
                SELECT * FROM analytics_analytics.unified_health_model;
            """)
            print("[DuckDB] Unified table created or already exists.")

            # If the table exists, append data
            con.execute("""
                INSERT INTO analytics.unified_health_model
                SELECT * FROM analytics_analytics.unified_health_model;
            """)
            print("[DuckDB] Data appended to the unified_health_model table.")

            # Log row count
            row_count = con.execute("""
                SELECT COUNT(*)
                FROM analytics.unified_health_model;
            """).fetchone()[0]
            print(f"[DuckDB] Unified table now contains {row_count} rows.")

        except duckdb.CatalogException as e:
            print(f"[DuckDB] Catalog Exception: {e}")
            raise
        except Exception as e:
            print(f"[DuckDB] Failed to create or append to the unified table: {e}")
            raise



def export_to_iceberg(**kwargs):
    try:
        # Connect to DuckDB and load the httpfs extension
        conn = duckdb.connect(DUCKDB_PATH)
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")

        # Configure DuckDB for S3/MinIO access
        conn.sql("""
            SET s3_region='us-east-1';
            SET s3_url_style='path';
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_use_ssl=false;
        """)

        bucket_name = "warehouse"
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        s3_file_path = f"s3://{bucket_name}/unified_health_model_{timestamp}.parquet"

        # Export data directly to S3 in Parquet format
        conn.sql(f"""
            COPY (SELECT * FROM analytics.unified_health_model)
            TO '{s3_file_path}' (FORMAT 'parquet');
        """)
        print(f"[DuckDB] Exported unified data to MinIO bucket '{bucket_name}' at path '{s3_file_path}'")

        # Log data content before export
        query = "SELECT * FROM analytics.unified_health_model LIMIT 10;"
        result = conn.execute(query).fetchall()
        print("[DuckDB] Sample data being exported to Parquet:")
        for row in result:
            print(row)

    except Exception as e:
        print(f"[DuckDB] Failed to export data to Iceberg: {e}")
        raise e

    finally:
        # Ensure the connection is closed
        conn.close()


# Default Arguments
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="parallel_data_engineering_pipeline_with_dbt",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Task group to handle ETL for all datasets
    with TaskGroup("etl_tasks") as etl_tasks:
        for dataset in DATASETS:
            with TaskGroup(group_id=f"{dataset['name']}_group") as dataset_group:
                extract_task = PythonOperator(
                    task_id="extract",
                    python_callable=extract_data,
                    op_kwargs={"dataset": dataset},
                )

                transform_task = PythonOperator(
                    task_id="transform",
                    python_callable=transform_data,
                    op_kwargs={
                        "data_json": "{{ ti.xcom_pull(task_ids='etl_tasks." + dataset['name'] + "_group.extract') }}"
                    },
                )

                load_task = PythonOperator(
                    task_id="load",
                    python_callable=load_to_duckdb,
                    op_kwargs={
                        "data_json": "{{ ti.xcom_pull(task_ids='etl_tasks." + dataset['name'] + "_group.transform') }}",
                        "dataset": dataset,
                    },
                )

                extract_task >> transform_task >> load_task
            
            dataset_group  # Add the dataset's ETL group to the DAG

    # Dummy operator to fan-in all ETL tasks
    all_etl_done = DummyOperator(task_id="all_etl_done")

    # Downstream tasks
    run_dbt = DbtRunOperator(
        task_id="run_dbt_models",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )

    create_table_task = PythonOperator(
        task_id="create_unified_table",
        python_callable=create_unified_table,
    )

    iceberg_task = PythonOperator(
        task_id="export_to_iceberg",
        python_callable=export_to_iceberg,
    )

    # Dependencies
    etl_tasks >> all_etl_done >> run_dbt >> create_table_task >> iceberg_task
