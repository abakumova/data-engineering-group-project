import os
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator

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


# Default Arguments
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
with DAG(
        dag_id="data_engineering_pipeline_with_dbt",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    previous_task = None  # To chain tasks sequentially

    for dataset in DATASETS:
        extract_task = PythonOperator(
            task_id=f"extract_{dataset['name']}",
            python_callable=extract_data,
            op_kwargs={"dataset": dataset},
        )

        transform_task = PythonOperator(
            task_id=f"transform_{dataset['name']}",
            python_callable=transform_data,
            op_kwargs={"data_json": "{{ ti.xcom_pull(task_ids='" + f"extract_{dataset['name']}" + "') }}"},
        )

        load_task = PythonOperator(
            task_id=f"load_to_duckdb_{dataset['name']}",
            python_callable=load_to_duckdb,
            op_kwargs={
                "data_json": "{{ ti.xcom_pull(task_ids='" + f"transform_{dataset['name']}" + "') }}",
                "dataset": dataset,
            },
        )

        # Chain tasks sequentially
        if previous_task:
            previous_task >> extract_task

        extract_task >> transform_task >> load_task
        previous_task = load_task

    # dbt task to run models
    run_dbt = DbtRunOperator(
        task_id="run_dbt_models",
        project_dir="/usr/app/dbt_project",
        profiles_dir="/opt/airflow/dbt_profiles",
        dag=dag,
    )

    # Ensure dbt runs after all loading tasks
    previous_task >> run_dbt
