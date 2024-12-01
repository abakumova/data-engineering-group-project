import os
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator

# Constants
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

# Functions
def extract_data(dataset, **kwargs):
    data = pd.read_csv(dataset["path"])
    print(f"Extracted {len(data)} rows from {dataset['name']}")
    return data.to_json()

def transform_data(data_json, **kwargs):
    data = pd.read_json(data_json)
    data.columns = [col.lower().replace(" ", "_") for col in data.columns]
    print(f"Transformed dataset with {len(data)} rows")
    return data.to_json()

def load_to_duckdb(data_json, dataset, **kwargs):
    data = pd.read_json(data_json)
    table_name = dataset["name"]

    with duckdb.connect(DUCKDB_PATH) as con:
        con.register("temp_table", data)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_table")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_table")
        print(f"Loaded data to DuckDB table: {table_name}")

def export_to_iceberg(**kwargs):
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("""
            COPY (SELECT * FROM unified_health_model) TO 's3://warehouse/unified_health_model.parquet'
            (FORMAT 'parquet', S3_REGION 'us-east-1', S3_ENDPOINT 'http://minio:9000',
            S3_ACCESS_KEY_ID 'minioadmin', S3_SECRET_ACCESS_KEY 'minioadmin');
        """)
        print("Exported unified data to Iceberg via MinIO")

# DAG Definition
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="process_all_datasets_parallel",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Parallel tasks for all datasets
    load_tasks = []
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

        # Set task dependencies for each dataset
        extract_task >> transform_task >> load_task

        # Collect all load tasks
        load_tasks.append(load_task)

    # dbt Task
    dbt_task = DbtRunOperator(
        task_id="run_dbt_models",
        project_dir="./dbt",
        profiles_dir="./dbt_profiles",
        do_xcom_push=False,
    )

    # Iceberg Export Task
    iceberg_task = PythonOperator(
        task_id="export_to_iceberg",
        python_callable=export_to_iceberg,
    )

    # Final dependencies
    load_tasks >> dbt_task >> iceberg_task
