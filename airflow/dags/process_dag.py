import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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

def extract_data(dataset, **kwargs):
    data = pd.read_csv(dataset["path"])
    print(f"Extracted {len(data)} rows from {dataset['name']}")
    return data.to_json()

def transform_data(data_json, **kwargs):
    data = pd.read_json(data_json)
    data.columns = [col.lower().replace(" ", "_") for col in data.columns]
    print(f"Transformed dataset with {len(data)} rows")
    return data.to_json()

def load_data(data_json, dataset, **kwargs):
    data = pd.read_json(data_json)
    output_dir = "/opt/airflow/dags/output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{dataset['name']}_processed.csv")
    data.to_csv(output_path, index=False)
    print(f"Loaded data to {output_path}")

# Default arguments for the DAG
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="process_all_datasets",
    default_args=default_args,
    schedule_interval=None,  # Manually trigger
    catchup=False,
) as dag:

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
            task_id=f"load_{dataset['name']}",
            python_callable=load_data,
            op_kwargs={
                "data_json": "{{ ti.xcom_pull(task_ids='" + f"transform_{dataset['name']}" + "') }}",
                "dataset": dataset,
            },
        )

        extract_task >> transform_task >> load_task
