from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import sys
import random

DATA_PATH = "/opt/airflow/dags/data.csv"
EXPECTED_HEADERS = ["name", "age", "city"]

def generate():
    sys.path.insert(0, '/opt/airflow/dags')
    from chaos_generator import generate_data
    chaos = random.choice([True, False])
    generate_data(chaos=chaos)

def ingest():
    with open(DATA_PATH, "r") as f:
        reader = csv.reader(f)
        headers = next(reader)

        if headers != EXPECTED_HEADERS:
            raise ValueError(f"Schema drift detected! Got: {headers}")

        for row in reader:
            print(f"Ingested: {dict(zip(headers, row))}")

with DAG(
    dag_id="ingest_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest
    )

    generate_task >> ingest_task