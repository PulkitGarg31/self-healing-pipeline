from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import psycopg2

DATA_PATH = "/opt/airflow/dags/data.csv"
EXPECTED_HEADERS = ["name", "age", "city"]

DB_CONN = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def generate():
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    from chaos_generator import generate_data
    import random
    chaos = random.choice([True, False])
    generate_data(chaos=chaos)

def validate():
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    from healer import heal

    def check_file():
        with open(DATA_PATH, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)

            if headers != EXPECTED_HEADERS:
                return False, headers, []

            rows = list(reader)
            for row in rows:
                try:
                    age = int(row[1])
                except ValueError:
                    raise ValueError(f"Age must be a number, got: {row[1]}")
                if age < 0 or age > 120:
                    raise ValueError(f"Invalid age: {age}")
                if not row[0]:
                    raise ValueError("Name cannot be empty")
            return True, headers, rows

    valid, headers, rows = check_file()

    if not valid:
        error = f"Schema drift detected! Got: {headers}"
        print(error)
        fixed = heal(error, DATA_PATH)

        if fixed:
            print("Pipeline healed! Re-validating...")
            valid, headers, rows = check_file()
            if not valid:
                raise ValueError(f"Healing failed, headers still wrong: {headers}")
        else:
            raise ValueError(error)

    print("Validation passed!")

def ingest():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    # raw table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER,
            city VARCHAR(100),
            ingested_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # run log table for debugging
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            id SERIAL PRIMARY KEY,
            run_time TIMESTAMP DEFAULT NOW(),
            rows_processed INTEGER,
            min_timestamp TIMESTAMP,
            max_timestamp TIMESTAMP
        )
    """)

    rows_inserted = 0
    with open(DATA_PATH, "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO raw_users (name, age, city) VALUES (%s, %s, %s)",
                (row[0], int(row[1]), row[2])
            )
            rows_inserted += 1

    # log this run
    cur.execute("""
        INSERT INTO pipeline_run_log (rows_processed, min_timestamp, max_timestamp)
        SELECT %s, MIN(ingested_at), MAX(ingested_at) FROM raw_users
    """, (rows_inserted,))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {rows_inserted} rows to PostgreSQL!")

def run_dbt():
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "/opt/airflow/dbt_project",
         "--profiles-dir", "/opt/airflow/dbt_project"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)

with DAG(
    dag_id="ingest_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    generate_task = PythonOperator(task_id="generate_data", python_callable=generate)
    validate_task = PythonOperator(task_id="validate_data", python_callable=validate)
    ingest_task = PythonOperator(task_id="ingest_data", python_callable=ingest)
    dbt_task = PythonOperator(task_id="transform_data", python_callable=run_dbt)

    generate_task >> validate_task >> ingest_task >> dbt_task