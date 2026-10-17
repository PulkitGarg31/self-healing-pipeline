"""Supplier DAG: pretends to be the company that sends data. It is not part of the pipeline.

Every 2 minutes it drops one CSV file into data/incoming/ and marks the incoming dataset as
updated, which starts the ingest_pipeline DAG.
"""
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from pipeline import config, supplier


def drop_file(params, **_):
    path, kind = supplier.write_supplier_file(config.incoming_dir(), kind=params["kind"])
    print(f"Dropped {path.name} (kind: {kind})")
    return {"file": path.name, "kind": kind}


with DAG(
    dag_id="supplier_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    params={
        "kind": Param("random", type="string", enum=["random", *supplier.KINDS],
                      description="Which kind of file to drop"),
    },
    tags=["self-healing"],
) as dag:
    PythonOperator(
        task_id="drop_file",
        python_callable=drop_file,
        outlets=[Dataset(config.INCOMING_DATASET_URI)],
    )
