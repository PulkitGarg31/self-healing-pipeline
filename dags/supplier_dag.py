"""Supplier DAGs: pretend to be the company that sends data. They are not part of the pipeline.

supplier_dag drops one random CSV file every 2 minutes. supplier_on_demand drops one file of a
chosen kind when the dashboard's buttons ask for it, even while supplier_dag is paused. Both mark
the incoming dataset as updated, which starts the ingest_pipeline DAG.
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


def make_supplier_dag(dag_id: str, schedule, description: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2024, 1, 1),
        schedule=schedule,
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
    return dag


supplier_dag = make_supplier_dag("supplier_dag", "*/2 * * * *", "Drops one random file every 2 minutes.")
supplier_on_demand = make_supplier_dag(
    "supplier_on_demand", None, "Drops one file of a chosen kind when the dashboard asks.")
