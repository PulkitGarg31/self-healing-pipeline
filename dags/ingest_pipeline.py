"""Pipeline DAG: starts when new data lands in data/incoming/, then runs 4 linked tasks.

validate_schema >> quality_checks >> ingest_data >> transform_data
"""
import subprocess
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from pipeline import config, db, files, quality
from pipeline.healer import heal_file
from pipeline.llm import ask_ollama

DBT_DIR = "/opt/airflow/dbt_project"


def validate_schema(run_id, **_):
    data_dir = config.data_dir()
    raw_files = files.claim_incoming(data_dir, run_id)
    if not raw_files:
        raise AirflowSkipException("No new files in data/incoming/")
    valid_dir = files.run_dir(data_dir, run_id) / "valid"
    results = [heal_file(path, valid_dir / path.name, ask_ollama) for path in raw_files]
    conn = db.connect()
    try:
        db.ensure_tables(conn)
        for result in results:
            if result.healed:
                db.log_heal(conn, run_id, result)
    finally:
        conn.close()
    failed = [r for r in results if not r.success]
    if failed:
        raise ValueError("Could not repair: " + "; ".join(f"{r.source_file} ({r.error_message})" for r in failed))
    return {"files": len(raw_files), "heals": sum(1 for r in results if r.healed)}


def quality_checks(run_id, **_):
    folder = files.run_dir(config.data_dir(), run_id)
    good, rejected = quality.split_rows(sorted((folder / "valid").glob("*.csv")))
    quality.write_good(good, folder / "good.csv")
    quality.write_rejected(rejected, folder / "rejected.csv")
    return {"good": len(good), "rejected": len(rejected)}


def ingest_data(run_id, **_):
    folder = files.run_dir(config.data_dir(), run_id)
    good = quality.read_good(folder / "good.csv")
    rejected = quality.read_rejected(folder / "rejected.csv")
    conn = db.connect()
    try:
        db.ensure_tables(conn)
        _, _, skipped = db.load_rows(conn, run_id, good, rejected)
        # Report everything this run has in the database, so a re-run never logs fewer rows.
        total_good, total_rejected = db.run_totals(conn, run_id)
    finally:
        conn.close()
    if skipped:
        print("Already loaded, so not saved again: " + ", ".join(skipped))
    return {"rows_loaded": total_good, "rows_rejected": total_rejected, "files_skipped": skipped}


def transform_data(**_):
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError((result.stdout + result.stderr)[-1500:])


def _task_output(dag_run, task_id) -> dict:
    ti = dag_run.get_task_instance(task_id)
    return (ti.xcom_pull(task_ids=task_id) if ti else None) or {}


def on_run_success(context):
    """DAG callback: log a finished run (or a run with no new files) and archive its files."""
    dag_run = context["dag_run"]
    try:
        validate_ti = dag_run.get_task_instance("validate_schema")
        status = "no_data" if validate_ti and validate_ti.state == "skipped" else "success"
        validated = _task_output(dag_run, "validate_schema")
        ingested = _task_output(dag_run, "ingest_data")
        conn = db.connect()
        try:
            db.ensure_tables(conn)
            db.write_run_log(
                conn, dag_run.run_id, status, dag_run.start_date, datetime.now(timezone.utc),
                files_processed=validated.get("files", 0),
                heals=validated.get("heals", 0),
                rows_loaded=ingested.get("rows_loaded", 0),
                rows_rejected=ingested.get("rows_rejected", 0),
            )
        finally:
            conn.close()
    finally:
        # Archive the files even if the run log could not be written.
        files.finish_run(config.data_dir(), dag_run.run_id, "processed")


def on_task_failure(context):
    """Task callback after the last retry fails: log the failed run and keep its files in data/failed/."""
    dag_run = context["dag_run"]
    data_dir = config.data_dir()
    try:
        validated = _task_output(dag_run, "validate_schema")
        ingested = _task_output(dag_run, "ingest_data")
        conn = db.connect()
        try:
            db.ensure_tables(conn)
            db.write_run_log(
                conn, dag_run.run_id, "failed", dag_run.start_date, datetime.now(timezone.utc),
                files_processed=len(files.raw_files(data_dir, dag_run.run_id)),
                heals=validated.get("heals", 0),
                rows_loaded=ingested.get("rows_loaded", 0),
                rows_rejected=ingested.get("rows_rejected", 0),
                failed_task=context["task_instance"].task_id,
                error_message=str(
                    context.get("exception")
                    or "task failed without an error message (killed or marked failed outside the task)"
                )[-1000:],
            )
        finally:
            conn.close()
    finally:
        # Archive the files even if the run log could not be written.
        files.finish_run(data_dir, dag_run.run_id, "failed")


with DAG(
    dag_id="ingest_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset(config.INCOMING_DATASET_URI)],
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_task_failure,
    },
    on_success_callback=on_run_success,
    tags=["self-healing"],
) as dag:
    validate_task = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    quality_task = PythonOperator(task_id="quality_checks", python_callable=quality_checks)
    ingest_task = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=10),
    )

    validate_task >> quality_task >> ingest_task >> transform_task
