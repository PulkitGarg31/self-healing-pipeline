import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

from pipeline import config  # noqa: E402

pytestmark = pytest.mark.airflow

PIPELINE_TASKS = ["validate_schema", "quality_checks", "ingest_data", "transform_data"]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_dags_load_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"supplier_dag", "supplier_on_demand", "ingest_pipeline"}


def test_pipeline_has_four_tasks_linked_in_order(dagbag):
    tasks = dagbag.get_dag("ingest_pipeline").task_dict
    assert sorted(tasks) == sorted(PIPELINE_TASKS)
    for current, following in zip(PIPELINE_TASKS, PIPELINE_TASKS[1:]):
        assert tasks[current].downstream_task_ids == {following}
    assert tasks["transform_data"].downstream_task_ids == set()


def test_every_pipeline_task_retries_twice(dagbag):
    assert all(task.retries == 2 for task in dagbag.get_dag("ingest_pipeline").tasks)


def test_pipeline_logs_every_run(dagbag):
    dag = dagbag.get_dag("ingest_pipeline")
    assert dag.on_success_callback is not None
    assert all(task.on_failure_callback is not None for task in dag.tasks)


def test_pipeline_starts_when_new_data_lands(dagbag):
    dag = dagbag.get_dag("ingest_pipeline")
    assert type(dag.timetable).__name__ == "DatasetTriggeredTimetable"
    assert config.INCOMING_DATASET_URI in repr(dag.dataset_triggers)


def test_suppliers_mark_the_dataset_updated(dagbag):
    for dag_id in ("supplier_dag", "supplier_on_demand"):
        task = dagbag.get_dag(dag_id).get_task("drop_file")
        assert [outlet.uri for outlet in task.outlets] == [config.INCOMING_DATASET_URI]


def test_on_demand_supplier_only_runs_when_asked(dagbag):
    assert type(dagbag.get_dag("supplier_on_demand").timetable).__name__ == "NullTimetable"


def test_pipeline_package_is_not_scanned_for_dags():
    from pathlib import Path
    from airflow.utils.file import list_py_file_paths
    paths = list_py_file_paths("/opt/airflow/dags", safe_mode=False, include_examples=False)
    assert sorted(Path(p).name for p in paths) == ["ingest_pipeline.py", "supplier_dag.py"]
