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
    assert set(dagbag.dag_ids) == {"supplier_dag", "ingest_pipeline"}


def test_pipeline_has_four_tasks_linked_in_order(dagbag):
    tasks = dagbag.get_dag("ingest_pipeline").task_dict
    assert sorted(tasks) == sorted(PIPELINE_TASKS)
    for current, following in zip(PIPELINE_TASKS, PIPELINE_TASKS[1:]):
        assert tasks[current].downstream_task_ids == {following}
    assert tasks["transform_data"].downstream_task_ids == set()


def test_every_pipeline_task_retries_twice(dagbag):
    assert all(task.retries == 2 for task in dagbag.get_dag("ingest_pipeline").tasks)


def test_pipeline_starts_when_new_data_lands(dagbag):
    dag = dagbag.get_dag("ingest_pipeline")
    assert type(dag.timetable).__name__ == "DatasetTriggeredTimetable"
    assert config.INCOMING_DATASET_URI in repr(dag.dataset_triggers)


def test_supplier_marks_the_dataset_updated(dagbag):
    task = dagbag.get_dag("supplier_dag").get_task("drop_file")
    assert [outlet.uri for outlet in task.outlets] == [config.INCOMING_DATASET_URI]
