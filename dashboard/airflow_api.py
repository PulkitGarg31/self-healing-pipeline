"""Sends test files and switches the automatic supplier on or off, through Airflow's REST API."""
import os

import requests

ON_DEMAND_DAG = "supplier_on_demand"
AUTOMATIC_DAG = "supplier_dag"


class ApiError(Exception):
    """A plain-language reason the Airflow call did not work."""


def _base() -> str:
    return os.environ.get("AIRFLOW_API_URL", "http://airflow:8080/api/v1").rstrip("/")


def _auth() -> tuple[str, str]:
    try:
        return os.environ["AIRFLOW_DASHBOARD_USER"], os.environ["AIRFLOW_DASHBOARD_PASSWORD"]
    except KeyError as exc:
        raise ApiError(f"The setting {exc.args[0]} is missing, so the dashboard can't talk to Airflow.") from exc


def _call(method: str, path: str, **kwargs) -> dict:
    auth = _auth()
    try:
        response = requests.request(method, f"{_base()}{path}", auth=auth, timeout=5, **kwargs)
    except requests.RequestException as exc:
        raise ApiError("Airflow is not reachable. Is the airflow container running?") from exc
    if response.status_code in (401, 403):
        raise ApiError("Airflow refused the dashboard's login. Check AIRFLOW_DASHBOARD_USER and "
                       "AIRFLOW_DASHBOARD_PASSWORD in .env.")
    if not response.ok:
        raise ApiError(f"Airflow returned HTTP {response.status_code}: {response.text[:200]}")
    try:
        return response.json()
    except ValueError as exc:  # requests' JSONDecodeError is a ValueError too
        raise ApiError("Airflow sent a reply the dashboard could not read.") from exc


def send_test_file(kind: str) -> str:
    """Ask supplier_on_demand to drop one file of this kind. Returns the new run's id."""
    body = _call("POST", f"/dags/{ON_DEMAND_DAG}/dagRuns", json={"conf": {"kind": kind}})
    return body.get("dag_run_id", "")


def automatic_files_on() -> bool | None:
    """True if supplier_dag is running every 2 minutes, False if paused, None if Airflow can't say."""
    try:
        return not _call("GET", f"/dags/{AUTOMATIC_DAG}")["is_paused"]
    except (ApiError, KeyError, TypeError):
        return None


def set_automatic_files(on: bool) -> None:
    _call("PATCH", f"/dags/{AUTOMATIC_DAG}", params={"update_mask": "is_paused"}, json={"is_paused": not on})
