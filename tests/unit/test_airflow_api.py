import pytest
import requests

import airflow_api


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code = status_code
        self.ok = status_code < 400
        self._body = body if body is not None else {}
        self.text = text

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


@pytest.fixture
def calls(monkeypatch):
    monkeypatch.setenv("AIRFLOW_API_URL", "http://airflow.test/api/v1/")
    monkeypatch.setenv("AIRFLOW_DASHBOARD_USER", "dashboard")
    monkeypatch.setenv("AIRFLOW_DASHBOARD_PASSWORD", "secret")
    made, replies = [], []

    def fake_request(method, url, **kwargs):
        made.append({"method": method, "url": url, **kwargs})
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(airflow_api.requests, "request", fake_request)
    return made, replies


def test_send_test_file(calls):
    made, replies = calls
    replies.append(FakeResponse(body={"dag_run_id": "manual__1"}))
    assert airflow_api.send_test_file("renamed") == "manual__1"
    assert made[0]["method"] == "POST"
    assert made[0]["url"] == "http://airflow.test/api/v1/dags/supplier_on_demand/dagRuns"
    assert made[0]["json"] == {"conf": {"kind": "renamed"}}
    assert made[0]["auth"] == ("dashboard", "secret")
    assert made[0]["timeout"] == 5


def test_automatic_files_on_reads_the_pause_state(calls):
    made, replies = calls
    replies.extend([FakeResponse(body={"is_paused": False}), FakeResponse(body={"is_paused": True})])
    assert airflow_api.automatic_files_on() is True
    assert airflow_api.automatic_files_on() is False
    assert made[0]["url"] == "http://airflow.test/api/v1/dags/supplier_dag"


def test_automatic_files_on_is_none_when_airflow_is_down(calls):
    _, replies = calls
    replies.append(requests.ConnectionError("refused"))
    assert airflow_api.automatic_files_on() is None


def test_set_automatic_files(calls):
    made, replies = calls
    replies.append(FakeResponse(body={"is_paused": True}))
    airflow_api.set_automatic_files(False)
    assert made[0]["method"] == "PATCH"
    assert made[0]["url"] == "http://airflow.test/api/v1/dags/supplier_dag"
    assert made[0]["params"] == {"update_mask": "is_paused"}
    assert made[0]["json"] == {"is_paused": True}


def test_wrong_login_is_explained(calls):
    _, replies = calls
    replies.append(FakeResponse(status_code=401, text="Unauthorized"))
    with pytest.raises(airflow_api.ApiError, match="refused the dashboard's login"):
        airflow_api.send_test_file("clean")


def test_other_http_errors_keep_airflow_text(calls):
    _, replies = calls
    replies.append(FakeResponse(status_code=404, text='{"detail": "DAG not found"}'))
    with pytest.raises(airflow_api.ApiError, match="HTTP 404.*DAG not found"):
        airflow_api.send_test_file("clean")


def test_a_reply_that_is_not_json_is_explained(calls):
    _, replies = calls
    replies.append(FakeResponse(body=requests.exceptions.JSONDecodeError("Expecting value", "<html>", 0),
                                text="<html>"))
    with pytest.raises(airflow_api.ApiError, match="^Airflow sent a reply the dashboard could not read.$"):
        airflow_api.send_test_file("clean")


def test_unreachable_airflow_is_explained(calls):
    _, replies = calls
    replies.append(requests.ConnectionError("refused"))
    with pytest.raises(airflow_api.ApiError, match="not reachable"):
        airflow_api.send_test_file("clean")


def test_missing_login_settings(monkeypatch):
    monkeypatch.delenv("AIRFLOW_DASHBOARD_USER", raising=False)
    with pytest.raises(airflow_api.ApiError, match="AIRFLOW_DASHBOARD_USER"):
        airflow_api.send_test_file("clean")
