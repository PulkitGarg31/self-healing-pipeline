import uuid
from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from pipeline import db  # noqa: E402
from pipeline.quality import Rejected  # noqa: E402

pytestmark = pytest.mark.db


@pytest.fixture
def conn():
    try:
        connection = db.connect()
    except Exception as exc:
        pytest.skip(f"Postgres is not reachable: {exc}")
    db.ensure_tables(connection)
    yield connection
    connection.close()


@pytest.fixture
def run_id(conn):
    value = f"test-{uuid.uuid4()}"
    yield value
    with conn.cursor() as cur:
        for table in ("raw_users", "rejected_rows", "pipeline_run_log", "healing_log"):
            cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (value,))
    conn.commit()


def count(conn, table, run_id):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
        return cur.fetchone()[0]


def test_ensure_tables_can_run_twice(conn):
    db.ensure_tables(conn)
    db.ensure_tables(conn)


def test_load_rows_saves_good_and_rejected_rows(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"]), ("a.csv", ["Ravi", "41", "Pune"])]
    rejected = [Rejected("a.csv", ["", "22", "Goa"], "name is empty")]
    assert db.load_rows(conn, run_id, good, rejected) is True
    assert count(conn, "raw_users", run_id) == 2
    assert count(conn, "rejected_rows", run_id) == 1


def test_loading_the_same_run_twice_saves_rows_once(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"])]
    rejected = [Rejected("a.csv", ["", "22", "Goa"], "name is empty")]
    assert db.load_rows(conn, run_id, good, rejected) is True
    assert db.load_rows(conn, run_id, good, rejected) is False
    assert count(conn, "raw_users", run_id) == 1
    assert count(conn, "rejected_rows", run_id) == 1


def test_crash_halfway_saves_nothing(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"]), ("a.csv", ["Ravi", "not-a-number", "Pune"])]
    with pytest.raises(ValueError):
        db.load_rows(conn, run_id, good, [])
    assert count(conn, "raw_users", run_id) == 0


def test_run_log_keeps_one_row_per_run(conn, run_id):
    now = datetime.now(timezone.utc)
    db.write_run_log(conn, run_id, "failed", now, now, failed_task="ingest_data", error_message="boom")
    db.write_run_log(conn, run_id, "success", now, now, rows_loaded=3)
    with conn.cursor() as cur:
        cur.execute("SELECT status, rows_loaded, failed_task FROM pipeline_run_log WHERE run_id = %s", (run_id,))
        assert cur.fetchall() == [("success", 3, None)]


def test_log_heal_saves_any_ai_answer(conn, run_id):
    from pipeline.healer import HealResult
    result = HealResult("a.csv", healed=True, success=False, method="llm", drift_type="missing columns: name",
                        llm_diagnosis=["not", "a", "dict"], error_message="ValueError: AI answer has no 'mapping' object")
    db.log_heal(conn, run_id, result)
    with conn.cursor() as cur:
        cur.execute("SELECT method, success, llm_diagnosis FROM healing_log WHERE run_id = %s", (run_id,))
        assert cur.fetchall() == [("llm", False, '["not", "a", "dict"]')]
