import uuid
from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from pipeline import db  # noqa: E402
from pipeline.healer import HealResult  # noqa: E402
from pipeline.quality import Rejected  # noqa: E402

pytestmark = pytest.mark.db


@pytest.fixture
def conn():
    """A connection whose tables live in a new, empty schema that is dropped afterwards.

    Tests never touch the live pipeline tables, so nothing they write reaches dbt or the dashboard.
    """
    try:
        connection = db.connect()
    except Exception as exc:
        pytest.skip(f"Postgres is not reachable: {exc}")
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with connection.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
    connection.commit()
    yield connection
    connection.rollback()
    with connection.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    connection.commit()
    connection.close()


@pytest.fixture
def run_id(conn):
    db.ensure_tables(conn)
    return f"test-{uuid.uuid4()}"


def count(conn, table, run_id):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
        return cur.fetchone()[0]


def test_ensure_tables_can_run_twice(conn):
    db.ensure_tables(conn)  # empty schema: creates the tables
    db.ensure_tables(conn)  # tables already there: must not fail
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema = current_schema() ORDER BY table_name""")
        assert [row[0] for row in cur.fetchall()] == ["healing_log", "pipeline_run_log", "raw_users", "rejected_rows"]


def test_old_database_is_upgraded(conn):
    # The table shapes and rows the previous version of the project left behind.
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE raw_users (
            id SERIAL PRIMARY KEY, name VARCHAR(100), age INTEGER, city VARCHAR(100),
            ingested_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("""CREATE TABLE pipeline_run_log (
            id SERIAL PRIMARY KEY, run_time TIMESTAMP DEFAULT NOW(), rows_processed INTEGER,
            min_timestamp TIMESTAMP, max_timestamp TIMESTAMP)""")
        cur.execute("""CREATE TABLE healing_log (
            id SERIAL PRIMARY KEY, error_message TEXT, llm_diagnosis TEXT, success BOOLEAN,
            healed_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("INSERT INTO raw_users (name, age, city) VALUES ('Meera', 52, 'Agra')")
        cur.execute("INSERT INTO pipeline_run_log (rows_processed) VALUES (3)")
        cur.execute("INSERT INTO healing_log (error_message, success) VALUES ('old error', TRUE)")
    conn.commit()

    db.ensure_tables(conn)

    run_id = f"test-{uuid.uuid4()}"
    assert db.load_rows(conn, run_id, [("a.csv", ["Asha", "30", "Delhi"])], []) == (1, 0, [])
    now = datetime.now(timezone.utc)
    db.write_run_log(conn, run_id, "failed", now, now, failed_task="run_dbt", error_message="boom")
    db.write_run_log(conn, run_id, "success", now, now, rows_loaded=1)
    db.log_heal(conn, run_id, HealResult("a.csv", healed=True, success=True, method="rules", drift_type="separator ';'"))
    assert count(conn, "raw_users", run_id) == 1
    assert count(conn, "pipeline_run_log", run_id) == 1
    assert count(conn, "healing_log", run_id) == 1
    with conn.cursor() as cur:
        for table in ("raw_users", "pipeline_run_log", "healing_log"):
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id IS NULL")
            assert cur.fetchone()[0] == 1, f"old row missing from {table}"


def test_load_rows_saves_good_and_rejected_rows(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"]), ("a.csv", ["Ravi", "41", "Pune"])]
    rejected = [Rejected("a.csv", ["", "22", "Goa"], "name is empty")]
    assert db.load_rows(conn, run_id, good, rejected) == (2, 1, [])
    assert count(conn, "raw_users", run_id) == 2
    assert count(conn, "rejected_rows", run_id) == 1


def test_a_file_is_never_loaded_twice(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"])]
    rejected = [Rejected("a.csv", ["", "22", "Goa"], "name is empty")]
    assert db.load_rows(conn, run_id, good, rejected) == (1, 1, [])
    assert db.load_rows(conn, run_id, good, rejected) == (0, 0, ["a.csv"])
    assert count(conn, "raw_users", run_id) == 1
    assert count(conn, "rejected_rows", run_id) == 1


def test_new_files_in_a_cleared_run_are_still_loaded(conn, run_id):
    # A successful run that is cleared in Airflow claims new files under its old run id.
    assert db.load_rows(conn, run_id, [("a.csv", ["Asha", "30", "Delhi"])], []) == (1, 0, [])
    good = [("a.csv", ["Asha", "30", "Delhi"]), ("b.csv", ["Ravi", "41", "Pune"])]
    assert db.load_rows(conn, run_id, good, []) == (1, 0, ["a.csv"])
    assert count(conn, "raw_users", run_id) == 2
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM raw_users WHERE run_id = %s AND source_file = 'b.csv'", (run_id,))
        assert cur.fetchall() == [("Ravi",)]


def test_loading_nothing_works(conn, run_id):
    assert db.load_rows(conn, run_id, [], []) == (0, 0, [])


def test_a_file_with_only_rejected_rows_is_never_loaded_twice(conn, run_id):
    rejected = [Rejected("r.csv", ["", "1", "x"], "name is empty")]
    assert db.load_rows(conn, run_id, [], rejected) == (0, 1, [])
    assert db.load_rows(conn, run_id, [], rejected) == (0, 0, ["r.csv"])
    assert count(conn, "rejected_rows", run_id) == 1


def test_run_totals_counts_every_row_of_the_run(conn, run_id):
    good = [("a.csv", ["Asha", "30", "Delhi"]), ("a.csv", ["Ravi", "41", "Pune"])]
    rejected = [Rejected("a.csv", ["", "22", "Goa"], "name is empty")]
    db.load_rows(conn, run_id, good, rejected)
    assert db.run_totals(conn, run_id) == (2, 1)


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


def test_a_success_row_is_never_replaced_by_no_data(conn, run_id):
    # A successful run that is cleared and then finds no new files must keep its success row.
    now = datetime.now(timezone.utc)
    db.write_run_log(conn, run_id, "success", now, now, files_processed=1, rows_loaded=3, rows_rejected=1, heals=1)
    db.write_run_log(conn, run_id, "no_data", now, now)
    with conn.cursor() as cur:
        cur.execute("""SELECT status, files_processed, rows_loaded, rows_rejected, heals
                       FROM pipeline_run_log WHERE run_id = %s""", (run_id,))
        assert cur.fetchall() == [("success", 1, 3, 1, 1)]


def test_log_heal_saves_any_ai_answer(conn, run_id):
    # An empty answer is still an answer: it must be stored as '{}', not left empty.
    result = HealResult("a.csv", healed=True, success=False, method="llm", drift_type="missing columns: name",
                        llm_diagnosis={}, error_message="ValueError: AI answer has no 'mapping' object")
    db.log_heal(conn, run_id, result)
    with conn.cursor() as cur:
        cur.execute("SELECT method, success, llm_diagnosis FROM healing_log WHERE run_id = %s", (run_id,))
        assert cur.fetchall() == [("llm", False, "{}")]


def test_log_text_with_nul_characters_is_saved(conn, run_id):
    result = HealResult("a.csv", healed=True, success=False, method=None, drift_type="unknown columns: na\x00me",
                        error_message="ValueError: bad header na\x00me")
    db.log_heal(conn, run_id, result)
    now = datetime.now(timezone.utc)
    db.write_run_log(conn, run_id, "failed", now, now, failed_task="heal\x00", error_message="boom\x00")
    with conn.cursor() as cur:
        cur.execute("SELECT drift_type, error_message FROM healing_log WHERE run_id = %s", (run_id,))
        assert cur.fetchall() == [("unknown columns: name", "ValueError: bad header name")]
        cur.execute("SELECT failed_task, error_message FROM pipeline_run_log WHERE run_id = %s", (run_id,))
        assert cur.fetchall() == [("heal", "boom")]
