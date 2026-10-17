"""Postgres access: create and upgrade tables, save rows, and log repairs and runs."""
import json
from datetime import datetime

import psycopg2

from pipeline import config

# CREATE ... IF NOT EXISTS and ADD COLUMN IF NOT EXISTS let an older database upgrade in place.
TABLE_SQL = [
    """CREATE TABLE IF NOT EXISTS raw_users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INTEGER,
        city VARCHAR(100),
        ingested_at TIMESTAMP DEFAULT NOW()
    )""",
    "ALTER TABLE raw_users ADD COLUMN IF NOT EXISTS run_id TEXT",
    "ALTER TABLE raw_users ADD COLUMN IF NOT EXISTS source_file TEXT",
    """CREATE TABLE IF NOT EXISTS rejected_rows (
        id SERIAL PRIMARY KEY,
        run_id TEXT,
        source_file TEXT,
        row_data TEXT,
        reason TEXT,
        rejected_at TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS healing_log (
        id SERIAL PRIMARY KEY,
        error_message TEXT,
        llm_diagnosis TEXT,
        success BOOLEAN,
        healed_at TIMESTAMP DEFAULT NOW()
    )""",
    "ALTER TABLE healing_log ADD COLUMN IF NOT EXISTS run_id TEXT",
    "ALTER TABLE healing_log ADD COLUMN IF NOT EXISTS source_file TEXT",
    "ALTER TABLE healing_log ADD COLUMN IF NOT EXISTS drift_type TEXT",
    "ALTER TABLE healing_log ADD COLUMN IF NOT EXISTS method TEXT",
    "ALTER TABLE healing_log ADD COLUMN IF NOT EXISTS mapping TEXT",
    """CREATE TABLE IF NOT EXISTS pipeline_run_log (
        id SERIAL PRIMARY KEY,
        run_time TIMESTAMP DEFAULT NOW()
    )""",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS run_id TEXT",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS status TEXT",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS started_at TIMESTAMP",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS files_processed INTEGER",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS rows_loaded INTEGER",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS rows_rejected INTEGER",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS heals INTEGER",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS failed_task TEXT",
    "ALTER TABLE pipeline_run_log ADD COLUMN IF NOT EXISTS error_message TEXT",
    "CREATE UNIQUE INDEX IF NOT EXISTS pipeline_run_log_run_id ON pipeline_run_log (run_id)",
]


def connect():
    return psycopg2.connect(**config.db_settings())


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for sql in TABLE_SQL:
            cur.execute(sql)
    conn.commit()


def log_heal(conn, run_id: str, result) -> None:
    """Save one repair attempt (a healer.HealResult) to healing_log."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO healing_log
                   (run_id, source_file, drift_type, method, mapping, llm_diagnosis, success, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                run_id,
                result.source_file,
                result.drift_type,
                result.method,
                json.dumps(result.mapping) if result.mapping is not None else None,
                json.dumps(result.llm_diagnosis) if result.llm_diagnosis is not None else None,
                result.success,
                result.error_message,
            ),
        )
    conn.commit()


def load_rows(conn, run_id: str, good, rejected) -> bool:
    """Save good rows to raw_users and bad rows to rejected_rows in one transaction: all or nothing.

    A run that is already saved (it was re-run after a later task failed) is not saved twice.
    Returns True when rows were saved, False when the run was already loaded.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM raw_users WHERE run_id = %s
                   UNION ALL SELECT 1 FROM rejected_rows WHERE run_id = %s
                   LIMIT 1""",
                (run_id, run_id),
            )
            if cur.fetchone():
                conn.rollback()
                return False
            for source_file, (name, age, city) in good:
                cur.execute(
                    "INSERT INTO raw_users (name, age, city, run_id, source_file) VALUES (%s, %s, %s, %s, %s)",
                    (name, int(age), city, run_id, source_file),
                )
            for item in rejected:
                cur.execute(
                    "INSERT INTO rejected_rows (run_id, source_file, row_data, reason) VALUES (%s, %s, %s, %s)",
                    (run_id, item.source_file, json.dumps(item.row), item.reason),
                )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def write_run_log(conn, run_id: str, status: str, started_at: datetime | None, finished_at: datetime,
                  files_processed: int = 0, rows_loaded: int = 0, rows_rejected: int = 0, heals: int = 0,
                  failed_task: str | None = None, error_message: str | None = None) -> None:
    """Save one row per pipeline run. Writing the same run again updates its row."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO pipeline_run_log
                   (run_id, status, started_at, finished_at, files_processed, rows_loaded,
                    rows_rejected, heals, failed_task, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (run_id) DO UPDATE SET
                   status = EXCLUDED.status,
                   started_at = EXCLUDED.started_at,
                   finished_at = EXCLUDED.finished_at,
                   files_processed = EXCLUDED.files_processed,
                   rows_loaded = EXCLUDED.rows_loaded,
                   rows_rejected = EXCLUDED.rows_rejected,
                   heals = EXCLUDED.heals,
                   failed_task = EXCLUDED.failed_task,
                   error_message = EXCLUDED.error_message""",
            (run_id, status, started_at, finished_at, files_processed, rows_loaded,
             rows_rejected, heals, failed_task, error_message),
        )
    conn.commit()
