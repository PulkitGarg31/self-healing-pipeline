"""Live dashboard for the self-healing pipeline. Refreshes itself every 5 seconds."""
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Self-Healing Pipeline", layout="wide")

METHOD_LABELS = {"rules": "Rules", "llm": "LLaMA 3.2"}
STATUS_COLOURS = {"success": "#2e7d32", "failed": "#c62828"}


def connect():
    """Open a database connection. Raises KeyError if a setting is missing from the environment."""
    return psycopg2.connect(
        host=os.environ.get("PIPELINE_DB_HOST", "postgres"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=3,
    )


def db_error() -> str | None:
    """Say why the database cannot be reached, or return None when a connection works."""
    try:
        connect().close()
    except KeyError as exc:
        return f"the setting {exc} is missing from the environment"
    except psycopg2.Error as exc:
        return str(exc).strip() or type(exc).__name__
    return None


def query(sql: str) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame. Returns an empty one if the table does not exist yet."""
    try:
        conn = connect()
    except (KeyError, psycopg2.Error) as exc:
        print(f"dashboard: cannot connect to the database: {type(exc).__name__}: {exc}", file=sys.stderr)
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    except psycopg2.Error as exc:
        print(f"dashboard: query failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return pd.DataFrame()
    finally:
        conn.close()


def show_json(value):
    """Show a JSON object or list as a tree, and anything else (a JSON string, a number) as text."""
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        st.code(str(value))
        return
    if isinstance(parsed, (dict, list)):
        st.json(parsed)
    else:
        st.code(parsed if isinstance(parsed, str) else str(value))


st.title("Self-Healing Pipeline Dashboard")


@st.fragment(run_every=5)
def live_view():
    st.caption(f"Updated at {datetime.now(timezone.utc):%H:%M:%S} UTC. "
               "Refreshes every 5 seconds. Times are UTC.")

    error = db_error()
    if error:
        st.warning(f"Cannot reach the database: {error}")
        return

    runs = query("""
        SELECT run_id, status, started_at, finished_at, files_processed, rows_loaded,
               rows_rejected, heals, failed_task, error_message
        FROM pipeline_run_log WHERE run_id IS NOT NULL ORDER BY started_at DESC NULLS LAST""")
    # A retry repairs the same file again, so keep only the latest attempt for each file.
    repairs = query("""
        SELECT * FROM (
            SELECT DISTINCT ON (run_id, source_file)
                   healed_at, run_id, source_file, drift_type, method, mapping, llm_diagnosis,
                   success, error_message
            FROM healing_log WHERE drift_type IS NOT NULL
            ORDER BY run_id, source_file, healed_at DESC
        ) latest ORDER BY healed_at DESC""")
    attempts = query("SELECT COUNT(*) AS n FROM healing_log WHERE drift_type IS NOT NULL")
    rejected = query("""
        SELECT rejected_at, source_file, row_data, reason
        FROM rejected_rows ORDER BY rejected_at DESC LIMIT 50""")
    cities = query("SELECT city, total_users, avg_age FROM city_summary ORDER BY total_users DESC")

    if runs.empty:
        st.info("No pipeline runs yet. The supplier drops a file every 2 minutes, "
                "or trigger supplier_dag in Airflow.")
        return

    last = runs.iloc[0]
    finished = runs[runs["status"] != "no_data"]
    fixed = int(repairs["success"].sum()) if not repairs.empty else 0
    repair_rate = f"{fixed / len(repairs) * 100:.0f}%" if len(repairs) else "n/a"

    cols = st.columns(6)
    cols[0].metric("Last run", str(last["status"]))
    cols[1].metric("Runs", len(finished))
    cols[2].metric("Rows loaded", int(runs["rows_loaded"].fillna(0).sum()))
    cols[3].metric("Rows rejected", int(runs["rows_rejected"].fillna(0).sum()))
    cols[4].metric("Files repaired", fixed)
    cols[5].metric("Repair success", repair_rate)
    if pd.notna(last["started_at"]):
        st.caption(f"Last run started {last['started_at']:%Y-%m-%d %H:%M:%S}")

    st.subheader("Runs")
    if finished.empty:
        st.info("No finished runs yet.")
    else:
        chart = px.scatter(finished.sort_values("started_at"), x="started_at", y="rows_loaded", color="status",
                           color_discrete_map=STATUS_COLOURS, hover_data=["run_id", "failed_task"],
                           labels={"started_at": "Run started", "rows_loaded": "Rows loaded"})
        st.plotly_chart(chart, use_container_width=True)

    st.subheader("Failed runs")
    failed = runs[runs["status"] == "failed"]
    if failed.empty:
        st.success("No failed runs.")
    else:
        st.dataframe(failed[["started_at", "run_id", "failed_task", "error_message"]],
                     use_container_width=True, hide_index=True)

    st.subheader("Repairs")
    if repairs.empty:
        st.info("No repairs yet.")
    else:
        total_attempts = int(attempts["n"].iloc[0]) if not attempts.empty else len(repairs)
        st.caption(f"Latest attempt for each file. {total_attempts} attempts in total, including retries.")
        left, right = st.columns([1, 2])
        left.bar_chart(repairs["method"].map(METHOD_LABELS).fillna("Not repaired").value_counts())
        with right:
            for _, row in repairs.head(20).iterrows():
                method = METHOD_LABELS.get(row["method"])
                if method is None:
                    title = f"Not repaired: {row['source_file']} ({row['drift_type']})"
                else:
                    outcome = "Fixed" if row["success"] else "Failed"
                    title = f"{outcome} by {method}: {row['source_file']} ({row['drift_type']})"
                with st.expander(title):
                    if row["mapping"]:
                        st.write("Column mapping:")
                        show_json(row["mapping"])
                    if row["llm_diagnosis"]:
                        st.write("AI answer:")
                        show_json(row["llm_diagnosis"])
                    if row["error_message"]:
                        st.error(row["error_message"])

    left, right = st.columns(2)
    with left:
        st.subheader("Rejected rows")
        if rejected.empty:
            st.info("No rejected rows.")
        else:
            st.dataframe(rejected, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Users by city")
        if cities.empty:
            st.info("No city data yet.")
        else:
            st.plotly_chart(px.pie(cities, names="city", values="total_users"), use_container_width=True)


live_view()
