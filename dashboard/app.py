"""Live dashboard for the self-healing pipeline. Refreshes itself every 5 seconds."""
import json
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Self-Healing Pipeline", layout="wide")

METHOD_LABELS = {"rules": "Rules", "llm": "LLaMA 3.2"}


def query(sql: str) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame. Returns an empty one if the table does not exist yet."""
    try:
        conn = psycopg2.connect(
            host=os.environ.get("PIPELINE_DB_HOST", "postgres"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
    except psycopg2.Error:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    except psycopg2.Error:
        return pd.DataFrame()
    finally:
        conn.close()


def show_json(value):
    try:
        st.json(json.loads(value))
    except (TypeError, ValueError):
        st.write(value)


st.title("Self-Healing Pipeline Dashboard")


@st.fragment(run_every=5)
def live_view():
    runs = query("""
        SELECT run_id, status, started_at, finished_at, files_processed, rows_loaded,
               rows_rejected, heals, failed_task, error_message
        FROM pipeline_run_log WHERE run_id IS NOT NULL ORDER BY started_at DESC""")
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

    st.caption(f"Updated at {datetime.now():%H:%M:%S}. Refreshes every 5 seconds. Times are UTC.")

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
    cols[4].metric("Files repaired", len(repairs))
    cols[5].metric("Repair success", repair_rate)
    st.caption(f"Last run started {last['started_at']}")

    st.subheader("Runs")
    if not finished.empty:
        chart = px.bar(finished.sort_values("started_at"), x="started_at", y="rows_loaded", color="status",
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
                outcome = "Fixed" if row["success"] else "Failed"
                method = METHOD_LABELS.get(row["method"], "no method")
                with st.expander(f"{outcome} by {method}: {row['source_file']} ({row['drift_type']})"):
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
