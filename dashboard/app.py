import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import json
from datetime import datetime

DB_CONN = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def get_conn():
    return psycopg2.connect(**DB_CONN)

def safe_query(sql):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()

def get_healing_log():
    return safe_query("SELECT * FROM healing_log ORDER BY healed_at DESC")

def get_run_log():
    return safe_query("SELECT * FROM pipeline_run_log ORDER BY run_time DESC")

def get_raw_users():
    return safe_query("SELECT * FROM raw_users ORDER BY ingested_at DESC LIMIT 20")

def get_city_summary():
    return safe_query("SELECT * FROM city_summary")

# --- PAGE CONFIG ---
st.set_page_config(page_title="Pipeline Dashboard", layout="wide")
st.title("Self-Healing Pipeline Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

st.button("Refresh")

# --- LOAD DATA ---
healing_df = get_healing_log()
run_df = get_run_log()

# --- TOP METRICS ---
total_heals = len(healing_df)
successful_heals = len(healing_df[healing_df["success"] == True]) if not healing_df.empty else 0
heal_rate = round((successful_heals / total_heals * 100), 1) if total_heals > 0 else 0
total_runs = len(run_df)
total_rows = int(run_df["rows_processed"].sum()) if not run_df.empty else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Pipeline Runs", total_runs)
col2.metric("Total Rows Ingested", total_rows)
col3.metric("AI Interventions", total_heals)
col4.metric("Heal Success Rate", f"{heal_rate}%")

st.divider()

# --- HEALING LOG ---
st.subheader("AI Intervention Log")
if healing_df.empty:
    st.info("No interventions yet — trigger the DAG to see healing in action.")
else:
    for _, row in healing_df.iterrows():
        try:
            diagnosis = json.loads(row["llm_diagnosis"]) if isinstance(row["llm_diagnosis"], str) else row["llm_diagnosis"]
        except:
            diagnosis = row["llm_diagnosis"]
        status = "✅" if row["success"] else "❌"
        with st.expander(f"{status} {row['healed_at']} — {str(row['error_message'])[:60]}"):
            st.write("**Error:**", row["error_message"])
            st.write("**LLM Diagnosis:**")
            st.json(diagnosis)
            st.write("**Outcome:**", "Success" if row["success"] else "Failed")

st.divider()

# --- CHARTS ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Rows Ingested Over Time")
    if not run_df.empty:
        fig = px.bar(run_df, x="run_time", y="rows_processed",
                     labels={"run_time": "Run Time", "rows_processed": "Rows"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No run data yet.")

with col2:
    st.subheader("Users by City")
    city_df = get_city_summary()
    if not city_df.empty:
        fig = px.pie(city_df, names="city", values="total_users")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No city data yet.")

st.divider()

# --- RECENT DATA ---
st.subheader("Recently Ingested Records")
users_df = get_raw_users()
if not users_df.empty:
    st.dataframe(users_df, use_container_width=True)
else:
    st.info("No data yet.")

st.divider()

# --- RUN LOG ---
st.subheader("Pipeline Run Log")
if not run_df.empty:
    st.dataframe(run_df, use_container_width=True)
else:
    st.info("No runs yet.")