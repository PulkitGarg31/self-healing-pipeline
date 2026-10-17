"""Live dashboard for the self-healing pipeline, built for demos.

The sidebar sends test files and switches the automatic supplier on or off. The main area tells
the story of one file at a time: what arrived, what was wrong, how it was repaired, what was loaded.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

import airflow_api
import view_data

st.set_page_config(page_title="Self-Healing Pipeline", layout="wide")

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/data"))
TEST_FILES = [
    ("clean", "Clean file"),
    ("pipe_extra_column", "Pipes and extra column"),
    ("reordered", "Columns reordered"),
    ("renamed", "Columns renamed (AI)"),
    ("bad_rows", "Bad rows"),
]
CHIP_COLOURS = {"ok": "green", "ai": "blue", "warn": "orange", "bad": "red", "busy": "gray"}
HEALTH_LABELS = [
    ("files_handled", "Files handled"),
    ("could_not_repair", "Could not repair"),
    ("rows_loaded", "Rows loaded"),
    ("rows_set_aside", "Rows set aside"),
    ("fixed_by_rules", "Fixed by rules"),
    ("fixed_by_ai", "Fixed by AI"),
]


def connect():
    return psycopg2.connect(
        host=os.environ.get("PIPELINE_DB_HOST", "postgres"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=3,
    )


def rows(sql: str) -> list[dict]:
    """Run a SELECT and return its rows as dicts, or [] if the database or table isn't there yet."""
    try:
        conn = connect()
    except (KeyError, psycopg2.Error) as exc:
        print(f"dashboard: cannot connect: {type(exc).__name__}: {exc}", file=sys.stderr)
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        print(f"dashboard: query failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return []
    finally:
        conn.close()


def db_error() -> str | None:
    """None when the database answers, otherwise the reason it doesn't."""
    try:
        connect().close()
    except KeyError as exc:
        return f"the setting {exc.args[0]} is missing from the environment"
    except psycopg2.Error as exc:
        return str(exc).strip() or type(exc).__name__
    return None


def chip(label: str, tone: str) -> str:
    return f":{CHIP_COLOURS[tone]}-background[{label}]"


def in_progress(story: view_data.FileStory, busy: set[str]) -> bool:
    """True while the story's run folder is still in data/processing/ (busy holds those folder names)."""
    return view_data.run_folder_name(story.run_id) in busy


def pin(key: str) -> None:
    st.session_state["pinned"] = key


def show_latest() -> None:
    st.session_state.pop("pinned", None)


def send(kind: str, label: str) -> None:
    try:
        airflow_api.send_test_file(kind)
        st.session_state["sent_message"] = f"Sent: {label}. It shows up in about 15 seconds."
        st.session_state.pop("pinned", None)  # so the card follows the new file
    except airflow_api.ApiError as exc:
        st.session_state["api_message"] = str(exc)


def switch_automatic_files() -> None:
    wanted = st.session_state["automatic_files"]
    try:
        airflow_api.set_automatic_files(wanted)
    except airflow_api.ApiError as exc:
        st.session_state["automatic_files"] = not wanted
        st.session_state["api_message"] = str(exc)


@st.fragment(run_every=5)
def health_panel() -> None:
    st.subheader("Pipeline health")
    found = rows(view_data.HEALTH_SQL)
    if not found and db_error():
        st.caption("Not available: the database can't be reached.")
        return
    numbers = found[0] if found else {}
    for key, label in HEALTH_LABELS:
        name, value = st.columns([3, 1])
        name.write(label)
        value.write(f"**{numbers.get(key) or 0}**")


def status_line(now: datetime, newest: view_data.FileStory | None, newest_in_progress: bool) -> None:
    """One line on top: newest file in progress, then a failed last run, then a failed newest file."""
    if newest_in_progress:
        st.info("Working on the newest file…")
        return
    found = rows(view_data.LAST_RUN_SQL)
    if not found:
        return
    last = found[0]
    when = view_data.time_ago(last["finished_at"] or last["started_at"], now)
    if last["status"] == "failed":
        st.error(f"The last run failed during {view_data.task_label(last['failed_task'])} ({when}): "
                 f"{view_data.friendly_error(last['error_message'])}")
    elif newest is not None and view_data.result(newest)[1] == "bad":
        st.warning("The newest file could not be repaired. See its story below.")
    elif newest is not None:
        # The run log is written after dbt finishes, so the newest file can be more recent than it.
        st.success(f"Pipeline is healthy. Last file arrived {view_data.time_ago(newest.seen_at, now)}.")
    else:
        st.success(f"Pipeline is healthy. Last file handled {when}.")


def file_card(story: view_data.FileStory, is_latest: bool, now: datetime, working: bool) -> None:
    """working is True while the pipeline is still working on this file."""
    label, tone = view_data.result(story, in_progress=working)
    with st.container(border=True):
        title, result_col = st.columns([3, 1.4])
        title.subheader(("Latest file: " if is_latest else "File: ")
                        + view_data.what_arrived(story.drift_type, story.rows_set_aside))
        result_col.markdown(chip(label, tone))
        st.caption(f"{story.source_file} · arrived {view_data.time_ago(story.seen_at, now)}")

        for column, (step, detail) in zip(st.columns(4), view_data.journey(story, now, in_progress=working)):
            column.markdown(f"**{step}**  \n{detail}")

        folder = view_data.find_run_folder(DATA_DIR, story.run_id, story.source_file)
        before, after = st.columns(2)
        with before:
            hand_fixed = view_data.result(story)[0] == "Fixed by hand"
            st.caption("What arrived (as edited by hand)" if hand_fixed else "What arrived")
            lines = view_data.raw_preview(folder, story.source_file) if folder else []
            st.code("\n".join(lines) if lines else "The original file is not available.", language=None)
        with after:
            caption, table = view_data.after_rows(folder, story.source_file) if folder else ("After repair", [])
            if caption == "What was loaded" and not story.rows_loaded and len(table) > 1:
                caption = "Passed the row checks " + ("(still loading)" if working else "(not loaded)")
            elif caption == "After repair (not loaded)" and working:
                caption = "After repair (still loading)"
            st.caption(caption)
            if len(table) > 1:
                st.dataframe(pd.DataFrame(table[1:], columns=table[0]), hide_index=True,
                             use_container_width=True)
            elif not folder and story.rows_loaded:
                st.write("The repaired file is not available.")
            elif working:
                st.write("Nothing loaded yet.")
            else:
                st.write("Nothing from this file was loaded.")

        if story.method == "llm" and story.repaired_ok:
            # Show the checked mapping, not the AI's free-text sentence, which is often vague or wrong.
            matched = view_data.ai_summary(story.mapping)
            if matched:
                st.info(f"LLaMA 3.2 matched the columns: {matched}. The pipeline checked this before using it.")
            if story.llm_answer:
                with st.expander("Full AI answer"):
                    st.code(story.llm_answer, language="json")
        if story.repaired_ok is False:
            reason = view_data.friendly_error(story.error_message)
            if story.rows_loaded or story.rows_set_aside:
                st.warning(f"The automatic repair failed ({reason}). The file was then fixed by hand and loaded.")
            elif not working:  # while working, Airflow may still retry the repair
                st.error(f"Could not repair: {reason}")
        if not is_latest:
            st.button("Show latest", key="show_latest", on_click=show_latest)


def recent_files(stories: list[view_data.FileStory], shown: view_data.FileStory, now: datetime,
                 busy: set[str]) -> None:
    st.subheader("Recent files")
    for story in stories:
        label, tone = view_data.result(story, in_progress=in_progress(story, busy))
        when, what, result_col, view = st.columns([1.3, 3, 2, 1])
        when.write(view_data.time_ago(story.seen_at, now))
        what.write(view_data.what_arrived(story.drift_type, story.rows_set_aside))
        result_col.markdown(chip(label, tone))
        view.button("View", key=f"view_{story.key}", on_click=pin, args=(story.key,),
                    disabled=story.key == shown.key)


def folded_sections(now: datetime) -> None:
    with st.expander("Rows set aside, and why"):
        aside = rows(view_data.REJECTED_SQL)
        if aside:
            table = [{"When": view_data.time_ago(r["rejected_at"], now),
                      "Row": view_data.row_text(r["row_data"]),
                      "Why": r["reason"]} for r in aside]
            st.dataframe(pd.DataFrame(table), hide_index=True, use_container_width=True)
        else:
            st.write("No rows have been set aside yet.")
    with st.expander("Users by city"):
        cities = rows(view_data.CITY_SQL)
        if cities:
            st.plotly_chart(px.pie(pd.DataFrame(cities), names="city", values="total_users"),
                            use_container_width=True)
        else:
            st.write("No city data yet.")


@st.fragment(run_every=5)
def main_view() -> None:
    now = datetime.now(timezone.utc)
    error = db_error()
    if error:
        st.error(f"Can't reach the database: {error}")
        return
    stories = view_data.stories_from_rows(rows(view_data.RECENT_FILES_SQL))
    busy = view_data.processing_runs(DATA_DIR)
    newest = stories[0] if stories else None
    status_line(now, newest, newest is not None and in_progress(newest, busy))
    if not stories:
        st.info("Waiting for the first file. Press a button on the left to send one.")
        return
    pinned = st.session_state.get("pinned")
    shown = next((s for s in stories if s.key == pinned), stories[0])
    file_card(shown, is_latest=shown is stories[0], now=now, working=in_progress(shown, busy))
    recent_files(stories, shown, now, busy)
    folded_sections(now)
    st.caption(f"Updated {now:%H:%M:%S} UTC. Refreshes every 5 seconds.")


with st.sidebar:
    st.header("Send a test file")
    for kind, label in TEST_FILES:
        st.button(label, key=f"send_{kind}", on_click=send, args=(kind, label), use_container_width=True)
    automatic = airflow_api.automatic_files_on()
    if automatic is None:
        st.caption("Automatic files: Airflow is not reachable.")
    else:
        st.session_state["automatic_files"] = automatic  # Airflow's real state, so the switch never drifts
        st.toggle("Automatic file every 2 minutes", key="automatic_files", on_change=switch_automatic_files)
    if "api_message" in st.session_state:
        st.error(st.session_state.pop("api_message"))
    st.divider()
    health_panel()

if "sent_message" in st.session_state:
    st.toast(st.session_state.pop("sent_message"))

st.title("Self-Healing Pipeline")
st.caption("CSV files arrive with problems. The pipeline checks their columns, repairs them with rules "
           "or LLaMA 3.2, sets bad rows aside, and loads the rest into PostgreSQL.")
main_view()
