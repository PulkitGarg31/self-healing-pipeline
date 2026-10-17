from datetime import datetime, timedelta, timezone

import pytest

import view_data
from pipeline import files as pipeline_files
from view_data import FileStory

NOW = datetime(2026, 10, 17, 15, 0, 0, tzinfo=timezone.utc)
RUN = "manual__2026-10-17T14:31:30+00:00"


def story(**overrides):
    values = dict(run_id="run1", source_file="a.csv", seen_at=NOW - timedelta(seconds=30),
                  rows_loaded=3, rows_set_aside=0)
    values.update(overrides)
    return FileStory(**values)


def make_run(data_dir, area, folder_name, source_file, raw="name,age,city\nAsha,30,Delhi\n", valid=None):
    folder = data_dir / area / folder_name
    (folder / "raw").mkdir(parents=True)
    (folder / "raw" / source_file).write_text(raw, encoding="utf-8")
    if valid is not None:
        (folder / "valid").mkdir()
        (folder / "valid" / source_file).write_text(valid, encoding="utf-8")
    return folder


@pytest.mark.parametrize("drift, aside, expected", [
    (None, 0, "Clean file"),
    (None, 3, "Some bad rows"),
    ("separator '|'; extra columns: unknown_column", 0, "Different separator, extra columns"),
    ("column order: city, name, age", 0, "Columns reordered"),
    ("missing columns: name, age, city; unknown columns: full_name, years, town", 0, "Columns renamed"),
    ("unreadable file", 0, "Unreadable file"),
    ("duplicate columns: name", 0, "Repeated columns"),
])
def test_what_arrived(drift, aside, expected):
    assert view_data.what_arrived(drift, aside) == expected


@pytest.mark.parametrize("overrides, expected", [
    ({}, ("No repair needed", "ok")),
    ({"rows_set_aside": 1}, ("1 row set aside", "warn")),
    ({"rows_set_aside": 3}, ("3 rows set aside", "warn")),
    ({"drift_type": "column order: city, name, age", "method": "rules", "repaired_ok": True},
     ("Fixed by rules", "ok")),
    ({"drift_type": "missing columns: name", "method": "llm", "repaired_ok": True}, ("Fixed by AI", "ai")),
    ({"drift_type": "missing columns: name", "method": "llm", "repaired_ok": False, "rows_loaded": 0},
     ("Could not repair", "bad")),
    ({"drift_type": "missing columns: name", "method": "llm", "repaired_ok": False, "rows_loaded": 2},
     ("Fixed by hand", "ok")),
    ({"drift_type": "column order: city, name, age", "method": "rules", "repaired_ok": True, "rows_loaded": 0},
     ("Repaired, not loaded", "warn")),
    ({"drift_type": "missing columns: name", "method": "llm", "repaired_ok": True, "rows_loaded": 0},
     ("Repaired, not loaded", "warn")),
])
def test_result(overrides, expected):
    assert view_data.result(story(**overrides)) == expected


@pytest.mark.parametrize("delta, expected", [
    (timedelta(seconds=2), "just now"),
    (timedelta(seconds=40), "40 seconds ago"),
    (timedelta(minutes=2, seconds=5), "2 min ago"),
    (timedelta(hours=3), "3 h ago"),
    (timedelta(days=1, hours=2), "1 day ago"),
    (timedelta(days=4), "4 days ago"),
    (timedelta(seconds=-30), "just now"),
])
def test_time_ago(delta, expected):
    assert view_data.time_ago(NOW - delta, NOW) == expected


def test_time_ago_treats_naive_times_as_utc():
    naive = (NOW - timedelta(minutes=5)).replace(tzinfo=None)
    assert view_data.time_ago(naive, NOW) == "5 min ago"


def test_time_ago_without_a_time():
    assert view_data.time_ago(None, NOW) == "at an unknown time"


@pytest.mark.parametrize("message, expected", [
    (None, "No reason was recorded."),
    ("ValueError: AI used the same column twice", "AI used the same column twice"),
    ("Could not repair: m.csv (ValueError: AI mapped 'age' to 'size', but most of its values are not ages (0-120))",
     "AI mapped 'age' to 'size', but most of its values are not ages (0-120)"),
    ("RuntimeError: dbt failed", "dbt failed"),
    ("task failed without an error message (killed or marked failed outside the task)",
     "task failed without an error message (killed or marked failed outside the task)"),
    ("Could not repair: a.csv (ValueError: x); b.csv (ValueError: y)", "x; y"),
    ("Could not repair: d.csv (the file repeats a column name, so it is not clear which one to use)",
     "the file repeats a column name, so it is not clear which one to use"),
    ("Could not repair: a.csv (ValueError: x); d.csv (the file repeats a column name, so it is not clear which one to use)",
     "x; the file repeats a column name, so it is not clear which one to use"),
    ("Could not repair: m.csv (ValueError: most of its values are not ages (0-120)); d.csv (repeated column)",
     "most of its values are not ages (0-120); repeated column"),
])
def test_friendly_error(message, expected):
    assert view_data.friendly_error(message) == expected


@pytest.mark.parametrize("task, expected", [
    ("validate_schema", "the column check"),
    ("quality_checks", "the row checks"),
    ("ingest_data", "loading"),
    ("transform_data", "dbt"),
    (None, "an unknown step"),
])
def test_task_label(task, expected):
    assert view_data.task_label(task) == expected


def test_ai_summary():
    mapping = '{"name": "full_name", "age": "years", "city": "town"}'
    assert view_data.ai_summary(mapping) == "full_name is name, years is age, town is city"


@pytest.mark.parametrize("value", [None, "", "not json", "[1, 2]"])
def test_ai_summary_without_a_mapping(value):
    assert view_data.ai_summary(value) is None


def test_ai_said():
    assert view_data.ai_said('{"mapping": {}, "problem": " Columns were renamed. "}') == "Columns were renamed."


@pytest.mark.parametrize("value", [None, "nope", '{"mapping": {}}', '"just text"', '{"problem": ""}'])
def test_ai_said_without_a_problem(value):
    assert view_data.ai_said(value) is None


@pytest.mark.parametrize("value, expected", [
    ('["Asha", "abc", "Delhi"]', "Asha, abc, Delhi"),
    ('["", "30", "Pune"]', "(empty), 30, Pune"),
    ("[]", "(empty row)"),
    (None, "(empty row)"),
    ("not json", "not json"),
])
def test_row_text(value, expected):
    assert view_data.row_text(value) == expected


@pytest.mark.parametrize("run_id", [RUN, "dataset_triggered__2026-10-17T11:20:00+00:00", ".", "..", "a b/c"])
def test_run_folder_name_matches_the_pipeline(run_id):
    assert view_data.run_folder_name(run_id) == pipeline_files.run_folder_name(run_id)


def test_find_run_folder_in_processed(tmp_path):
    folder = make_run(tmp_path, "processed", view_data.run_folder_name(RUN), "a.csv")
    assert view_data.find_run_folder(tmp_path, RUN, "a.csv") == folder


def test_find_run_folder_skips_folders_without_the_file(tmp_path):
    make_run(tmp_path, "failed", view_data.run_folder_name(RUN), "other.csv")
    folder = make_run(tmp_path, "processed", view_data.run_folder_name(RUN) + "__2", "a.csv")
    assert view_data.find_run_folder(tmp_path, RUN, "a.csv") == folder


def test_find_run_folder_ignores_similar_names(tmp_path):
    make_run(tmp_path, "processed", view_data.run_folder_name(RUN) + "x", "a.csv")
    assert view_data.find_run_folder(tmp_path, RUN, "a.csv") is None


def test_find_run_folder_without_a_data_folder(tmp_path):
    assert view_data.find_run_folder(tmp_path / "missing", RUN, "a.csv") is None


def test_raw_preview_shows_lines_as_they_arrived(tmp_path):
    raw = "\ufeffname|age|city|unknown_column\nAsha|30|Delhi|x\n\nRavi|41|Pune|y\nSara|22|Goa|z\nMore|1|X|y\n"
    folder = make_run(tmp_path, "processed", "r", "a.csv", raw=raw)
    assert view_data.raw_preview(folder, "a.csv") == [
        "name|age|city|unknown_column", "Asha|30|Delhi|x", "Ravi|41|Pune|y", "Sara|22|Goa|z"]


def test_raw_preview_cuts_long_lines(tmp_path):
    folder = make_run(tmp_path, "processed", "r", "a.csv", raw="x" * 500 + "\n")
    assert view_data.raw_preview(folder, "a.csv") == ["x" * 120]


def test_raw_preview_of_a_missing_file(tmp_path):
    assert view_data.raw_preview(tmp_path, "nope.csv") == []


def test_repaired_rows(tmp_path):
    valid = "name,age,city\nA,1,X\nB,2,Y\nC,3,Z\nD,4,W\n"
    folder = make_run(tmp_path, "processed", "r", "a.csv", valid=valid)
    assert view_data.repaired_rows(folder, "a.csv") == [
        ["name", "age", "city"], ["A", "1", "X"], ["B", "2", "Y"], ["C", "3", "Z"]]


def test_repaired_rows_of_a_missing_file(tmp_path):
    folder = make_run(tmp_path, "processed", "r", "a.csv")
    assert view_data.repaired_rows(folder, "a.csv") == []


def test_after_rows_shows_only_this_files_loaded_rows(tmp_path):
    folder = make_run(tmp_path, "processed", "r", "a.csv", valid="name,age,city\nV,9,V\n")
    (folder / "good.csv").write_text(
        "source_file,name,age,city\na.csv,A,1,X\nb.csv,Q,9,Q\na.csv,B,2,Y\na.csv,C,3,Z\nb.csv,R,8,R\na.csv,D,4,W\n",
        encoding="utf-8")
    assert view_data.after_rows(folder, "a.csv") == ("What was loaded", [
        ["name", "age", "city"], ["A", "1", "X"], ["B", "2", "Y"], ["C", "3", "Z"]])


def test_after_rows_when_no_rows_of_this_file_were_loaded(tmp_path):
    folder = make_run(tmp_path, "processed", "r", "a.csv", valid="name,age,city\nV,9,V\n")
    (folder / "good.csv").write_text("source_file,name,age,city\nb.csv,Q,9,Q\n", encoding="utf-8")
    assert view_data.after_rows(folder, "a.csv") == ("What was loaded", [])


def test_after_rows_falls_back_to_the_repaired_file(tmp_path):
    folder = make_run(tmp_path, "failed", "r", "a.csv", valid="name,age,city\nA,1,X\n")
    assert view_data.after_rows(folder, "a.csv") == (
        "After repair (not loaded)", [["name", "age", "city"], ["A", "1", "X"]])


def test_after_rows_with_nothing_to_show(tmp_path):
    folder = make_run(tmp_path, "failed", "r", "a.csv")
    assert view_data.after_rows(folder, "a.csv") == ("After repair", [])


def test_journey_for_a_clean_file():
    assert view_data.journey(story(), NOW) == [
        ("Received", "30 seconds ago"),
        ("Checked", "columns were fine"),
        ("Repaired", "not needed"),
        ("Loaded", "3 saved, 0 set aside"),
    ]


def test_journey_for_an_ai_repair():
    steps = view_data.journey(story(drift_type="missing columns: name", method="llm", repaired_ok=True), NOW)
    assert steps[1:3] == [("Checked", "columns renamed"), ("Repaired", "by LLaMA 3.2")]


def test_journey_for_a_failed_repair():
    failed = story(drift_type="missing columns: name", method="llm", repaired_ok=False, rows_loaded=0)
    assert view_data.journey(failed, NOW)[2:] == [("Repaired", "failed"), ("Loaded", "nothing loaded")]


def test_journey_for_a_repair_fixed_by_hand():
    fixed = story(drift_type="missing columns: name", method="llm", repaired_ok=False, rows_loaded=2)
    assert view_data.journey(fixed, NOW)[2] == ("Repaired", "fixed by hand")


def test_stories_from_rows():
    found = view_data.stories_from_rows([
        {"run_id": "r", "source_file": "a.csv", "rows_loaded": 3, "rows_set_aside": 0, "seen_at": NOW,
         "drift_type": None, "method": None, "mapping": None, "llm_diagnosis": None, "success": None,
         "error_message": None},
        {"run_id": "r", "source_file": "b.csv", "rows_loaded": 0, "rows_set_aside": 0, "seen_at": NOW,
         "drift_type": "missing columns: name", "method": "llm", "mapping": None,
         "llm_diagnosis": '{"problem": "x"}', "success": False, "error_message": "ValueError: no"},
    ])
    assert found[0].repaired_ok is None
    assert found[0].key == "r|a.csv"
    assert found[1].repaired_ok is False
    assert found[1].llm_answer == '{"problem": "x"}'


def test_sql_mentions_only_existing_columns():
    for sql in (view_data.RECENT_FILES_SQL, view_data.HEALTH_SQL, view_data.LAST_RUN_SQL,
                view_data.REJECTED_SQL, view_data.CITY_SQL):
        assert "SELECT" in sql
    assert "LIMIT 15" in view_data.RECENT_FILES_SQL


def test_sql_uses_first_sight_and_pipeline_rows_only():
    assert "LEAST(h.first_healed_at, l.first_at, a.first_at) AS seen_at" in view_data.RECENT_FILES_SQL
    assert "FROM rejected_rows WHERE run_id IS NOT NULL) AS rows_set_aside" in view_data.HEALTH_SQL
