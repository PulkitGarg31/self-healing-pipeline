"""Plain-language facts for the dashboard, built from the pipeline's tables and run folders.

No Streamlit, pandas or database-driver imports here, so this file can be unit-tested anywhere.
"""
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

RECENT_FILES = 15

# One row per file: its latest repair attempt, how many rows were saved or set aside, and when it
# was first seen. A retry repairs the same file again, so only the latest attempt counts, but the
# first attempt's time is kept as the time the file was seen.
RECENT_FILES_SQL = f"""
WITH heal AS (
    SELECT DISTINCT ON (run_id, source_file)
           run_id, source_file, healed_at, drift_type, method, mapping, llm_diagnosis,
           success, error_message,
           MIN(healed_at) OVER (PARTITION BY run_id, source_file) AS first_healed_at
    FROM healing_log WHERE drift_type IS NOT NULL
    ORDER BY run_id, source_file, healed_at DESC
),
loaded AS (
    SELECT run_id, source_file, COUNT(*) AS n, MIN(ingested_at) AS first_at
    FROM raw_users WHERE run_id IS NOT NULL GROUP BY run_id, source_file
),
aside AS (
    SELECT run_id, source_file, COUNT(*) AS n, MIN(rejected_at) AS first_at
    FROM rejected_rows WHERE run_id IS NOT NULL GROUP BY run_id, source_file
),
files AS (
    SELECT run_id, source_file FROM heal
    UNION SELECT run_id, source_file FROM loaded
    UNION SELECT run_id, source_file FROM aside
)
SELECT f.run_id, f.source_file,
       COALESCE(l.n, 0) AS rows_loaded, COALESCE(a.n, 0) AS rows_set_aside,
       LEAST(h.first_healed_at, l.first_at, a.first_at) AS seen_at,
       h.drift_type, h.method, h.mapping, h.llm_diagnosis, h.success, h.error_message
FROM files f
LEFT JOIN heal h ON h.run_id = f.run_id AND h.source_file = f.source_file
LEFT JOIN loaded l ON l.run_id = f.run_id AND l.source_file = f.source_file
LEFT JOIN aside a ON a.run_id = f.run_id AND a.source_file = f.source_file
ORDER BY seen_at DESC NULLS LAST
LIMIT {RECENT_FILES}
"""

HEALTH_SQL = """
WITH heal AS (
    SELECT DISTINCT ON (run_id, source_file) run_id, source_file, method, success
    FROM healing_log WHERE drift_type IS NOT NULL
    ORDER BY run_id, source_file, healed_at DESC
),
saved AS (
    SELECT run_id, source_file FROM raw_users WHERE run_id IS NOT NULL
    UNION SELECT run_id, source_file FROM rejected_rows WHERE run_id IS NOT NULL
)
SELECT
    (SELECT COUNT(*) FROM (SELECT run_id, source_file FROM heal
                           UNION SELECT run_id, source_file FROM saved) f) AS files_handled,
    (SELECT COUNT(*) FROM heal h WHERE NOT h.success AND NOT EXISTS (
        SELECT 1 FROM saved s WHERE s.run_id = h.run_id AND s.source_file = h.source_file)) AS could_not_repair,
    (SELECT COUNT(*) FROM raw_users WHERE run_id IS NOT NULL) AS rows_loaded,
    (SELECT COUNT(*) FROM rejected_rows WHERE run_id IS NOT NULL) AS rows_set_aside,
    (SELECT COUNT(*) FROM heal WHERE success AND method = 'rules') AS fixed_by_rules,
    (SELECT COUNT(*) FROM heal WHERE success AND method = 'llm') AS fixed_by_ai
"""

LAST_RUN_SQL = """
SELECT status, started_at, finished_at, failed_task, error_message
FROM pipeline_run_log WHERE run_id IS NOT NULL AND status <> 'no_data'
ORDER BY started_at DESC NULLS LAST LIMIT 1
"""

REJECTED_SQL = "SELECT rejected_at, row_data, reason FROM rejected_rows ORDER BY rejected_at DESC LIMIT 50"

CITY_SQL = "SELECT city, total_users FROM city_summary ORDER BY total_users DESC"

_ERROR_NAME = re.compile(r"^\w+(?:Error|Exception): ")

# Where one failed file ends and the next begins in "Could not repair: a.csv (reason); b.csv (reason)".
# A reason may hold brackets of any depth, such as "(0-120)" or the nested ones in a requests error.
_NEXT_FILE = re.compile(r"\); (?=[^();]+ \()")

# The requests library's words when Ollama is down, slow or unreachable.
_OLLAMA_DOWN = re.compile(
    r"HTTPConnectionPool|Max retries exceeded|Read timed out|NewConnectionError")
OLLAMA_DOWN = "LLaMA 3.2 (Ollama) did not answer. Is Ollama running?"

TASK_LABELS = {
    "validate_schema": "the column check",
    "quality_checks": "the row checks",
    "ingest_data": "loading",
    "transform_data": "dbt",
}


@dataclass
class FileStory:
    run_id: str
    source_file: str
    seen_at: datetime | None
    rows_loaded: int
    rows_set_aside: int
    drift_type: str | None = None
    method: str | None = None
    mapping: str | None = None  # JSON text
    llm_answer: str | None = None  # JSON text of the AI's answer
    repaired_ok: bool | None = None  # None when the file needed no repair
    error_message: str | None = None

    @property
    def key(self) -> str:
        return f"{self.run_id}|{self.source_file}"


def stories_from_rows(rows: list[dict]) -> list[FileStory]:
    stories = []
    for row in rows:
        drift = row.get("drift_type")
        stories.append(FileStory(
            run_id=row["run_id"],
            source_file=row["source_file"],
            seen_at=row.get("seen_at"),
            rows_loaded=int(row.get("rows_loaded") or 0),
            rows_set_aside=int(row.get("rows_set_aside") or 0),
            drift_type=drift,
            method=row.get("method"),
            mapping=row.get("mapping"),
            llm_answer=row.get("llm_diagnosis"),
            repaired_ok=None if drift is None else bool(row.get("success")),
            error_message=row.get("error_message"),
        ))
    return stories


def what_arrived(drift_type: str | None, rows_set_aside: int = 0) -> str:
    """What was wrong with the file when it arrived, in plain words."""
    if not drift_type:
        return "Some bad rows" if rows_set_aside else "Clean file"
    if drift_type == "unreadable file":
        return "Unreadable file"
    if drift_type.startswith("duplicate columns"):
        return "Repeated columns"
    if "missing columns" in drift_type:
        return "Columns renamed"
    parts = []
    if drift_type.startswith("separator"):
        parts.append("different separator")
    if "extra columns" in drift_type:
        parts.append("extra columns")
    if "column order" in drift_type:
        parts.append("columns reordered")
    text = ", ".join(parts) or drift_type
    return text[0].upper() + text[1:]


def result(story: FileStory, in_progress: bool = False) -> tuple[str, str]:
    """(label, tone) for the file's outcome. Tone is one of ok, ai, warn, bad, busy.

    in_progress is True while the file's run folder is still in data/processing/.
    """
    if in_progress:
        return "In progress", "busy"
    if story.repaired_ok is False:
        if story.rows_loaded or story.rows_set_aside:
            return "Fixed by hand", "ok"
        return "Could not repair", "bad"
    if story.repaired_ok and not (story.rows_loaded or story.rows_set_aside):
        return "Repaired, not loaded", "warn"
    if story.method == "llm":
        return "Fixed by AI", "ai"
    if story.method == "rules":
        return "Fixed by rules", "ok"
    if story.rows_set_aside:
        count = story.rows_set_aside
        return f"{count} row{'' if count == 1 else 's'} set aside", "warn"
    return "No repair needed", "ok"


def journey(story: FileStory, now: datetime | None = None, in_progress: bool = False) -> list[tuple[str, str]]:
    """The four steps of the file story: received, checked, repaired, loaded.

    While the file is in progress, a repair that has not worked yet may still be retried, and
    nothing loaded may only mean not loaded yet.
    """
    if story.drift_type is None:
        checked, repaired = "columns were fine", "not needed"
    else:
        checked = what_arrived(story.drift_type).lower()
        if story.repaired_ok:
            repaired = {"llm": "by LLaMA 3.2", "rules": "by rules"}.get(story.method, "done")
        elif story.rows_loaded or story.rows_set_aside:
            repaired = "fixed by hand"
        elif in_progress:
            repaired = "working on it"
        else:
            repaired = "failed"
    if story.rows_loaded or story.rows_set_aside:
        loaded = f"{story.rows_loaded} saved, {story.rows_set_aside} set aside"
    elif in_progress:
        loaded = "loading…"
    else:
        loaded = "nothing loaded"
    return [
        ("Received", time_ago(story.seen_at, now)),
        ("Checked", checked),
        ("Repaired", repaired),
        ("Loaded", loaded),
    ]


def time_ago(when: datetime | None, now: datetime | None = None) -> str:
    """'just now', '40 seconds ago', '2 min ago', '3 h ago', '4 days ago'. Naive times are UTC."""
    if when is None:
        return "at an unknown time"
    now = now or datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    seconds = int((now - when).total_seconds())
    if seconds < 5:
        return "just now"
    if seconds < 60:
        return f"{seconds} seconds ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} h ago"
    days = hours // 24
    return f"{days} day{'' if days == 1 else 's'} ago"


def friendly_error(message: str | None) -> str:
    """The reason in plain words: drops the 'Could not repair: file (' wrapper and Python error names.

    A run with several failed files gives one reason per file, joined with '; ', whether or not the
    reason starts with an error name. A reason that shows Ollama did not answer becomes one plain
    sentence, given once even if several files hit it.
    """
    if not message:
        return "No reason was recorded."
    text = _ERROR_NAME.sub("", message.strip())
    reasons = [text]
    if text.startswith("Could not repair: "):
        reasons = _file_reasons(text.removeprefix("Could not repair: ")) or reasons
    plain = [_plain_reason(reason) for reason in reasons]
    return "; ".join(dict.fromkeys(plain))[:300]


def _file_reasons(body: str) -> list[str]:
    """The reasons in 'a.csv (reason); b.csv (reason)', or [] if the text has another shape."""
    body = body.rstrip()
    if not body.endswith(")"):
        return []
    reasons = []
    for part in _NEXT_FILE.split(body[:-1]):
        _, bracket, reason = part.partition(" (")
        if not bracket:
            return []
        reasons.append(reason)
    return reasons


def _plain_reason(reason: str) -> str:
    reason = _ERROR_NAME.sub("", reason.strip())
    return OLLAMA_DOWN if _OLLAMA_DOWN.search(reason) else reason


def task_label(task_id: str | None) -> str:
    return TASK_LABELS.get(task_id or "", "an unknown step")


def _json(text: str | None):
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def ai_summary(mapping_json: str | None) -> str | None:
    """'full_name is name, years is age, town is city', or None."""
    mapping = _json(mapping_json)
    if not isinstance(mapping, dict):
        return None
    parts = [f"{mapping[col]} is {col}" for col in ("name", "age", "city") if col in mapping]
    return ", ".join(parts) or None


def row_text(row_json: str | None) -> str:
    """A rejected row as 'Asha, abc, Delhi'. Empty cells show as (empty)."""
    if not row_json:
        return "(empty row)"
    values = _json(row_json)
    if values is None:
        return str(row_json)
    if not isinstance(values, list):
        return str(values)
    return ", ".join(str(v) if str(v) != "" else "(empty)" for v in values) or "(empty row)"


def run_folder_name(run_id: str) -> str:
    """Same rule as pipeline.files.run_folder_name (a unit test keeps them identical)."""
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    return name if name.strip(".") else name.replace(".", "_")


def processing_runs(data_dir: Path) -> set[str]:
    """Names of the run folders in data/processing/: runs the pipeline is still working on."""
    try:
        return {folder.name for folder in (data_dir / "processing").iterdir() if folder.is_dir()}
    except OSError:
        return set()


def find_run_folder(data_dir: Path, run_id: str, source_file: str) -> Path | None:
    """The newest run folder holding this file. Archived copies may end in __2, __3, ...

    The pipeline moves run folders between areas, so a folder that vanishes mid-scan is skipped.
    """
    pattern = re.compile(re.escape(run_folder_name(run_id)) + r"(__\d+)?")
    found = []
    for area in ("processing", "processed", "failed"):
        try:
            for folder in (data_dir / area).iterdir():
                if pattern.fullmatch(folder.name) and (folder / "raw" / source_file).is_file():
                    try:
                        found.append((folder.stat().st_mtime, folder))
                    except OSError:
                        continue
        except OSError:
            continue
    return max(found, key=lambda pair: pair[0])[1] if found else None


def raw_preview(folder: Path, source_file: str, max_lines: int = 4, max_chars: int = 120) -> list[str]:
    """The first non-blank lines of the file exactly as it arrived."""
    try:
        text = (folder / "raw" / source_file).read_text(encoding="utf-8-sig", errors="replace")
    except (OSError, ValueError):
        return []
    lines = [line[:max_chars] for line in text.splitlines() if line.strip()]
    return lines[:max_lines]


def repaired_rows(folder: Path, source_file: str, max_rows: int = 3) -> list[list[str]]:
    """Header plus the first rows of the repaired file, or [] if there is none."""
    try:
        with open(folder / "valid" / source_file, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
    except (OSError, ValueError, csv.Error):
        return []
    return rows[: max_rows + 1]


def after_rows(folder: Path, source_file: str, max_rows: int = 3) -> tuple[str, list[list[str]]]:
    """(caption, header plus rows) for the right side of the file story.

    good.csv holds what was actually loaded, so use it when the run got that far. Otherwise show the
    repaired file and say it was not loaded.
    """
    good = folder / "good.csv"
    if good.is_file():
        rows = []
        try:
            with open(good, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # header: source_file, name, age, city
                for row in reader:
                    if row and row[0] == source_file:
                        rows.append(row[1:4])
                        if len(rows) == max_rows:
                            break
        except (OSError, ValueError, csv.Error):
            rows = []
        return "What was loaded", ([["name", "age", "city"], *rows] if rows else [])
    if (folder / "valid" / source_file).is_file():
        return "After repair (not loaded)", repaired_rows(folder, source_file, max_rows)
    return "After repair", []
