# Self-Healing Data Pipeline

An event-driven data pipeline built with Apache Airflow. When incoming CSV files change shape
(schema drift), it repairs them by itself: fixed rules handle the problems it already knows, and a
local LLaMA 3.2 model (through Ollama) maps column names it has never seen. Every repair is checked
and logged in PostgreSQL, and a Streamlit dashboard shows the pipeline's health live.

## How it works

```
supplier_dag (every 2 min) --drops a CSV--> data/incoming/
                                                 |
                                   Airflow Dataset update starts the pipeline
                                                 v
ingest_pipeline:  validate_schema >> quality_checks >> ingest_data >> transform_data
                        |                                  |               |
              rules, then LLaMA 3.2               PostgreSQL, one        dbt models
              for unknown columns                 transaction
```

- `supplier_dag` pretends to be the company that sends the data. Every 2 minutes it drops one
  file. It is not part of the pipeline.
- `ingest_pipeline` starts a few seconds after a file lands (Airflow Datasets), so there is no
  fixed daily schedule.

## The four tasks

| Task | What it does |
|---|---|
| `validate_schema` | Moves the new files into a folder for this run, checks the columns, repairs drift |
| `quality_checks` | Checks every row: name not empty, age a whole number from 0 to 120, city not empty, name and city at most 100 characters. Bad rows are set aside with a reason |
| `ingest_data` | Saves good rows to `raw_users` and bad rows to `rejected_rows` in one transaction. A file that was already loaded is never loaded again |
| `transform_data` | Runs dbt: `cleaned_users` and `city_summary` |

Every task retries twice before the run is marked failed.

## How healing works

The supplier sends 5 kinds of file: clean, pipes plus an extra column, columns in a different
order, renamed columns, and files with bad rows.

1. Find the separator (comma, pipe, semicolon or tab) and read the column names.
2. If `name`, `age` and `city` are all there, rules map them by name. This handles a different
   separator, extra columns and a different order. The AI is not called.
3. If any is missing, LLaMA 3.2 gets each column name with its first two values. It answers in
   JSON with a mapping, for example `{"name": "full_name", "age": "years", "city": "town"}`, and
   one sentence on what was renamed.
4. The answer is checked before it is used: all three columns are mapped, every mapped column
   exists in the file and appears once, no column is used twice, columns that already have the
   right name stay where they are, and at least half of the values in the age column are whole
   numbers from 0 to 120. If a check fails, the run fails and nothing from it is loaded.
5. Every repair attempt is saved to `healing_log`: what drifted, rules or AI, the mapping, the AI's
   full answer and the result.

The AI only chooses which column is which. It never changes a value.

## Tables

| Table | Holds |
|---|---|
| `raw_users` | Every good row as received (after column repair) |
| `cleaned_users` | dbt model: names and cities capitalised, invalid rows filtered out |
| `city_summary` | dbt model: number of users and average age per city |
| `rejected_rows` | Rows that failed the quality checks, with the reason |
| `healing_log` | Every repair attempt |
| `pipeline_run_log` | One row per pipeline run: status (`success`, `failed`, or `no_data` when no new file was waiting), counts, and for a failed run the task and error |

## Fault tolerance

- Tasks run in a fixed order and retry twice.
- Each run moves its files into its own folder (`data/processing/<run>/`), so runs never share a file.
- Loading is one transaction: a crash leaves no half-loaded data. A file is never loaded twice.
- A bad row is set aside instead of failing the whole run.
- A failed run keeps its files in `data/failed/<run>/` and is logged with the failed task and the
  error. Clearing the run in Airflow retries the same files. Archived runs are never deleted.

## Dashboard

http://localhost:8501 refreshes every 5 seconds. It shows the last run's status, runs over time
(failed runs in red), failed runs with the reason, repairs by rules vs AI with each mapping,
rejected rows and users by city.

## Run it

Needs Docker Desktop and Ollama.

```bash
ollama pull llama3.2
cp .env.example .env        # then set your own password in .env
docker compose up -d
```

About `.env`:
- Use only letters, digits, `-` and `_` in the password (it goes into a database URL).
- Keep the file UTF-8. `cp` or `Copy-Item` is fine; `>` in Windows PowerShell 5.1 writes UTF-16.
- The database keeps the password it was created with. To change it later, run
  `docker compose down -v`, which also deletes the stored data.

Then:
- Airflow: http://localhost:8080 (user `admin`; the password is printed in `docker compose logs airflow`)
- Dashboard: http://localhost:8501

Both DAGs start unpaused, so a file arrives every 2 minutes. To show one case on demand:

```bash
docker compose exec airflow airflow dags trigger supplier_dag --conf '{"kind": "renamed"}'
```

Kinds: `clean`, `pipe_extra_column`, `reordered`, `renamed`, `bad_rows`. Pause `supplier_dag` in
Airflow to stop new files. To retry a failed run, clear the whole run in Airflow.

## Tests

```bash
python -m pytest tests/unit                               # no services needed (Python 3.10+, pytest, requests)
docker compose exec airflow python -m pytest -v           # everything, inside the container
```

| Group | Covers |
|---|---|
| `tests/unit` | settings, supplier, run folders, schema, healer (with a fake AI, including bad answers), quality checks, Ollama client |
| `tests/integration/test_healer_live.py` | real LLaMA 3.2 maps renamed columns (skipped if Ollama is down) |
| `tests/integration/test_ingest.py` | one-transaction loading, no double loads, run log, upgrade of an older database (each test in its own throwaway schema; skipped without Postgres) |
| `tests/integration/test_dags.py` | 4 tasks linked in order, retries, callbacks, Dataset trigger (skipped without Airflow) |

## Limits

- Near real-time, not streaming: a file is handled a few seconds after it lands.
- Here the Dataset is updated by the `supplier_dag` task that drops the file. With a real outside
  supplier, the pipeline would need a sensor, a cloud storage event or a call to Airflow's REST
  API to start.
- Demo-size data (3 to 5 rows per file) on one machine (LocalExecutor).
- If the name and city columns are both renamed, the checks cannot prove which is which, because
  both hold text. The AI is asked to judge by the values, and its answer is logged for review.
- Files are matched by name, so a new file with the name of one already loaded is skipped.
