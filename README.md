# Self-Healing Data Pipeline with LLM Schema Management

A production-grade data pipeline that automatically detects and fixes schema drift using a locally hosted LLM (LLaMA 3.2), eliminating manual intervention for common data quality failures.

---

## What it does

When upstream data changes its structure unexpectedly (schema drift), most pipelines crash and wait for a human fix. This pipeline:

1. Detects the schema drift
2. Sends the error to a local LLM for diagnosis
3. Applies the AI-generated fix automatically
4. Retries the pipeline without human intervention
5. Logs every AI decision for auditability

---

## Architecture

```
Chaos Generator (simulated upstream)
        ↓
Airflow DAG
        ↓
validate_data → detects schema drift
        ↓ (if drift)
LLM Healer (LLaMA 3.2 via Ollama)
        ↓
Fix applied → re-validate
        ↓
ingest_data → PostgreSQL
        ↓
dbt transformations (Silver + Gold layers)
        ↓
Streamlit Dashboard (real-time observability)
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 2.9 | Pipeline orchestration |
| PostgreSQL 15 | Data storage |
| dbt | SQL transformations |
| Ollama + LLaMA 3.2 | Local LLM for diagnosis |
| Streamlit | Observability dashboard |
| Docker Compose | Infrastructure |
| pytest | Testing |

---

## Data Architecture (Medallion Pattern)

```
Bronze → raw_users          (raw ingested data, never modified)
Silver → cleaned_users      (validated, capitalisation fixed)
Gold   → city_summary       (aggregated, business ready)
```

---

## Key Features

**Self-Healing Engine**
- Detects schema drift (wrong headers, wrong delimiter, extra columns)
- Sends structured prompt to locally hosted LLaMA 3.2
- Parses and validates LLM JSON response
- Rewrites CSV with correct schema
- Re-validates before continuing

**Data Quality Layer**
- Header validation
- Data type validation (age must be numeric)
- Range validation (age 0-120)
- Null checks (name cannot be empty)
- dbt incremental models with 2-hour lookback window

**Observability Dashboard**
- Real-time pipeline health metrics
- AI intervention log with full LLM reasoning
- Rows ingested over time chart
- Users by city distribution
- Pipeline run audit log

**Audit Trail**
- Every AI intervention logged to `healing_log` table
- Every pipeline run logged to `pipeline_run_log` table
- Full LLM reasoning stored with each intervention

---

## Database Tables

| Table | Description |
|---|---|
| `raw_users` | Bronze layer — raw ingested records |
| `cleaned_users` | Silver layer — dbt cleaned records |
| `city_summary` | Gold layer — aggregated by city |
| `pipeline_run_log` | Audit log of every pipeline run |
| `healing_log` | Audit log of every AI intervention |

---

## Setup

**Prerequisites**
- Docker Desktop
- Ollama (https://ollama.com)

**Steps**

1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/self-healing-pipeline.git
cd self-healing-pipeline
```

2. Pull the LLM model
```bash
ollama pull llama3.2
ollama serve
```

3. Start the pipeline
```bash
docker-compose up
```

4. Open Airflow at http://localhost:8080 (login: admin)
5. Open Dashboard at http://localhost:8501
6. Trigger the `ingest_pipeline` DAG

---

## Running Tests

```bash
docker-compose exec --user airflow airflow python -m pytest /opt/airflow/tests/test_pipeline.py -v
```

**10/10 tests covering:**
- Normal and chaos data generation
- Schema validation (headers, types, ranges, nulls)
- AI healer (extra columns, pipe delimiter)

---

## Known Limitations & Future Improvements

| Limitation | Planned Fix |
|---|---|
| Healer only fixes schema drift | Extend to handle data type errors and dbt failures |
| Shared CSV file causes race conditions | Use unique filenames per execution date |
| LLM response not Pydantic validated | Add Pydantic schemas for strict response parsing |
| No alerting on failures | Add email/Slack alerts via Airflow callbacks |

---
