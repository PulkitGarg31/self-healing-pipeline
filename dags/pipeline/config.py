"""Settings shared by the DAGs, the pipeline code and the tests."""
import os
from pathlib import Path

EXPECTED_COLUMNS = ["name", "age", "city"]

# Both DAGs use this URI: the supplier marks it updated, the pipeline waits for it.
INCOMING_DATASET_URI = "file:///opt/airflow/data/incoming"


def data_dir() -> Path:
    return Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))


def incoming_dir() -> Path:
    return data_dir() / "incoming"


def ollama_url() -> str:
    return os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")


def ollama_model() -> str:
    return os.environ.get("OLLAMA_MODEL", "llama3.2")


def db_settings() -> dict:
    """Connection settings for psycopg2. The password has no default on purpose."""
    return {
        "host": os.environ.get("PIPELINE_DB_HOST", "postgres"),
        "port": int(os.environ.get("PIPELINE_DB_PORT", "5432")),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }
