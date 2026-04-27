import requests
import json
import psycopg2
import csv

DB_CONN = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

OLLAMA_URL = "http://host.docker.internal:11434/api/generate"

def ask_llm(error_message, current_headers):
    prompt = f"""
You are a data engineer. A pipeline failed with this error:

ERROR: {error_message}

The CSV file has these headers: {current_headers}
The expected headers are: ['name', 'age', 'city']

Respond ONLY with a JSON object, nothing else. No explanation.
Format:
{{
    "problem": "one line description",
    "fix": "rename/reorder/remove",
    "expected_headers": ["name", "age", "city"]
}}
"""
    response = requests.post(OLLAMA_URL, json={
        "model": "llama3.2",
        "prompt": prompt,
        "stream": False
    }, timeout=60)

    raw = response.json()["response"]
    start = raw.find("{")
    end = raw.rfind("}") + 1
    return json.loads(raw[start:end])

def log_intervention(error, llm_response, success):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS healing_log (
            id SERIAL PRIMARY KEY,
            error_message TEXT,
            llm_diagnosis TEXT,
            success BOOLEAN,
            healed_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        INSERT INTO healing_log (error_message, llm_diagnosis, success)
        VALUES (%s, %s, %s)
    """, (error, json.dumps(llm_response), success))

    conn.commit()
    cur.close()
    conn.close()

def heal(error_message, data_path):
    with open(data_path, "r") as f:
        reader = csv.reader(f)
        current_headers = next(reader)
        rows = list(reader)

    print(f"Asking LLM to diagnose: {error_message}")

    try:
        diagnosis = ask_llm(error_message, current_headers)
        print(f"LLM diagnosis: {diagnosis}")

        fixed_headers = diagnosis["expected_headers"]

        with open(data_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(fixed_headers)
            for row in rows:
                # handle pipe-delimited rows
                if len(row) == 1 and "|" in row[0]:
                    row = row[0].split("|")
                writer.writerow(row[:len(fixed_headers)])

        log_intervention(error_message, diagnosis, True)
        print("Fix applied successfully!")
        return True

    except Exception as e:
        log_intervention(error_message, {"error": str(e)}, False)
        print(f"Fix failed: {e}")
        return False