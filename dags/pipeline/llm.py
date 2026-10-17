"""Asks LLaMA 3.2, running in Ollama, which file column holds name, age and city."""
import json

import requests

from pipeline import config

PROMPT = """You are a data engineer. A CSV file arrived with column names we do not recognise.

Each file column with its sample values:
{by_column}

Match each expected column to the file column whose sample values hold the same kind of data.
Judge by the values, not only by the column names:
- name: a person's name
- age: a whole number of years
- city: a city name

Reply with JSON only, in exactly this format:
{{"mapping": {{"name": "<file column>", "age": "<file column>", "city": "<file column>"}}, "problem": "<one short sentence, with no quote marks, saying which expected columns were renamed and to what>"}}
"""


def build_prompt(columns: list[str], samples: list[list[str]]) -> str:
    """List each file column with its own sample values, so the model judges by the data, not by position."""
    by_column = "\n".join(
        f"- {json.dumps(column)}: {json.dumps([row[i] if i < len(row) else '' for row in samples])}"
        for i, column in enumerate(columns)
    )
    return PROMPT.format(by_column=by_column)


def ask_ollama(columns: list[str], samples: list[list[str]], timeout: int = 60) -> object:
    """Send the prompt to Ollama and return the AI's JSON answer. Errors say what went wrong, including Ollama's own message."""
    response = requests.post(
        config.ollama_url(),
        json={
            "model": config.ollama_model(),
            "prompt": build_prompt(columns, samples),
            "stream": False,
            "format": "json",
            "options": {"temperature": 0},
        },
        timeout=timeout,
    )
    if not response.ok:
        raise RuntimeError(f"Ollama returned HTTP {response.status_code}: {response.text[:300]}")
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Ollama reply is not JSON: {response.text[:300]!r}") from exc
    if not isinstance(body, dict) or "response" not in body:
        raise RuntimeError(f"Ollama reply has no 'response' field: {str(body)[:300]}")
    text = body["response"]
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI did not reply with valid JSON: {text[:300]!r}") from exc
