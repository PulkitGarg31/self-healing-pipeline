"""Asks LLaMA 3.2, running in Ollama, which file column holds name, age and city."""
import json

import requests

from pipeline import config

PROMPT = """You are a data engineer. A CSV file arrived with column names we do not recognise.

Columns in the file: {columns}
Sample rows: {samples}

Match each expected column to the file column that holds the same kind of data:
- name: a person's name
- age: a whole number of years
- city: a city name

Reply with JSON only, in exactly this format:
{{"mapping": {{"name": "<file column>", "age": "<file column>", "city": "<file column>"}}, "problem": "<one line saying what changed>"}}
"""


def build_prompt(columns: list[str], samples: list[list[str]]) -> str:
    return PROMPT.format(columns=json.dumps(columns), samples=json.dumps(samples))


def ask_ollama(columns: list[str], samples: list[list[str]], timeout: int = 60) -> dict:
    """Send the prompt to Ollama and return its JSON answer.

    If the reply is not valid JSON, raise ValueError with the start of the reply, so the repair log shows it.
    """
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
    response.raise_for_status()
    text = response.json()["response"]
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI did not reply with valid JSON: {text[:300]!r}") from exc
