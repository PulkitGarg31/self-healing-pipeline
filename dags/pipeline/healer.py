"""Repairs a file whose columns drifted: rules first, then LLaMA 3.2 for column names the rules do not know."""
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pipeline.config import EXPECTED_COLUMNS
from pipeline.schema import RawFile, apply_mapping, clean_name, describe_drift, is_clean, read_raw

# ask_llm(columns, sample_rows) returns the AI's answer, already parsed from JSON.
AskLLM = Callable[[list[str], list[list[str]]], dict]


@dataclass
class HealResult:
    source_file: str
    healed: bool  # True when the file needed a repair
    success: bool
    method: str | None  # "rules", "llm", or None when nothing was repaired
    drift_type: str
    mapping: dict | None = None
    llm_diagnosis: dict | None = None
    error_message: str | None = None


def rules_mapping(raw: RawFile) -> dict | None:
    """If every expected column is present by name, map each one to itself."""
    if all(col in raw.header for col in EXPECTED_COLUMNS):
        return {col: col for col in EXPECTED_COLUMNS}
    return None


def check_llm_mapping(answer: dict, raw: RawFile) -> dict:
    """Return the AI's mapping after checking it, or raise ValueError saying what is wrong."""
    if not isinstance(answer, dict) or not isinstance(answer.get("mapping"), dict):
        raise ValueError("AI answer has no 'mapping' object")
    mapping = {clean_name(str(key)): clean_name(str(value)) for key, value in answer["mapping"].items()}
    if sorted(mapping) != sorted(EXPECTED_COLUMNS):
        raise ValueError(f"AI mapping must have the keys {EXPECTED_COLUMNS}, got {sorted(mapping)}")
    for col, source in mapping.items():
        if source not in raw.header:
            raise ValueError(f"AI mapped '{col}' to '{source}', which is not a column in the file")
    if len(set(mapping.values())) != len(mapping):
        raise ValueError("AI used the same column twice")
    samples = raw.rows[:2]
    age_pos = raw.header.index(mapping["age"])
    numbers = sum(1 for row in samples if age_pos < len(row) and row[age_pos].strip().isdigit())
    if samples and numbers * 2 < len(samples):
        raise ValueError(f"AI mapped 'age' to '{mapping['age']}', but its sample values are not numbers")
    return mapping


def heal_file(raw_path: Path, out_path: Path, ask_llm: AskLLM) -> HealResult:
    """Write a clean name,age,city copy of raw_path to out_path and report what was done."""
    name = raw_path.name
    try:
        raw = read_raw(raw_path)
    except ValueError as exc:
        return HealResult(name, healed=True, success=False, method=None,
                          drift_type="unreadable file", error_message=str(exc))

    if is_clean(raw):
        apply_mapping(raw, {col: col for col in EXPECTED_COLUMNS}, out_path)
        return HealResult(name, healed=False, success=True, method=None, drift_type="")

    drift = describe_drift(raw)
    mapping = rules_mapping(raw)
    if mapping is not None:
        apply_mapping(raw, mapping, out_path)
        return HealResult(name, healed=True, success=True, method="rules", drift_type=drift, mapping=mapping)

    answer = None
    try:
        answer = ask_llm(raw.header, raw.rows[:2])
        mapping = check_llm_mapping(answer, raw)
    except Exception as exc:  # Ollama down, bad JSON, or a mapping that failed the checks
        return HealResult(name, healed=True, success=False, method="llm", drift_type=drift,
                          llm_diagnosis=answer if isinstance(answer, dict) else None,
                          error_message=str(exc))
    apply_mapping(raw, mapping, out_path)
    return HealResult(name, healed=True, success=True, method="llm", drift_type=drift,
                      mapping=mapping, llm_diagnosis=answer)
