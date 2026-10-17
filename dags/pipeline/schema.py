"""Reads a CSV file, finds how its columns differ from name,age,city, and rewrites it."""
import csv
from dataclasses import dataclass
from pathlib import Path

from pipeline.config import EXPECTED_COLUMNS

SEPARATORS = ",|;\t"


@dataclass
class RawFile:
    separator: str
    header: list[str]  # trimmed and lower case
    rows: list[list[str]]


def clean_name(name: str) -> str:
    return name.strip().lower()


def is_age(value: str) -> bool:
    """A whole number from 0 to 120. The healer and the quality checks both use this rule."""
    value = value.strip()
    return value.isdecimal() and len(value) <= 3 and int(value) <= 120


def detect_separator(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=SEPARATORS).delimiter
    except csv.Error:
        return ","


def read_raw(path: Path) -> RawFile:
    text = path.read_text(encoding="utf-8-sig")
    if not text.strip():
        raise ValueError(f"{path.name} is empty")
    lines = text.splitlines()
    while not lines[0].strip():
        lines.pop(0)
    separator = detect_separator("\n".join(lines[:5]))
    reader = csv.reader(lines, delimiter=separator)
    header = [clean_name(name) for name in next(reader)]
    rows = [row for row in reader if any(cell.strip() for cell in row)]
    return RawFile(separator, header, rows)


def is_clean(raw: RawFile) -> bool:
    return raw.separator == "," and raw.header == EXPECTED_COLUMNS


def describe_drift(raw: RawFile) -> str:
    """One line saying how the file differs from name,age,city."""
    parts = []
    if raw.separator != ",":
        parts.append(f"separator {raw.separator!r}")
    missing = [col for col in EXPECTED_COLUMNS if col not in raw.header]
    extra = [col for col in raw.header if col not in EXPECTED_COLUMNS]
    if missing:
        parts.append("missing columns: " + ", ".join(missing))
        if extra:
            parts.append("unknown columns: " + ", ".join(extra))
    else:
        if extra:
            parts.append("extra columns: " + ", ".join(extra))
        present = [col for col in raw.header if col in EXPECTED_COLUMNS]
        if present != EXPECTED_COLUMNS:
            parts.append("column order: " + ", ".join(present))
    return "; ".join(parts)


def apply_mapping(raw: RawFile, mapping: dict[str, str], out_path: Path) -> None:
    """Write name,age,city with commas, taking each value from its mapped column. Other columns are dropped."""
    positions = [raw.header.index(mapping[col]) for col in EXPECTED_COLUMNS]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_COLUMNS)
        for row in raw.rows:
            writer.writerow([row[pos].strip() if pos < len(row) else "" for pos in positions])
