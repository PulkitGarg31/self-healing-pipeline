"""Row checks: good rows go on to the database, bad rows are set aside with a reason."""
import csv
import json
from dataclasses import dataclass
from pathlib import Path

from pipeline.schema import is_age

GoodRow = tuple[str, list[str]]  # (source file, [name, age, city])
MAX_TEXT = 100  # raw_users.name and city are VARCHAR(100)


@dataclass
class Rejected:
    source_file: str
    row: list[str]
    reason: str


def check_row(row: list[str]) -> str | None:
    """Return why the row is bad, or None if it is good. Age uses the same rule as the healer."""
    name, age, city = (row + ["", "", ""])[:3]
    if not name.strip():
        return "name is empty"
    if not is_age(age):
        value = age.strip()
        if value.isdecimal() and len(value) > 3:
            return f"age has more than 3 digits: {value[:10]}"
        if value.isdecimal():
            return f"age out of range 0-120: {value}"
        return f"age is not a whole number: {value[:20]!r}"
    if not city.strip():
        return "city is empty"
    if len(name.strip()) > MAX_TEXT:
        return f"name longer than {MAX_TEXT} characters"
    if len(city.strip()) > MAX_TEXT:
        return f"city longer than {MAX_TEXT} characters"
    if "\x00" in name + city:
        return "name or city contains a NUL character"
    return None


def split_rows(valid_files: list[Path]) -> tuple[list[GoodRow], list[Rejected]]:
    good, rejected = [], []
    for path in valid_files:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # header: name,age,city
            for row in reader:
                reason = check_row(row)
                if reason:
                    rejected.append(Rejected(path.name, row, reason))
                else:
                    good.append((path.name, [cell.strip() for cell in row[:3]]))
    return good, rejected


def write_good(rows: list[GoodRow], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_file", "name", "age", "city"])
        for source_file, values in rows:
            writer.writerow([source_file, *values])


def read_good(path: Path) -> list[GoodRow]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return [(row[0], row[1:4]) for row in reader]


def write_rejected(rows: list[Rejected], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_file", "row", "reason"])
        for item in rows:
            stored = json.dumps([cell[:200] for cell in item.row], ensure_ascii=False)
            writer.writerow([item.source_file, stored, item.reason])


def read_rejected(path: Path) -> list[Rejected]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return [Rejected(row[0], json.loads(row[1]), row[2]) for row in reader]
