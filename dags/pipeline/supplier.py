"""The fake data supplier: writes one CSV file of a chosen kind into a folder."""
import csv
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

KINDS = ["clean", "pipe_extra_column", "reordered", "renamed", "bad_rows"]

NAMES = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Sara", "Vikram", "Ananya"]
CITIES = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata", "Pune", "Jaipur"]

# Column names the rules do not know, so only the AI can map them.
RENAMED_HEADERS = [
    ["full_name", "years", "town"],
    ["customer_name", "age_in_years", "location"],
    ["person", "yrs", "home_city"],
]


def make_rows(rng: random.Random, count: int) -> list[list[str]]:
    return [[rng.choice(NAMES), str(rng.randint(18, 70)), rng.choice(CITIES)] for _ in range(count)]


def build_file(kind: str, rng: random.Random) -> tuple[list[str], list[list[str]], str]:
    """Return (header, rows, separator) for one kind of file."""
    rows = make_rows(rng, rng.randint(3, 5))
    if kind == "clean":
        return ["name", "age", "city"], rows, ","
    if kind == "pipe_extra_column":
        return ["name", "age", "city", "unknown_column"], [row + ["extra"] for row in rows], "|"
    if kind == "reordered":
        return ["city", "name", "age"], [[city, name, age] for name, age, city in rows], ","
    if kind == "renamed":
        return list(rng.choice(RENAMED_HEADERS)), rows, ","
    if kind == "bad_rows":
        bad = [
            [rng.choice(NAMES), "abc", rng.choice(CITIES)],
            ["", "30", rng.choice(CITIES)],
            [rng.choice(NAMES), "150", rng.choice(CITIES)],
        ]
        return ["name", "age", "city"], rows[:2] + bad, ","
    raise ValueError(f"Unknown kind: {kind}")


def write_supplier_file(folder: Path, kind: str = "random", rng: random.Random | None = None) -> tuple[Path, str]:
    """Write one file and return (path, kind). The file appears in one step, never half-written."""
    rng = rng or random.Random()
    if kind == "random":
        kind = rng.choice(KINDS)
    header, rows, separator = build_file(kind, rng)
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = folder / f"supplier_{stamp}_{uuid.uuid4().hex[:6]}_{kind}.csv"
    temp = path.with_suffix(".tmp")
    try:
        with open(temp, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=separator)
            writer.writerow(header)
            writer.writerows(rows)
        temp.replace(path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    return path, kind
