import csv

import pytest

from pipeline import quality


@pytest.mark.parametrize("row, reason", [
    (["Asha", "30", "Delhi"], None),
    (["", "30", "Delhi"], "name is empty"),
    (["Asha", "abc", "Delhi"], "age is not a whole number: 'abc'"),
    (["Asha", "150", "Delhi"], "age out of range 0-120: 150"),
    (["Asha", "30", " "], "city is empty"),
    (["Asha"], "age is not a whole number: ''"),
    (["Asha", "²", "Delhi"], "age is not a whole number: '²'"),
    (["Asha", "9" * 5000, "Delhi"], "age out of range 0-120: 9999999999"),
])
def test_check_row(row, reason):
    assert quality.check_row(row) == reason


def write_valid(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "age", "city"])
        writer.writerows(rows)
    return path


def test_split_rows_sets_bad_rows_aside(tmp_path):
    a = write_valid(tmp_path / "a.csv", [["Asha", "30", "Delhi"], ["", "41", "Pune"]])
    b = write_valid(tmp_path / "b.csv", [["Ravi", "150", "Goa"], ["Sara", "22", "Jaipur"]])
    good, rejected = quality.split_rows([a, b])
    assert good == [("a.csv", ["Asha", "30", "Delhi"]), ("b.csv", ["Sara", "22", "Jaipur"])]
    assert [(r.source_file, r.reason) for r in rejected] == [
        ("a.csv", "name is empty"),
        ("b.csv", "age out of range 0-120: 150"),
    ]


def test_good_rows_round_trip(tmp_path):
    rows = [("a.csv", ["Asha", "30", "Delhi"])]
    quality.write_good(rows, tmp_path / "good.csv")
    assert quality.read_good(tmp_path / "good.csv") == rows


def test_rejected_rows_round_trip(tmp_path):
    rows = [quality.Rejected("a.csv", ["", "41", "Pune"], "name is empty")]
    quality.write_rejected(rows, tmp_path / "rejected.csv")
    assert quality.read_rejected(tmp_path / "rejected.csv") == rows
