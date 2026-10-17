import csv

import pytest

from pipeline import schema

IDENTITY = {"name": "name", "age": "age", "city": "city"}


def write(tmp_path, text, name="input.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


@pytest.mark.parametrize("sample, expected", [
    ("name,age,city\nAsha,30,Delhi", ","),
    ("name|age|city|unknown_column\nAsha|30|Delhi|extra", "|"),
    ("name;age;city\nAsha;30;Delhi", ";"),
    ("name\tage\tcity\nAsha\t30\tDelhi", "\t"),
    ("name,age,city\nAsha", ","),
])
def test_detect_separator(sample, expected):
    assert schema.detect_separator(sample) == expected


def test_read_raw_cleans_the_header(tmp_path):
    raw = schema.read_raw(write(tmp_path, " Name ,AGE,City\nAsha,30,Delhi\n"))
    assert raw.header == ["name", "age", "city"]
    assert raw.rows == [["Asha", "30", "Delhi"]]


def test_read_raw_skips_blank_lines(tmp_path):
    raw = schema.read_raw(write(tmp_path, "name,age,city\nAsha,30,Delhi\n\n,,\n"))
    assert raw.rows == [["Asha", "30", "Delhi"]]


def test_empty_file_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="empty"):
        schema.read_raw(write(tmp_path, "  \n"))


def test_clean_file_is_clean(tmp_path):
    assert schema.is_clean(schema.read_raw(write(tmp_path, "name,age,city\nAsha,30,Delhi\n")))


def test_pipe_file_is_not_clean(tmp_path):
    raw = schema.read_raw(write(tmp_path, "name|age|city|unknown_column\nAsha|30|Delhi|extra\n"))
    assert not schema.is_clean(raw)
    assert schema.describe_drift(raw) == "separator '|'; extra columns: unknown_column"


def test_describe_column_order(tmp_path):
    raw = schema.read_raw(write(tmp_path, "city,name,age\nDelhi,Asha,30\n"))
    assert schema.describe_drift(raw) == "column order: city, name, age"


def test_describe_renamed_columns(tmp_path):
    raw = schema.read_raw(write(tmp_path, "full_name,years,town\nAsha,30,Delhi\n"))
    assert schema.describe_drift(raw) == (
        "missing columns: name, age, city; unknown columns: full_name, years, town"
    )


def test_apply_mapping_reorders_and_drops_extra_columns(tmp_path):
    raw = schema.read_raw(write(tmp_path, "city|name|age|note\nDelhi|Asha|30|x\n"))
    out = tmp_path / "out" / "clean.csv"
    schema.apply_mapping(raw, IDENTITY, out)
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_apply_mapping_fills_short_rows_with_empty_values(tmp_path):
    raw = schema.read_raw(write(tmp_path, "name,age,city\nAsha\n"))
    out = tmp_path / "out.csv"
    schema.apply_mapping(raw, IDENTITY, out)
    assert read(out) == [["name", "age", "city"], ["Asha", "", ""]]
