import csv
import random

import pytest

from pipeline import supplier


def read(path, separator):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f, delimiter=separator))


def test_clean_file(tmp_path):
    path, kind = supplier.write_supplier_file(tmp_path, kind="clean", rng=random.Random(1))
    rows = read(path, ",")
    assert kind == "clean"
    assert rows[0] == ["name", "age", "city"]
    assert 3 <= len(rows) - 1 <= 5


def test_pipe_extra_column_file(tmp_path):
    path, _ = supplier.write_supplier_file(tmp_path, kind="pipe_extra_column", rng=random.Random(1))
    rows = read(path, "|")
    assert rows[0] == ["name", "age", "city", "unknown_column"]
    assert all(row[3] == "extra" for row in rows[1:])


def test_reordered_file_puts_city_first(tmp_path):
    path, _ = supplier.write_supplier_file(tmp_path, kind="reordered", rng=random.Random(1))
    rows = read(path, ",")
    assert rows[0] == ["city", "name", "age"]
    assert all(row[0] in supplier.CITIES for row in rows[1:])


def test_renamed_file_uses_unknown_names(tmp_path):
    path, _ = supplier.write_supplier_file(tmp_path, kind="renamed", rng=random.Random(1))
    header = read(path, ",")[0]
    assert header in supplier.RENAMED_HEADERS
    assert "name" not in header


def test_bad_rows_file_has_three_bad_rows(tmp_path):
    path, _ = supplier.write_supplier_file(tmp_path, kind="bad_rows", rng=random.Random(1))
    rows = read(path, ",")[1:]
    bad = [row for row in rows if row[0] == "" or row[1] in ("abc", "150")]
    assert len(rows) == 5
    assert len(bad) == 3


def test_random_picks_a_known_kind(tmp_path):
    _, kind = supplier.write_supplier_file(tmp_path, rng=random.Random(7))
    assert kind in supplier.KINDS


def test_file_names_never_repeat(tmp_path):
    first, _ = supplier.write_supplier_file(tmp_path, kind="clean")
    second, _ = supplier.write_supplier_file(tmp_path, kind="clean")
    assert first != second


def test_no_temporary_file_is_left(tmp_path):
    supplier.write_supplier_file(tmp_path, kind="clean")
    assert list(tmp_path.glob("*.tmp")) == []


def test_unknown_kind_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="Unknown kind"):
        supplier.write_supplier_file(tmp_path, kind="mystery")
