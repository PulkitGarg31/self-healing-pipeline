import sys
import os
import csv
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

DATA_PATH = "/tmp/test_data.csv"

def write_csv(headers, rows, delimiter=","):
    with open(DATA_PATH, "w", newline="") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)

# --- CHAOS GENERATOR TESTS ---

def test_normal_data_generates_correctly():
    from chaos_generator import generate_data
    import chaos_generator
    chaos_generator.OUTPUT_PATH = DATA_PATH
    generate_data(chaos=False)
    with open(DATA_PATH) as f:
        reader = csv.reader(f)
        headers = next(reader)
    assert headers == ["name", "age", "city"]

def test_chaos_data_generates_drift():
    from chaos_generator import generate_data
    import chaos_generator
    chaos_generator.OUTPUT_PATH = DATA_PATH
    generate_data(chaos=True)
    with open(DATA_PATH) as f:
        content = f.read()
    assert "unknown_column" in content or "|" in content

# --- VALIDATION TESTS ---

def test_valid_data_passes_validation():
    write_csv(["name", "age", "city"], [["Alice", "30", "Delhi"]])
    import ingest
    ingest.DATA_PATH = DATA_PATH
    valid, headers, rows = ingest.check_file()
    assert valid == True
    assert len(rows) == 1

def test_wrong_headers_fails_validation():
    write_csv(["name", "age", "city", "extra"], [["Alice", "30", "Delhi", "x"]])
    import ingest
    ingest.DATA_PATH = DATA_PATH
    valid, headers, rows = ingest.check_file()
    assert valid == False

def test_invalid_age_raises_error():
    write_csv(["name", "age", "city"], [["Alice", "abc", "Delhi"]])
    import ingest
    ingest.DATA_PATH = DATA_PATH
    with pytest.raises(ValueError, match="Age must be a number"):
        ingest.check_file()

def test_age_out_of_range_raises_error():
    write_csv(["name", "age", "city"], [["Alice", "999", "Delhi"]])
    import ingest
    ingest.DATA_PATH = DATA_PATH
    with pytest.raises(ValueError, match="Invalid age"):
        ingest.check_file()

def test_empty_name_raises_error():
    write_csv(["name", "age", "city"], [["", "30", "Delhi"]])
    import ingest
    ingest.DATA_PATH = DATA_PATH
    with pytest.raises(ValueError, match="Name cannot be empty"):
        ingest.check_file()

def test_empty_file_raises_error():
    with open(DATA_PATH, "w") as f:
        f.write("")
    import ingest
    ingest.DATA_PATH = DATA_PATH
    with pytest.raises(ValueError, match="empty"):
        ingest.check_file()

# --- HEALER TESTS ---

def test_healer_fixes_extra_column():
    write_csv(["name", "age", "city", "unknown"], [["Alice", "30", "Delhi", "x"]])
    from healer import heal
    result = heal("Schema drift detected! Got: ['name', 'age', 'city', 'unknown']", DATA_PATH)
    assert result == True
    with open(DATA_PATH) as f:
        reader = csv.reader(f)
        headers = next(reader)
    assert headers == ["name", "age", "city"]

def test_healer_fixes_pipe_delimiter():
    with open(DATA_PATH, "w") as f:
        f.write("name|age|city|unknown_column\nAlice|30|Delhi|x\n")
    from healer import heal
    result = heal("Schema drift detected! Got: ['name|age|city|unknown_column']", DATA_PATH)
    assert result == True
    with open(DATA_PATH) as f:
        reader = csv.reader(f)
        headers = next(reader)
    assert headers == ["name", "age", "city"]