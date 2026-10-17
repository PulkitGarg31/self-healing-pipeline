import csv

import pytest

from pipeline import healer

GOOD_ANSWER = {
    "mapping": {"name": "full_name", "age": "years", "city": "town"},
    "problem": "columns were renamed",
}
RENAMED = "full_name,years,town\nAsha,30,Delhi\nRavi,41,Pune\nSara,22,Goa\n"


def write(tmp_path, text):
    path = tmp_path / "input.csv"
    path.write_text(text, encoding="utf-8")
    return path


def read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


class FakeLLM:
    """Stands in for Ollama: records each call and returns a fixed answer or raises an error."""

    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    def __call__(self, columns, samples):
        self.calls.append((columns, samples))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def test_clean_file_needs_no_repair(tmp_path):
    llm = FakeLLM(GOOD_ANSWER)
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "name,age,city\nAsha,30,Delhi\n"), out, llm)
    assert result.healed is False
    assert result.success is True
    assert llm.calls == []
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_rules_fix_pipes_and_extra_column(tmp_path):
    llm = FakeLLM(GOOD_ANSWER)
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "name|age|city|unknown_column\nAsha|30|Delhi|extra\n"), out, llm)
    assert result.success is True
    assert result.method == "rules"
    assert result.drift_type == "separator '|'; extra columns: unknown_column"
    assert llm.calls == []
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_rules_fix_column_order(tmp_path):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "city,name,age\nDelhi,Asha,30\n"), out, FakeLLM(GOOD_ANSWER))
    assert result.method == "rules"
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_ai_maps_renamed_columns(tmp_path):
    llm = FakeLLM(GOOD_ANSWER)
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, RENAMED), out, llm)
    assert result.success is True
    assert result.method == "llm"
    assert result.mapping == {"name": "full_name", "age": "years", "city": "town"}
    assert result.llm_diagnosis == GOOD_ANSWER
    assert llm.calls == [(["full_name", "years", "town"], [["Asha", "30", "Delhi"], ["Ravi", "41", "Pune"]])]
    assert read(out)[1] == ["Asha", "30", "Delhi"]


def test_ai_answer_in_capital_letters_is_accepted(tmp_path):
    answer = {"mapping": {"Name": "Full_Name", "AGE": "Years", "city": " town "}}
    result = healer.heal_file(write(tmp_path, RENAMED), tmp_path / "out.csv", FakeLLM(answer))
    assert result.success is True
    assert result.mapping == {"name": "full_name", "age": "years", "city": "town"}


@pytest.mark.parametrize("answer, message", [
    ({"problem": "no mapping here"}, "no 'mapping'"),
    ({"mapping": {"name": "full_name", "age": "years"}}, "must have the keys"),
    ({"mapping": {"name": "full_name", "age": "years", "city": "country"}}, "not a column"),
    ({"mapping": {"name": "full_name", "age": "years", "city": "full_name"}}, "same column twice"),
    ({"mapping": {"name": "full_name", "age": "town", "city": "years"}}, "not ages"),
    ({"mapping": {"name": "full_name", "NAME ": "town", "age": "years", "city": "full_name"}}, "repeats a key"),
])
def test_bad_ai_answers_are_rejected(tmp_path, answer, message):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, RENAMED), out, FakeLLM(answer))
    assert result.success is False
    assert message in result.error_message
    assert not out.exists()


def test_ollama_error_fails_safely(tmp_path):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, RENAMED), out, FakeLLM(ConnectionError("Ollama is down")))
    assert result.success is False
    assert result.method == "llm"
    assert "Ollama is down" in result.error_message
    assert not out.exists()


def test_empty_file_fails_safely(tmp_path):
    result = healer.heal_file(write(tmp_path, ""), tmp_path / "out.csv", FakeLLM(GOOD_ANSWER))
    assert result.success is False
    assert result.drift_type == "unreadable file"
    assert "empty" in result.error_message


def test_ai_cannot_move_a_column_that_kept_its_name(tmp_path):
    text = "name,age,town\nAsha,30,Delhi\nRavi,41,Pune\n"
    out = tmp_path / "out.csv"
    swap = {"mapping": {"name": "town", "age": "age", "city": "name"}}
    result = healer.heal_file(write(tmp_path, text), out, FakeLLM(swap))
    assert result.success is False
    assert "already has a 'name' column" in result.error_message
    assert not out.exists()

    right = {"mapping": {"name": "name", "age": "age", "city": "town"}}
    result = healer.heal_file(write(tmp_path, text), out, FakeLLM(right))
    assert result.success is True
    assert result.method == "llm"
    assert read(out)[1] == ["Asha", "30", "Delhi"]


def test_header_only_file_cannot_check_the_ai(tmp_path):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "full_name,years,town\n"), out, FakeLLM(GOOD_ANSWER))
    assert result.success is False
    assert "no data rows" in result.error_message
    assert not out.exists()


def test_failed_repair_removes_an_old_output(tmp_path):
    out = tmp_path / "out.csv"
    out.write_text("old", encoding="utf-8")
    result = healer.heal_file(write(tmp_path, RENAMED), out, FakeLLM(ConnectionError()))
    assert result.success is False
    assert result.error_message.startswith("ConnectionError")
    assert not out.exists()


def test_duplicate_columns_fail_without_asking_the_ai(tmp_path):
    llm = FakeLLM(GOOD_ANSWER)
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "name,age,city,name\nAsha,30,Delhi,X\n"), out, llm)
    assert result.success is False
    assert result.drift_type == "duplicate columns: name"
    assert llm.calls == []
    assert not out.exists()


def test_rules_ignore_empty_spare_columns(tmp_path):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "name,age,city,,\nAsha,30,Delhi,,\n"), out, FakeLLM(GOOD_ANSWER))
    assert result.success is True
    assert result.method == "rules"
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_rules_ignore_an_unused_repeated_column(tmp_path):
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, "name,age,city,note,note\nAsha,30,Delhi,a,b\n"), out, FakeLLM(GOOD_ANSWER))
    assert result.success is True
    assert result.method == "rules"
    assert read(out) == [["name", "age", "city"], ["Asha", "30", "Delhi"]]


def test_ai_cannot_use_a_repeated_column(tmp_path):
    out = tmp_path / "out.csv"
    text = "full_name,years,town,town\nAsha,30,Delhi,Goa\n"
    answer = {"mapping": {"name": "full_name", "age": "years", "city": "town"}}
    result = healer.heal_file(write(tmp_path, text), out, FakeLLM(answer))
    assert result.success is False
    assert "appears more than once" in result.error_message
    assert not out.exists()


def test_non_dict_ai_answer_is_kept_for_the_log(tmp_path):
    result = healer.heal_file(write(tmp_path, RENAMED), tmp_path / "out.csv", FakeLLM(["oops"]))
    assert result.success is False
    assert result.llm_diagnosis == ["oops"]


def test_broken_csv_is_unreadable(tmp_path):
    # An unclosed quote makes one field longer than the csv module's field limit.
    text = 'name,age,city\n"' + "x" * 140000
    result = healer.heal_file(write(tmp_path, text), tmp_path / "out.csv", FakeLLM(GOOD_ANSWER))
    assert result.success is False
    assert result.drift_type == "unreadable file"


def test_age_column_with_mostly_non_ages_is_rejected(tmp_path):
    text = "full_name,years,town\nAsha,5,Delhi\nRavi,x,Pune\nSara,y,Goa\n"
    out = tmp_path / "out.csv"
    result = healer.heal_file(write(tmp_path, text), out, FakeLLM(GOOD_ANSWER))
    assert result.success is False
    assert "not ages" in result.error_message
    assert not out.exists()
