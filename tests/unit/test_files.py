import pathlib

import pytest

from pipeline import files

RUN = "dataset_triggered__2026-10-17T11:20:00+00:00"


def drop(data_dir, name, text="name,age,city\n"):
    incoming = data_dir / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    path = incoming / name
    path.write_text(text, encoding="utf-8")
    return path


def test_run_folder_name_is_safe():
    assert files.run_folder_name(RUN) == "dataset_triggered__2026-10-17T11_20_00_00_00"


def test_claim_moves_waiting_csv_files(tmp_path):
    drop(tmp_path, "a.csv")
    drop(tmp_path, "b.csv")
    drop(tmp_path, "c.tmp")
    claimed = files.claim_incoming(tmp_path, RUN)
    assert [p.name for p in claimed] == ["a.csv", "b.csv"]
    assert all(p.parent == files.run_dir(tmp_path, RUN) / "raw" for p in claimed)
    assert [p.name for p in (tmp_path / "incoming").iterdir()] == ["c.tmp"]


def test_claim_with_nothing_waiting(tmp_path):
    assert files.claim_incoming(tmp_path, RUN) == []


def test_retry_reuses_the_same_files(tmp_path):
    drop(tmp_path, "a.csv")
    first = files.claim_incoming(tmp_path, RUN)
    drop(tmp_path, "late.csv")
    second = files.claim_incoming(tmp_path, RUN)
    assert second == first
    assert (tmp_path / "incoming" / "late.csv").exists()


def test_finish_run_moves_to_processed(tmp_path):
    drop(tmp_path, "a.csv")
    files.claim_incoming(tmp_path, RUN)
    moved = files.finish_run(tmp_path, RUN, "processed")
    assert moved == tmp_path / "processed" / files.run_folder_name(RUN)
    assert (moved / "raw" / "a.csv").exists()
    assert not files.run_dir(tmp_path, RUN).exists()


def test_finish_run_moves_to_failed(tmp_path):
    drop(tmp_path, "a.csv")
    files.claim_incoming(tmp_path, RUN)
    moved = files.finish_run(tmp_path, RUN, "failed")
    assert (moved / "raw" / "a.csv").exists()
    assert moved.parent == tmp_path / "failed"


def test_finish_run_without_a_folder(tmp_path):
    assert files.finish_run(tmp_path, RUN, "processed") is None


def test_finish_run_rejects_unknown_outcome(tmp_path):
    with pytest.raises(ValueError):
        files.finish_run(tmp_path, RUN, "lost")


def test_dot_run_ids_are_made_safe():
    assert files.run_folder_name(".") == "_"
    assert files.run_folder_name("..") == "__"


def test_finish_run_never_overwrites_an_archive(tmp_path):
    name = files.run_folder_name(RUN)
    old = tmp_path / "failed" / name / "raw" / "old.csv"
    old.parent.mkdir(parents=True)
    old.write_text("name,age,city\n", encoding="utf-8")
    new = files.run_dir(tmp_path, RUN) / "raw" / "new.csv"
    new.parent.mkdir(parents=True)
    new.write_text("name,age,city\n", encoding="utf-8")
    moved = files.finish_run(tmp_path, RUN, "failed")
    assert moved.name == name + "__2"
    assert moved.parent == tmp_path / "failed"
    assert old.exists()
    assert (moved / "raw" / "new.csv").exists()


def test_cleared_failed_run_takes_back_its_own_files(tmp_path):
    drop(tmp_path, "orig.csv")
    files.claim_incoming(tmp_path, RUN)
    files.finish_run(tmp_path, RUN, "failed")
    drop(tmp_path, "new.csv")
    claimed = files.claim_incoming(tmp_path, RUN)
    assert [p.name for p in claimed] == ["orig.csv"]
    assert (tmp_path / "incoming" / "new.csv").exists()


def test_run_cleared_after_failing_late_is_archived_as_processed(tmp_path):
    drop(tmp_path, "a.csv")
    files.claim_incoming(tmp_path, RUN)
    files.finish_run(tmp_path, RUN, "failed")
    moved = files.finish_run(tmp_path, RUN, "processed")
    assert moved == tmp_path / "processed" / files.run_folder_name(RUN)
    assert (moved / "raw" / "a.csv").exists()
    assert not (tmp_path / "failed" / files.run_folder_name(RUN)).exists()


def test_repeated_failures_keep_retrying_the_same_files(tmp_path):
    drop(tmp_path, "orig.csv")
    files.claim_incoming(tmp_path, RUN)
    files.finish_run(tmp_path, RUN, "failed")
    for attempt in range(2):  # clear in Airflow and fail again, twice
        drop(tmp_path, f"new{attempt}.csv")
        claimed = files.claim_incoming(tmp_path, RUN)
        assert [p.name for p in claimed] == ["orig.csv"]
        files.finish_run(tmp_path, RUN, "failed")
    folder = files.run_folder_name(RUN)
    assert (tmp_path / "failed" / folder / "raw" / "orig.csv").exists()
    assert not (tmp_path / "failed" / (folder + "__2")).exists()


def test_claim_skips_a_file_that_vanishes(tmp_path, monkeypatch):
    real_replace = pathlib.Path.replace

    def replace(self, target):
        if self.name == "gone.csv":
            raise FileNotFoundError(self)
        return real_replace(self, target)

    monkeypatch.setattr(pathlib.Path, "replace", replace)
    drop(tmp_path, "a.csv")
    drop(tmp_path, "gone.csv")
    claimed = files.claim_incoming(tmp_path, RUN)
    assert [p.name for p in claimed] == ["a.csv"]
