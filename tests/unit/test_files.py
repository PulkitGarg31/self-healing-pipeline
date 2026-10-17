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
