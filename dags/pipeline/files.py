"""Moves data files between the incoming, processing, processed and failed folders."""
import re
import shutil
from pathlib import Path


def run_folder_name(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are not safe in folder names."""
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    return name if name.strip(".") else name.replace(".", "_")


def run_dir(data_dir: Path, run_id: str) -> Path:
    return data_dir / "processing" / run_folder_name(run_id)


def raw_files(data_dir: Path, run_id: str) -> list[Path]:
    return sorted((run_dir(data_dir, run_id) / "raw").glob("*.csv"))


def _unique(path: Path) -> Path:
    """Return path, or path__2, path__3, ... if it already exists, so an archive is never overwritten."""
    candidate, number = path, 2
    while candidate.exists():
        candidate = path.with_name(f"{path.name}__{number}")
        number += 1
    return candidate


def claim_incoming(data_dir: Path, run_id: str) -> list[Path]:
    """Move this run's files into its raw/ folder and return their paths.

    On a retry the run already owns files, so return those. If the run failed before and was
    cleared in Airflow, bring its whole folder back from failed/ and retry the same files.
    Otherwise claim every waiting CSV.
    """
    already_claimed = raw_files(data_dir, run_id)
    if already_claimed:
        return already_claimed
    folder = run_dir(data_dir, run_id)
    failed_folder = data_dir / "failed" / run_folder_name(run_id)
    if not folder.exists() and any((failed_folder / "raw").glob("*.csv")):
        folder.parent.mkdir(parents=True, exist_ok=True)
        failed_folder.replace(folder)
        return raw_files(data_dir, run_id)
    waiting = sorted((data_dir / "incoming").glob("*.csv"))
    if not waiting:
        return []
    target_dir = folder / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)
    for path in waiting:
        try:
            path.replace(target_dir / path.name)
        except FileNotFoundError:
            continue  # another process took it between the listing and the move
    return raw_files(data_dir, run_id)


def finish_run(data_dir: Path, run_id: str, outcome: str) -> Path | None:
    """Move this run's folder to processed/ or failed/. Returns None if there is nothing to move.

    An existing archive with the same name is kept: the new one gets a __2, __3, ... suffix.
    """
    if outcome not in ("processed", "failed"):
        raise ValueError(f"outcome must be 'processed' or 'failed', got {outcome!r}")
    name = run_folder_name(run_id)
    source = run_dir(data_dir, run_id)
    if not source.exists() and outcome == "processed":
        # The run failed, was cleared in Airflow and then passed: its folder is already in failed/.
        failed_folder = data_dir / "failed" / name
        if any((failed_folder / "raw").glob("*.csv")):
            source = failed_folder
    if not source.exists():
        return None
    target = _unique(data_dir / outcome / name)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))
    return target
