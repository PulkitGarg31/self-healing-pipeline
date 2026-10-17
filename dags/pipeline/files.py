"""Moves data files between the incoming, processing, processed and failed folders."""
import re
import shutil
from pathlib import Path


def run_folder_name(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are not safe in folder names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def run_dir(data_dir: Path, run_id: str) -> Path:
    return data_dir / "processing" / run_folder_name(run_id)


def raw_files(data_dir: Path, run_id: str) -> list[Path]:
    return sorted((run_dir(data_dir, run_id) / "raw").glob("*.csv"))


def claim_incoming(data_dir: Path, run_id: str) -> list[Path]:
    """Move every waiting CSV into this run's raw/ folder and return the new paths.

    On a retry the run already owns files, so return those instead of claiming new ones.
    """
    already_claimed = raw_files(data_dir, run_id)
    if already_claimed:
        return already_claimed
    waiting = sorted((data_dir / "incoming").glob("*.csv"))
    if not waiting:
        return []
    target_dir = run_dir(data_dir, run_id) / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)
    for path in waiting:
        path.replace(target_dir / path.name)
    return raw_files(data_dir, run_id)


def finish_run(data_dir: Path, run_id: str, outcome: str) -> Path | None:
    """Move processing/<run> to processed/<run> or failed/<run>. Returns None if there is nothing to move."""
    if outcome not in ("processed", "failed"):
        raise ValueError(f"outcome must be 'processed' or 'failed', got {outcome!r}")
    source = run_dir(data_dir, run_id)
    if not source.exists():
        return None
    target = data_dir / outcome / run_folder_name(run_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(source), str(target))
    return target
