#!/usr/bin/env python3
"""Keep one immutable scenario snapshot after the gate's concurrent test barrier."""

import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile


def keep(log: Path, artifacts: Path, destination: Path) -> None:
    jobs = list(dict.fromkeys(re.findall(
        r"^FAIL(?:\([^\n)]+\))?\s+([A-Za-z0-9_.:-]+)(?:\s|$)",
        log.read_text(), re.MULTILINE,
    )))
    if not jobs:
        return
    destination.mkdir(parents=True, exist_ok=True)
    name = "gate-" + log.stem
    shared = destination / name
    if shared.exists():
        raise ValueError(f"evidence snapshot {shared} already exists; never overwrite it")
    temporary = Path(tempfile.mkdtemp(prefix=name + ".tmp-", dir=destination))
    try:
        if artifacts.is_dir():
            shutil.copytree(artifacts, temporary / "artifacts", symlinks=True)
        else:
            (temporary / "artifacts").mkdir()
        shutil.copy2(log, temporary / "gate.log")
        (temporary / "manifest.json").write_text(json.dumps({
            "gate_log": str(log.resolve()),
            "source_artifacts": str(artifacts.resolve()),
            "jobs": jobs,
        }, indent=2) + "\n")
        temporary.rename(shared)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    for job in jobs:
        view = destination / (name + "-" + job)
        view.mkdir(exist_ok=True)
        (view / "artifacts").symlink_to(os.path.relpath(shared / "artifacts", view), target_is_directory=True)
        job_log = Path(str(log) + ".d") / (job + ".log")
        if job_log.is_file():
            shutil.copy2(job_log, view / "gate.log")
    print(f"kept one evidence snapshot for {len(jobs)} refused jobs: {shared}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: keep-gate-evidence.py <gate.log> <artifacts> <kept-directory>")
    try:
        keep(*(Path(p) for p in sys.argv[1:4]))
    except (OSError, ValueError) as error:
        sys.exit(f"REFUSED evidence preservation: {error}")
