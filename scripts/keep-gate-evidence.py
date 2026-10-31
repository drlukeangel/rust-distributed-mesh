#!/usr/bin/env python3
"""Keep one immutable scenario snapshot after the gate's concurrent test barrier."""

import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile


def failed_cells(job_log: Path) -> list:
    """The function names of the cells a job's libtest log reports failed: each is the name of the
    cell's testkit span directory (`$TMPDIR/i143-testkit/<cell>`)."""
    if not job_log.is_file():
        return []
    names = re.findall(r"^(?:test )?([A-Za-z0-9_:]+)(?: \.\.\.)? (?:--- )?FAILED$", job_log.read_text(errors="replace"), re.MULTILINE)
    return list(dict.fromkeys(n.rsplit("::", 1)[-1] for n in names))


REGISTRY = Path("tools/mesh-audit/i143-acceptance-jobs.json")


def acceptance_dirs(job: str) -> list:
    """The cell directories of acceptance job `job` (the gate names an rshape job without its `i143-`
    prefix), from the acceptance registry. A later run of the same job (the export after the release)
    writes into them, so a refused job's are kept here with the gate's other evidence."""
    if not REGISTRY.is_file():
        return []
    reg = json.loads(REGISTRY.read_text())
    jobs = {**reg.get("jobs", {}), **{k: v for k, v in reg.get("rshape_jobs", {}).items() if k != "_contract"}}
    found = jobs.get(job) or jobs.get("i143-" + job) or {}
    return [Path(c["dir"]) for c in found.get("cells", []) if c.get("dir")]


def keep(log: Path, artifacts: Path, destination: Path, testkit: Path) -> None:
    jobs = list(dict.fromkeys(re.findall(
        r"^FAIL(?:\([^\n)]+\))?\s+([A-Za-z0-9_.:-]+)(?:\s|$)",
        log.read_text(), re.MULTILINE,
    )))
    if not jobs:
        return
    cells = list(dict.fromkeys(c for job in jobs for c in failed_cells(Path(str(log) + ".d") / (job + ".log"))))
    destination.mkdir(parents=True, exist_ok=True)
    # A second run at the same SHA keeps its own snapshot beside the first: never overwritten.
    name = "gate-" + log.stem
    run = 1
    while (destination / name).exists():
        run += 1
        name = f"gate-{log.stem}-run{run}"
    shared = destination / name
    temporary = Path(tempfile.mkdtemp(prefix=name + ".tmp-", dir=destination))
    try:
        if artifacts.is_dir():
            shutil.copytree(artifacts, temporary / "artifacts", symlinks=True)
        else:
            (temporary / "artifacts").mkdir()
        # A failed testkit cell's spans are in its own directory, which the cell's next run writes
        # into: kept here with the scenario artifacts.
        kept_cells = []
        for cell in cells:
            source = testkit / cell
            if source.is_dir():
                shutil.copytree(source, temporary / "testkit" / cell, symlinks=True)
                kept_cells.append(cell)
        kept_jobs = []
        for job in jobs:
            for source in acceptance_dirs(job):
                if source.is_dir():
                    shutil.copytree(source, temporary / "acceptance" / job / source.name, symlinks=True)
                    kept_jobs.append(f"{job}/{source.name}")
        shutil.copy2(log, temporary / "gate.log")
        (temporary / "manifest.json").write_text(json.dumps({
            "gate_log": str(log.resolve()),
            "source_artifacts": str(artifacts.resolve()),
            "jobs": jobs,
            "testkit_cells": kept_cells,
            "acceptance_cells": kept_jobs,
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
    if len(sys.argv) != 5:
        sys.exit("usage: keep-gate-evidence.py <gate.log> <artifacts> <kept-directory> <testkit-directory>")
    try:
        keep(*(Path(p) for p in sys.argv[1:5]))
    except (OSError, ValueError) as error:
        sys.exit(f"REFUSED evidence preservation: {error}")
