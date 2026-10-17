#!/usr/bin/env python3
"""Execute a cargo-test selection from the suite's existing compiler artifacts, without Cargo."""
import json
import os
from pathlib import Path
import sys


def refuse(reason):
    print(f"prebuilt-test: REFUSED {reason}", file=sys.stderr)
    raise SystemExit(2)


def main():
    args = sys.argv[1:]
    if not args or args.pop(0) != "test":
        refuse("the prebuilt suite accepts a test selection only; compilation is not a test job")
    package = target = None
    test_args = []
    while args:
        arg = args.pop(0)
        if arg in ("-p", "--package", "--test"):
            if not args:
                refuse(f"{arg} carries no value")
            value = args.pop(0)
            if arg == "--test":
                target = value
            else:
                package = value
        elif arg == "--lib":
            target = "lib"
        elif arg == "--":
            test_args.extend(args)
            break
        elif arg.startswith("-"):
            refuse(f"compilation option {arg!r} is not supported in a prebuilt test selection")
        else:
            test_args.append(arg)
    if not package or not target:
        refuse("the prebuilt test selection requires a package and a test target")
    artifact_path = os.environ.get("I143_PREBUILT_ARTIFACTS")
    if not artifact_path:
        refuse("the gate did not supply I143_PREBUILT_ARTIFACTS")
    try:
        rows = Path(artifact_path).read_text().splitlines()
    except OSError as error:
        refuse(f"compiler artifacts {artifact_path}: {error}")
    matches = {}
    for line in rows:
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("reason") != "compiler-artifact" or not row.get("executable") or not row.get("profile", {}).get("test"):
            continue
        identity = row["package_id"]
        fragment = identity.rsplit("#", 1)[-1]
        name = fragment.split("@", 1)[0] if "@" in fragment else identity.split("#", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        kind = row["target"]
        if name != package or not (target == kind["name"] or target == "lib" and "lib" in kind["kind"]):
            continue
        matches[row["executable"]] = str(Path(row["manifest_path"]).parent)
    if len(matches) != 1:
        refuse(f"{package}:{target} has {len(matches)} matching test executables in {artifact_path}; expected one")
    executable, directory = next(iter(matches.items()))
    if not os.path.isabs(executable) or not os.access(executable, os.X_OK):
        refuse(f"{package}:{target} test executable is missing or not executable: {executable}")
    env = dict(os.environ, CARGO_MANIFEST_DIR=directory)
    os.chdir(directory)
    os.execve(executable, [executable, *test_args], env)


if __name__ == "__main__":
    main()
