"""Freeze the tested candidate identity required by F6/F7/C4-C."""

import argparse
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

CPP_FILES = (
    "prototype/fault_io.cpp",
    "prototype/main.cpp",
    "prototype/recorder_core.hpp",
    "prototype/recorder_run.hpp",
    "prototype/recorder_storage.hpp",
)
SCRIPT_GLOBS = (
    "prototype/*.py",
    "prototype/CMakeLists.txt",
    "deployment/*.py",
    "deployment/*.md",
    "deployment/*.service",
    "deployment/*.timer",
    "deployment/*.slice",
    "deployment/logrotate.conf",
    "deployment/sbmdr-recorder@usdm.service.d/*.conf",
    "config/*.json",
)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_digest(root):
    entries = []
    for path in sorted(Path(root).rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            entries.append(
                f"{path.relative_to(root).as_posix()}\0{digest(path)}"
            )
    return hashlib.sha256("\n".join(entries).encode()).hexdigest()


def test_count(log):
    match = re.search(r"tests passed, \d+ tests failed out of (\d+)", log)
    return int(match.group(1)) if match else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--commit")
    parser.add_argument("--results", type=Path)
    a = parser.parse_args()
    source = a.source
    results = a.results or source / "results"
    release_binary = source / "release/sbmdr-c0"
    def require(condition, message):
        if not condition:
            raise RuntimeError(message)

    require(release_binary.is_file(), "missing release binary")
    commit = a.commit or subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    binary_sha256 = {
        mode: digest(source / mode / "sbmdr-c0")
        for mode in ("release", "asan", "tsan")
    }
    tests = {}
    for mode in ("release", "asan", "tsan"):
        exit_path = results / f"ctest-{mode}.exit"
        log_path = results / f"ctest-{mode}.log"
        require(exit_path.is_file(), f"missing {exit_path}")
        code = int(exit_path.read_text().strip())
        require(code == 0, f"{mode} matrix failed")
        log = log_path.read_text() if log_path.is_file() else ""
        tests[mode] = {
            "exit_code": code,
            "binary_sha256": binary_sha256[mode],
            "test_count": test_count(log),
        }
    cpp_files = {path: digest(source / path) for path in CPP_FILES}
    scripts = {}
    for pattern in SCRIPT_GLOBS:
        for path in sorted(source.glob(pattern)):
            if path.is_file():
                scripts[path.relative_to(source).as_posix()] = digest(path)
    candidate = {
        "status": "PASS",
        "source_commit": commit,
        "binary_sha256": binary_sha256["release"],
        "cpp_files": cpp_files,
        "script_files": scripts,
        "tests": tests,
        "utc": datetime.now(UTC).isoformat(),
    }
    (results / "native-candidate-tests.json").write_text(
        json.dumps(candidate, indent=2) + "\n"
    )
    ledger = {
        "source_commit": commit,
        "cpp_files": cpp_files,
        "script_files": scripts,
        "utc": candidate["utc"],
    }
    (results / "source-ledger.json").write_text(json.dumps(ledger, indent=2) + "\n")
    frozen = {
        "status": "FROZEN",
        "source_commit": commit,
        "binary_sha256": binary_sha256,
        "candidate_tests_sha256": digest(results / "native-candidate-tests.json"),
        "source_ledger_sha256": digest(results / "source-ledger.json"),
        "config_sha256": digest(source / "config/c4-mainstream.json"),
        "python_baseline_tree_sha256": tree_digest(
            source / "vendor/python-baseline"
        ),
        "tests": tests,
        "utc": candidate["utc"],
    }
    (results / "frozen-candidate.json").write_text(
        json.dumps(frozen, indent=2) + "\n"
    )
    print(json.dumps(frozen, indent=2))


if __name__ == "__main__":
    main()
