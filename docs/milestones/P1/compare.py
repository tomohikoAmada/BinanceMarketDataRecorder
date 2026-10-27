"""Local P1 comparison using retained P0 fixtures; no network or deployment.

Provide two already prepared source directories and one shared Python runtime.
The output directory must not exist. Each child uses PYTHONPATH for its selected
source, a fresh temporary data root, exclusive output and its own retained log.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sources = {
        "A": (args.baseline.resolve(), args.baseline_revision),
        "B": (args.candidate.resolve(), args.candidate_revision),
    }
    for relative in (
        "docs/milestones/P0/steady_baseline.py",
        "docs/milestones/P0/benchmark_checks.py",
        "docs/research/2026-10-26/preplan_baseline.py",
    ):
        if (sources["A"][0] / relative).read_bytes() != (sources["B"][0] / relative).read_bytes():
            parser.error(f"comparison instrumentation differs: {relative}")
    # Refuse files, existing directories and dangling links before any output or child.
    if os.path.lexists(args.output):
        parser.error("output already exists; choose a fresh directory")
    args.output.mkdir()
    records = []
    try:
        for family, order in (
            ("all-features", "ABBA"),
            ("core-ten-assets", "ABBA"),
            ("writer", "ABBAABBA"),
        ):
            for number, side in enumerate(order, 1):
                repo, revision = sources[side]
                name = f"{family}-{number}-{side}"
                output = args.output / f"{name}.json"
                if family == "all-features":
                    script = repo / "docs/milestones/P0/steady_baseline.py"
                    options = [
                        "--repo",
                        str(repo),
                        "--commit",
                        revision,
                        "--symbols",
                        "2",
                        "--warmup",
                        "20",
                        "--seconds",
                        "120",
                    ]
                elif family == "core-ten-assets":
                    script = repo / "docs/research/2026-10-26/preplan_baseline.py"
                    options = ["--symbols", "10", "--events", "2000", "--timeout", "120"]
                else:
                    script = Path(__file__).with_name("writer_benchmark.py")
                    options = ["--events", "20000"]
                command = [str(args.python), str(script), *options, "--output", str(output)]
                env = dict(os.environ, PYTHONPATH=f"{repo / 'src'}:{repo}")
                record = {
                    "name": name,
                    "source": str(repo),
                    "revision": revision,
                    "command": command,
                    "started_utc": datetime.now(UTC).isoformat(),
                }
                records.append(record)
                print(f"START {name}", flush=True)
                with (args.output / f"{name}.log").open("x") as log:
                    outcome = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                record.update(exit_code=outcome.returncode, ended_utc=datetime.now(UTC).isoformat())
                print(f"END {name} exit={outcome.returncode}", flush=True)
                if outcome.returncode:
                    raise SystemExit(outcome.returncode)
    finally:
        with (args.output / "execution.json").open("x") as receipt:
            receipt.write(
                json.dumps({"schema": "p1-local-comparison.v1", "runs": records}, indent=2) + "\n"
            )


if __name__ == "__main__":
    main()
