"""One bounded pass through the existing archive transaction engine per market."""

import json
import resource
import subprocess
import sys
from pathlib import Path
from time import time


BLOCKED_EXIT_REASONS = {
    "TARGET_ABSENT",
    "TARGET_NOT_READY",
    "TARGET_LOW_SPACE",
}


def classify(exit_code, parsed):
    exit_reason = parsed.get("exit_reason")
    backlog = parsed.get("backlog_files_after")
    blocked = exit_reason in BLOCKED_EXIT_REASONS
    # A target outage is only a failure when verified data is waiting.
    stalled = blocked and bool(backlog)
    return {
        "exit_reason": exit_reason,
        "target_state": parsed.get("target_state"),
        "backlog_files_after": backlog,
        "blocked": blocked,
        "stalled": stalled,
        "failed_transactions": parsed.get("failed_transactions"),
        "failed": exit_code != 0 or stalled,
    }


def main():
    release = Path(__file__).resolve().parent.parent
    roots = json.loads(Path("/etc/sbmdr/roots.json").read_text())
    started = time()
    failed = False
    reports = {}
    for market, root in roots.items():
        result = subprocess.run(
            [sys.executable, release / "prototype/operations.py", "archive", root],
            capture_output=True,
            text=True,
            check=False,
        )
        parsed = {}
        if result.stdout.strip():
            try:
                parsed = json.loads(result.stdout)
            except json.JSONDecodeError:
                parsed = {}
        classified = classify(result.returncode, parsed)
        failed |= classified["failed"]
        reports[market] = {
            "exit": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            **classified,
        }
    child = resource.getrusage(resource.RUSAGE_CHILDREN)
    own = resource.getrusage(resource.RUSAGE_SELF)
    row = {
        "start_utc_seconds": started,
        "finish_utc_seconds": time(),
        "cpu_seconds": child.ru_utime + child.ru_stime + own.ru_utime + own.ru_stime,
        "peak_rss_upper_bound_bytes": (child.ru_maxrss + own.ru_maxrss) * 1024,
        "failed": failed,
        "markets": reports,
    }
    with Path("/var/log/sbmdr/archive.jsonl").open("a") as out:
        out.write(json.dumps(row) + "\n")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
