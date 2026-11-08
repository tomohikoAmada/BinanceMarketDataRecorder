"""One bounded pass through the existing archive transaction engine per market."""

import json
import resource
import subprocess
import sys
from pathlib import Path
from time import time


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
        failed |= result.returncode != 0
        reports[market] = {
            "exit": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
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
