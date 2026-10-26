"""Attach one low-frequency sampler strictly inside P0's steady window."""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def process_tree_sample(root_pid):
    processes = {}
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            raw = path.read_text()
            fields = raw[raw.rindex(")") + 2 :].split()
            pid = int(path.parent.name)
            processes[pid] = (int(fields[1]), int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError, IndexError):
            continue
    owned = {root_pid}
    while True:
        expanded = owned | {pid for pid, (ppid, _) in processes.items() if ppid in owned}
        if expanded == owned:
            break
        owned = expanded
    return {
        "elapsed_clock": time.perf_counter(),
        "rss_sum_bytes": sum(processes[pid][1] for pid in owned if pid in processes),
        "pids": sorted(owned),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--profiler", type=Path, required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--wait-suite", action="store_true")
    args = p.parse_args()
    root = args.root.resolve()
    if args.wait_suite:
        deadline = time.monotonic() + 1200
        while not (root / "suite-complete.json").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("earlier finite benchmark suite did not finish")
            time.sleep(2)
    command = [
        sys.executable,
        str(root / "docs/milestones/P0/steady_baseline.py"),
        "--repo",
        str(root),
        "--commit",
        args.commit,
        "--symbols",
        "2",
        "--warmup",
        "20",
        "--seconds",
        "120",
        "--output",
        str(root / "profile-steady-n2.json"),
    ]
    env = dict(os.environ, PYTHONPATH=str(root / "src") + ":" + str(root))
    begin = time.time()
    samples = []
    with (
        (root / "profile-steady-n2.stdout").open("w") as out,
        (root / "profile-steady-n2.stderr").open("w") as err,
    ):
        process = subprocess.Popen(command, env=env, stdout=out, stderr=err)
        sampler = None
        try:
            deadline = time.monotonic() + 60
            while "STEADY_BEGIN" not in (root / "profile-steady-n2.stdout").read_text():
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("steady start was not observed")
                samples.append(process_tree_sample(process.pid))
                time.sleep(0.5)
            profiler_command = [
                str(args.profiler),
                "record",
                "--pid",
                str(process.pid),
                "--subprocesses",
                "--rate",
                "10",
                "--duration",
                "110",
                "--format",
                "raw",
                "--output",
                str(root / "profile-steady-n2.folded"),
            ]
            attach_time = time.time()
            with (
                (root / "profile-steady-sampler.stdout").open("w") as pout,
                (root / "profile-steady-sampler.stderr").open("w") as perr,
            ):
                sampler = subprocess.Popen(profiler_command, stdout=pout, stderr=perr)
                while sampler.poll() is None:
                    samples.append(process_tree_sample(process.pid))
                    if time.time() - attach_time > 125:
                        raise TimeoutError("sampler exceeded its bound")
                    time.sleep(0.5)
            sampler_end_time = time.time()
            assert sampler.returncode == 0
            assert "STEADY_END" not in (root / "profile-steady-n2.stdout").read_text()
            assert process.wait(timeout=120) == 0
        finally:
            for child in (sampler, process):
                if child is not None and child.poll() is None:
                    child.terminate()
                    child.wait(timeout=120)
    result = dict(
        argv=command,
        profiler_argv=profiler_command,
        start_epoch=begin,
        attach_epoch=attach_time,
        sampler_end_epoch=sampler_end_time,
        end_epoch=time.time(),
        exit_code=process.returncode,
        sampler_exit_code=sampler.returncode,
        sampler_finished_before_steady_end=True,
        process_tree_rss_peak_bytes=max(s["rss_sum_bytes"] for s in samples),
        telemetry_samples=samples,
    )
    (root / "profile-steady-n2.execution.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
