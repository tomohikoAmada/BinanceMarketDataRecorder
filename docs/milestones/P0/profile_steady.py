"""Attach one low-frequency sampler strictly inside P0's measured window."""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

OUTPUTS = (
    "profile-steady-n2.json",
    "profile-steady-n2.stdout",
    "profile-steady-n2.stderr",
    "profile-steady-n2.folded",
    "profile-steady-sampler.stdout",
    "profile-steady-sampler.stderr",
    "profile-steady-n2.execution.json",
)


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


def reject_output_conflicts(root):
    conflicts = [name for name in OUTPUTS if (root / name).exists() or (root / name).is_symlink()]
    if conflicts:
        # Refusal is reported to stderr by the CLI; no old receipt/log is changed.
        raise FileExistsError("existing profile outputs: " + ", ".join(conflicts))


def run_profile(args):
    root = args.root.resolve()
    reject_output_conflicts(root)
    command = [
        "nice",
        "-n",
        "10",
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
    result = dict(
        status="STARTING",
        argv=command,
        start_epoch=time.time(),
        profiler_argv=None,
        exit_code=None,
        sampler_exit_code=None,
    )
    process, sampler = None, None
    samples = []
    # Exclusive receipt also excludes a second cooperating launcher after preflight.
    with (root / "profile-steady-n2.execution.json").open("x") as receipt:
        try:
            if args.wait_suite:
                deadline = time.monotonic() + 1200
                while not (root / "suite-complete.json").exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("earlier finite benchmark suite did not finish")
                    time.sleep(2)
            with (
                (root / "profile-steady-n2.stdout").open("x") as out,
                (root / "profile-steady-n2.stderr").open("x") as err,
            ):
                process = subprocess.Popen(command, env=env, stdout=out, stderr=err)
                result["pid"] = process.pid
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
                result["profiler_argv"] = profiler_command
                result["attach_epoch"] = time.time()
                with (
                    (root / "profile-steady-sampler.stdout").open("x") as pout,
                    (root / "profile-steady-sampler.stderr").open("x") as perr,
                ):
                    sampler = subprocess.Popen(profiler_command, stdout=pout, stderr=perr)
                    while sampler.poll() is None:
                        samples.append(process_tree_sample(process.pid))
                        if time.time() - result["attach_epoch"] > 125:
                            raise TimeoutError("sampler exceeded its bound")
                        time.sleep(0.5)
                result["sampler_end_epoch"] = time.time()
                if sampler.returncode != 0:
                    raise RuntimeError("sampler failed")
                if "STEADY_END" in (root / "profile-steady-n2.stdout").read_text():
                    raise RuntimeError("sampler extended beyond measured window")
                result["sampler_finished_before_steady_end"] = True
                if process.wait(timeout=120) != 0:
                    raise RuntimeError("benchmark child failed")
                measured = json.loads((root / "profile-steady-n2.json").read_text())
                result["steady_state"] = measured["steady_state"]
                result["status"] = "COMPLETE"
        except BaseException as exc:
            result.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            cleanup_errors = []
            for child in (sampler, process):
                if child is not None and child.poll() is None:
                    try:
                        child.terminate()
                        try:
                            child.wait(timeout=120)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait(timeout=10)
                    except Exception as exc:
                        cleanup_errors.append(type(exc).__name__ + ": " + str(exc))
            result.update(
                end_epoch=time.time(),
                cleanup_errors=cleanup_errors,
                exit_code=None if process is None else process.returncode,
                sampler_exit_code=None if sampler is None else sampler.returncode,
                telemetry_samples=samples,
                process_tree_rss_peak_bytes=max((s["rss_sum_bytes"] for s in samples), default=0),
            )
            if cleanup_errors:
                result["status"] = "FAILED"
            receipt.write(json.dumps(result, indent=2) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--profiler", type=Path, required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--wait-suite", action="store_true")
    args = p.parse_args()
    try:
        run_profile(args)
    except Exception as exc:
        print(
            json.dumps({"status": "FAILED", "error_type": type(exc).__name__, "error": str(exc)}),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
