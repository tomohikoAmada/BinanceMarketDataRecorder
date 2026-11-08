"""Install a pinned candidate; start only after C3 PASS. Never touches old Python units/data."""

import argparse
import hashlib
import json
import os
import pwd
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--c3-report", type=Path, required=True)
    p.add_argument("--version", required=True)
    a = p.parse_args()
    c3 = json.loads(a.c3_report.read_text())
    assert c3["status"] == "PASS", "C3 not signed off"
    binary_sha256 = hashlib.sha256(
        (a.source / "release/sbmdr-c0").read_bytes()
    ).hexdigest()
    # C4 may contain its own fixes. Bind that candidate to current native checks;
    # retain the actual C3 baseline identity instead of relabeling its capture.
    checks = json.loads((a.source / "results/native-candidate-tests.json").read_text())
    assert checks["status"] == "PASS" and checks["binary_sha256"] == binary_sha256
    for path, digest in checks["cpp_files"].items():
        assert hashlib.sha256((a.source / path).read_bytes()).hexdigest() == digest
    for mode in ("release", "asan", "tsan"):
        assert checks["tests"][mode]["exit_code"] == 0
        assert (
            hashlib.sha256((a.source / mode / "sbmdr-c0").read_bytes()).hexdigest()
            == checks["tests"][mode]["binary_sha256"]
        )
    for mode in ("asan", "tsan"):
        assert (a.source / f"results/ctest-{mode}.exit").read_text().strip() == "0"
    user = pwd.getpwnam("bmdr")
    for unit in ("sbmdr-recorder@spot", "sbmdr-recorder@usdm", "sbmdr-archive.service"):
        state = subprocess.run(
            ["systemctl", "is-active", unit],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        assert state not in {"active", "activating", "deactivating"}, (unit, state)
    release = Path("/opt/sbmdr/releases") / a.version
    if release.exists():
        raise ValueError("immutable release already exists")
    release.mkdir(parents=True)
    (release / "release").mkdir()
    shutil.copy2(a.source / "release/sbmdr-c0", release / "release/sbmdr-c0")
    os.chmod(release / "release/sbmdr-c0", 0o755)
    (release / "bin").mkdir()
    (release / "bin/binance-market-recorder").symlink_to("../release/sbmdr-c0")
    shutil.copy2(
        a.source / "results/source-ledger.json", release / "source-ledger.json"
    )
    shutil.copy2(
        a.source / "results/native-candidate-tests.json",
        release / "native-candidate-tests.json",
    )
    for folder in ("prototype", "deployment", "config"):
        shutil.copytree(
            a.source / folder,
            release / folder,
            ignore=shutil.ignore_patterns("__pycache__", "._*"),
        )
    (release / "python").mkdir()
    shutil.copytree(a.source / "python/src", release / "python/src")
    runtime = Path("/srv/recorder-data/S-BMDR/runtime") / a.version
    runtime.mkdir(parents=True)
    shutil.copytree((a.source / "venv").resolve(), runtime / "venv", symlinks=True)
    for root, dirs, files in os.walk(runtime):
        os.chmod(root, 0o755)
        for name in files:
            path = Path(root) / name
            if not path.is_symlink():
                os.chmod(path, path.stat().st_mode | 0o444)
    (release / "venv").symlink_to(runtime / "venv")
    current = Path("/opt/sbmdr/current")
    temporary = current.with_name("current.new")
    temporary.symlink_to(release)
    temporary.replace(current)
    Path("/etc/sbmdr").mkdir(parents=True, exist_ok=True)
    shutil.copy2(release / "config/c4-mainstream.json", "/etc/sbmdr/config.json")
    log = Path("/var/log/sbmdr")
    log.mkdir(parents=True, exist_ok=True)
    os.chown(log, 0, 0)
    os.chmod(log, 0o755)
    old_logs = a.source / "results" / f"logs-before-{a.version}"
    old_logs.mkdir()
    for name in (
        "spot.stdout.jsonl",
        "spot.status.jsonl",
        "usdm.stdout.jsonl",
        "usdm.status.jsonl",
        "archive.jsonl",
    ):
        path = log / name
        if path.exists():
            if path.stat().st_size:
                shutil.copy2(path, old_logs / name)
            path.write_bytes(b"")
    archive_log = log / "archive.jsonl"
    archive_log.touch()
    os.chown(archive_log, user.pw_uid, user.pw_gid)
    label = datetime.now(UTC).strftime("live-%Y%m%dT%H%M%SZ")
    base = Path("/var/lib/binance-market-data-recorder/S-BMDR") / label
    roots = {}
    env = {**os.environ, "PYTHONPATH": str(release / "python/src")}
    for market in ("spot", "usdm"):
        root = base / market
        root.mkdir(parents=True)
        os.chown(root, user.pw_uid, user.pw_gid)
        target = Path("/srv/recorder-data/S-BMDR") / f"{label}-{market}"
        target.mkdir()
        os.chown(target, user.pw_uid, user.pw_gid)
        subprocess.run(
            [
                "runuser",
                "-u",
                "bmdr",
                "--",
                release / "venv/bin/python",
                release / "prototype/operations.py",
                "register",
                root,
                "--target",
                target,
            ],
            env=env,
            check=True,
        )
        lock = root / "state/c1.lock"
        lock.touch()
        os.chown(lock, user.pw_uid, user.pw_gid)
        roots[market] = str(root)
        Path(f"/etc/sbmdr/{market}.env").write_text(f"ROOT={root}\n")
        (log / f"{market}.stdout.jsonl").touch()
        (log / f"{market}.status.jsonl").touch()
    Path("/etc/sbmdr/roots.json").write_text(json.dumps(roots, indent=2) + "\n")
    for name in (
        "sbmdr-recorder@.service",
        "sbmdr-archive.service",
        "sbmdr-archive.timer",
        "sbmdr.slice",
    ):
        shutil.copy2(release / "deployment" / name, Path("/etc/systemd/system") / name)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(
        [
            "systemd-analyze",
            "verify",
            "/etc/systemd/system/sbmdr-recorder@spot.service",
            "/etc/systemd/system/sbmdr-archive.service",
        ],
        check=True,
    )
    evidence = a.source / "results/c4-c5-24h"
    evidence.mkdir()
    report = {
        "status": "INSTALLED_NOT_QUALIFIED",
        "release": str(release),
        "runtime": str(runtime),
        "roots": roots,
        "binary_sha256": binary_sha256,
        "c3_baseline_binary_sha256": c3["binary_sha256"],
        "resource_policy": {
            "slice_cpu_quota_percent": 175,
            "archive_cpu_quota_percent": 25,
            "archive_timeout_seconds": 240,
            "public_seal_target_bytes": 32 * 1024**2,
        },
        "config_sha256": hashlib.sha256(
            Path("/etc/sbmdr/config.json").read_bytes()
        ).hexdigest(),
        "evidence": str(evidence),
    }
    (a.source / "results/deployment.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
