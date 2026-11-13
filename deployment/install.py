"""Install a pinned candidate safely; qualify before enabling production restarts."""

import argparse
import hashlib
import json
import os
import pwd
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

BLOCKED_ARCHIVE_REASONS = {
    "TARGET_ABSENT",
    "TARGET_NOT_READY",
    "TARGET_LOW_SPACE",
}


def show(unit, prop):
    return subprocess.check_output(
        ["systemctl", "show", unit, "--property", prop, "--value"], text=True
    ).strip()


def run(*args, env=None, step=None):
    result = subprocess.run(
        [str(arg) for arg in args], env=env, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{step or args[0]} failed ({result.returncode}): {result.stderr.strip()}"
        )
    return result


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def usec(value):
    value = value.strip()
    if value.isdigit():
        return int(value)
    for suffix, scale in (("min", 60), ("ms", 0.001), ("s", 1)):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * scale * 1_000_000)
    raise ValueError(f"cannot parse systemd duration: {value}")


def backup(paths, destination):
    for source, name in paths:
        source = Path(source)
        target = destination / name
        if source.is_symlink():
            target.write_text(os.readlink(source))
        elif source.is_dir():
            shutil.copytree(source, target, symlinks=True)
        elif source.exists():
            shutil.copy2(source, target)


def restore(backup_dir, previous, current, production_drop_in, logrotate):
    if (backup_dir / "etc-sbmdr").is_dir():
        shutil.rmtree("/etc/sbmdr", ignore_errors=True)
        shutil.copytree(backup_dir / "etc-sbmdr", "/etc/sbmdr", symlinks=True)
    if previous is not None:
        temporary = current.with_name("current.new")
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        temporary.symlink_to(previous)
        temporary.replace(current)
    elif current.is_symlink():
        current.unlink()
    for name in (
        "sbmdr-recorder@.service",
        "sbmdr-archive.service",
        "sbmdr-archive.timer",
        "sbmdr.slice",
    ):
        saved = backup_dir / name
        if saved.exists():
            shutil.copy2(saved, Path("/etc/systemd/system") / name)
    for name in ("sbmdr-recorder@usdm.service.d", "sbmdr-recorder@.service.d"):
        saved = backup_dir / name
        destination = Path("/etc/systemd/system") / name
        if saved.is_dir():
            shutil.rmtree(destination, ignore_errors=True)
            shutil.copytree(saved, destination, symlinks=True)
    saved = backup_dir / production_drop_in.name
    if saved.exists():
        production_drop_in.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(saved, production_drop_in)
    saved = backup_dir / logrotate.name
    if saved.exists():
        shutil.copy2(saved, logrotate)
    disabled = Path("/etc/logrotate.d/sbmdr.qualification-disabled")
    if disabled.exists():
        disabled.unlink()
    subprocess.run(["systemctl", "daemon-reload"], check=False)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--c3-report", type=Path, required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--python", default="python3.12")
    a = p.parse_args()
    c3 = json.loads(a.c3_report.read_text())
    require(c3["status"] == "PASS", "C3 not signed off")
    binary_sha256 = hashlib.sha256(
        (a.source / "release/sbmdr-c0").read_bytes()
    ).hexdigest()
    checks = json.loads((a.source / "results/native-candidate-tests.json").read_text())
    require(
        checks["status"] == "PASS" and checks["binary_sha256"] == binary_sha256,
        "native candidate tests do not match the release binary",
    )
    for path, digest in checks["cpp_files"].items():
        require(
            hashlib.sha256((a.source / path).read_bytes()).hexdigest() == digest,
            f"source identity changed: {path}",
        )
    for mode in ("release", "asan", "tsan"):
        require(checks["tests"][mode]["exit_code"] == 0, f"{mode} tests failed")
        require(
            hashlib.sha256((a.source / mode / "sbmdr-c0").read_bytes()).hexdigest()
            == checks["tests"][mode]["binary_sha256"],
            f"{mode} binary identity changed",
        )
    for mode in ("asan", "tsan"):
        for suffix in (f"results/ctest-{mode}.exit", f"results/ctest-{mode}-pinned.exit"):
            require(
                (a.source / suffix).read_text().strip() == "0",
                f"missing or failed {suffix}",
            )
    require((a.source / "results/source-ledger.json").is_file(), "source ledger missing")
    for folder in ("prototype", "deployment", "config", "vendor/python-baseline"):
        require((a.source / folder).is_dir(), f"staging lacks {folder}")

    user = pwd.getpwnam("bmdr")
    # Qualification must not inherit the production restart policy or rotate
    # evidence while the run is in progress.
    subprocess.run(["systemctl", "stop", "sbmdr-archive.timer"], check=False)
    subprocess.run(["systemctl", "stop", "sbmdr-archive.service"], check=False)
    for unit in ("sbmdr-recorder@spot", "sbmdr-recorder@usdm", "sbmdr-archive.service"):
        state = show(unit, "ActiveState")
        require(
            state not in {"active", "activating", "deactivating"},
            f"{unit} is still {state}",
        )

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = a.source / "results" / f"install-backup-{stamp}"
    backup_dir.mkdir(parents=True)
    current = Path("/opt/sbmdr/current")
    previous = Path(os.readlink(current)) if current.is_symlink() else None
    backup(
        [
            ("/etc/sbmdr", "etc-sbmdr"),
            ("/etc/systemd/system/sbmdr-recorder@.service", "sbmdr-recorder@.service"),
            ("/etc/systemd/system/sbmdr-archive.service", "sbmdr-archive.service"),
            ("/etc/systemd/system/sbmdr-archive.timer", "sbmdr-archive.timer"),
            ("/etc/systemd/system/sbmdr.slice", "sbmdr.slice"),
            ("/etc/systemd/system/sbmdr-recorder@usdm.service.d", "sbmdr-recorder@usdm.service.d"),
            (
                "/etc/systemd/system/sbmdr-recorder@.service.d",
                "sbmdr-recorder@.service.d",
            ),
            ("/etc/logrotate.d/sbmdr", "logrotate-sbmdr"),
        ],
        backup_dir,
    )

    release = Path("/opt/sbmdr/releases") / a.version
    runtime = Path("/srv/recorder-data/S-BMDR/runtime") / a.version
    require(not release.exists(), "immutable release already exists")
    require(not runtime.exists(), "runtime already exists")
    production_drop_in = Path(
        "/etc/systemd/system/sbmdr-recorder@.service.d/production.conf"
    )
    logrotate = Path("/etc/logrotate.d/sbmdr")
    step = "prepare-release"
    try:
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
        baseline = a.source / "vendor/python-baseline"
        require((baseline / "BASELINE.json").is_file(), "missing Python baseline")
        shutil.copytree(baseline / "src", release / "python/src")
        for name in ("BASELINE.json", "README.md", "pyproject.toml"):
            shutil.copy2(baseline / name, release / "python" / name)
        shutil.copytree(baseline / "requirements", release / "python/requirements")
        lock = release / "python/requirements/linux-x86_64-python312.lock"
        require(lock.is_file(), "runtime requirements lock missing")

        step = "create-runtime"
        runtime.mkdir(parents=True)
        run(a.python, "-m", "venv", runtime / "venv", step="create runtime venv")
        run(
            runtime / "venv/bin/python",
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "-r",
            lock,
            step="install runtime requirements",
        )
        (release / "venv").symlink_to(runtime / "venv")

        step = "runtime-imports"
        env = {**os.environ, "PYTHONPATH": str(release / "python/src")}
        run(
            "runuser",
            "-u",
            "bmdr",
            "--",
            runtime / "venv/bin/python",
            "-c",
            "import aiohttp,cbor2,google_crc32c,pyarrow,pydantic,websockets,zstandard;"
            "import binance_market_data_recorder.storage.catalog;"
            "import binance_market_data_recorder.archive.drain",
            env=env,
            step="runtime imports",
        )

        step = "register-roots"
        Path("/etc/sbmdr").mkdir(parents=True, exist_ok=True)
        shutil.copy2(release / "config/c4-mainstream.json", "/etc/sbmdr/config.json")
        label = stamp
        base = Path("/var/lib/binance-market-data-recorder/S-BMDR") / label
        roots = {}
        archive_targets = {}
        for market in ("spot", "usdm"):
            root = base / market
            (root / "partitions").mkdir(parents=True)
            (root / "state").mkdir()
            os.chown(root, user.pw_uid, user.pw_gid)
            os.chown(root / "partitions", user.pw_uid, user.pw_gid)
            os.chown(root / "state", user.pw_uid, user.pw_gid)
            target = Path("/srv/recorder-data/S-BMDR") / f"{label}-{market}"
            target.mkdir()
            os.chown(target, user.pw_uid, user.pw_gid)
            lock_path = root / "state/c1.lock"
            lock_path.touch()
            os.chown(lock_path, user.pw_uid, user.pw_gid)
            roots[market] = str(root)
            archive_targets[market] = str(target)
            Path(f"/etc/sbmdr/{market}.env").write_text(f"ROOT={root}\n")
        Path("/etc/sbmdr/roots.json").write_text(json.dumps(roots, indent=2) + "\n")
        Path("/etc/sbmdr/archive-targets.json").write_text(
            json.dumps(archive_targets, indent=2) + "\n"
        )
        metadata_roots = {
            market: str(Path("/srv/recorder-data/S-BMDR/metadata") / market)
            for market in roots
        }
        Path("/etc/sbmdr/metadata-roots.json").write_text(
            json.dumps(metadata_roots, indent=2) + "\n"
        )

        step = "install-units"
        for name in (
            "sbmdr-recorder@.service",
            "sbmdr-archive.service",
            "sbmdr-archive.timer",
            "sbmdr.slice",
        ):
            shutil.copy2(release / "deployment" / name, Path("/etc/systemd/system") / name)
        usdm_drop = Path("/etc/systemd/system/sbmdr-recorder@usdm.service.d")
        usdm_drop.mkdir(exist_ok=True)
        shutil.copy2(
            release / "deployment/sbmdr-recorder@usdm.service.d/cpu-weight.conf",
            usdm_drop / "cpu-weight.conf",
        )
        # The C5-only production drop-in must not leak Restart=on-failure into
        # the qualification candidate.
        if production_drop_in.exists():
            shutil.move(str(production_drop_in), backup_dir / production_drop_in.name)
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        run(
            "systemd-analyze",
            "verify",
            "/etc/systemd/system/sbmdr-recorder@spot.service",
            "/etc/systemd/system/sbmdr-recorder@usdm.service",
            "/etc/systemd/system/sbmdr-archive.service",
            step="systemd-analyze verify",
        )

        step = "check-systemd-policy"
        for market in ("spot", "usdm"):
            unit = f"sbmdr-recorder@{market}.service"
            require(show(unit, "Restart") == "no", f"{unit} Restart is not no")
            require(
                show(unit, "CPUWeight") == ("100" if market == "spot" else "200"),
                f"{unit} CPUWeight mismatch",
            )
            require(
                usec(show(unit, "TimeoutStopUSec")) == 180_000_000,
                f"{unit} TimeoutStopUSec mismatch",
            )
            require(
                usec(show(unit, "TimeoutStartUSec")) == 300_000_000,
                f"{unit} TimeoutStartUSec mismatch",
            )
        require(
            show("sbmdr-archive.service", "Restart") == "no",
            "archive service Restart is not no",
        )
        require(
            usec(show("sbmdr-archive.service", "CPUQuotaPerSecUSec")) == 500_000,
            "archive CPUQuota mismatch",
        )
        require(
            usec(show("sbmdr-archive.service", "TimeoutStartUSec")) == 600_000_000,
            "archive TimeoutStartUSec mismatch",
        )
        require(
            usec(show("sbmdr.slice", "CPUQuotaPerSecUSec")) == 1_750_000,
            "slice CPUQuota mismatch",
        )

        step = "switch-current"
        temporary = current.with_name("current.new")
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        temporary.symlink_to(release)
        temporary.replace(current)

        step = "post-switch-imports"
        run(
            "runuser",
            "-u",
            "bmdr",
            "--",
            runtime / "venv/bin/python",
            "-c",
            "import binance_market_data_recorder.storage.catalog",
            env={**os.environ, "PYTHONPATH": "/opt/sbmdr/current/python/src"},
            step="post-switch imports",
        )

        step = "prepare-logs"
        log = Path("/var/log/sbmdr")
        log.mkdir(parents=True, exist_ok=True)
        os.chown(log, 0, 0)
        os.chmod(log, 0o755)
        old_logs = a.source / "results" / f"logs-before-{a.version}"
        old_logs.mkdir(exist_ok=True)
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
        for name in ("spot.stdout.jsonl", "spot.status.jsonl", "usdm.stdout.jsonl", "usdm.status.jsonl"):
            (log / name).touch()
        if logrotate.exists():
            logrotate.replace("/etc/logrotate.d/sbmdr.qualification-disabled")

        step = "evidence"
        evidence = a.source / "results/c4-c5-24h"
        if evidence.exists():
            legacy = a.source / "results/c4-b-60m-evidence"
            if legacy.exists():
                legacy = a.source / "results" / (
                    f"c4-b-60m-evidence-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
                )
            evidence.rename(legacy)
        evidence.mkdir()
    except BaseException as error:
        failed = {
            "status": "INSTALL_FAILED",
            "step": step,
            "error": repr(error),
            "backup": str(backup_dir),
        }
        (a.source / "results/deployment-failed.json").write_text(
            json.dumps(failed, indent=2) + "\n"
        )
        restore(
            backup_dir, previous, current, production_drop_in, logrotate
        )
        shutil.rmtree(release, ignore_errors=True)
        shutil.rmtree(runtime, ignore_errors=True)
        raise

    systemd = {}
    for market in ("spot", "usdm"):
        unit = f"sbmdr-recorder@{market}.service"
        systemd[market] = {
            "restart": show(unit, "Restart"),
            "cpu_weight": show(unit, "CPUWeight"),
            "timeout_start_usec": usec(show(unit, "TimeoutStartUSec")),
            "timeout_stop_usec": usec(show(unit, "TimeoutStopUSec")),
        }
    systemd["archive"] = {
        "restart": show("sbmdr-archive.service", "Restart"),
        "cpu_quota_usec": usec(show("sbmdr-archive.service", "CPUQuotaPerSecUSec")),
        "timeout_start_usec": usec(show("sbmdr-archive.service", "TimeoutStartUSec")),
    }
    systemd["slice_cpu_quota_usec"] = usec(show("sbmdr.slice", "CPUQuotaPerSecUSec"))
    report = {
        "status": "INSTALLED_NOT_QUALIFIED",
        "systemd": systemd,
        "release": str(release),
        "runtime": str(runtime),
        "runtime_build": "venv-at-runtime+require-hashes",
        "requirements_lock_sha256": hashlib.sha256(
            (release / "python/requirements/linux-x86_64-python312.lock").read_bytes()
        ).hexdigest(),
        "roots": roots,
        "binary_sha256": binary_sha256,
        "c3_baseline_binary_sha256": c3["binary_sha256"],
        "backup": str(backup_dir),
        "resource_policy": {
            "slice_cpu_quota_percent": 175,
            "archive_cpu_quota_percent": 50,
            "spot_cpu_weight": 100,
            "usdm_cpu_weight": 200,
            "recorder_restart": "no",
            "recorder_timeout_start_seconds": 300,
            "recorder_timeout_stop_seconds": 180,
            "archive_timeout_seconds": 600,
            "public_seal_target_bytes": 16 * 1024**2,
        },
        "logrotate": "disabled-until-production",
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
