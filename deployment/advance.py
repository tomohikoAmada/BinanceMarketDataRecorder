"""Gate the authorized C3 -> C4/C5 -> permanent service transition on real evidence."""

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


def run(*args, env=None):
    subprocess.run([str(a) for a in args], env=env, check=True)


BLOCKED_ARCHIVE_REASONS = {
    "TARGET_ABSENT",
    "TARGET_NOT_READY",
    "TARGET_LOW_SPACE",
}


def archive_pending(root):
    with sqlite3.connect(Path(root) / "state/catalog.sqlite") as db:
        pending = db.execute(
            "SELECT count(*) FROM chunks WHERE state='SEALED'"
        ).fetchone()[0]
        pending += db.execute(
            "SELECT count(*) FROM archive_transactions WHERE state!='LOCAL_DELETED'"
        ).fetchone()[0]
    return pending


def finish_production(release, roots):
    """Archive the verified tail and enable the already qualified services."""
    env = {**os.environ, "PYTHONPATH": str(release / "python/src")}
    for root in roots.values():
        for _ in range(100):
            pending = archive_pending(root)
            if not pending:
                break
            result = subprocess.run(
                [
                    "runuser",
                    "-u",
                    "bmdr",
                    "--",
                    str(release / "venv/bin/python"),
                    str(release / "prototype/operations.py"),
                    "archive",
                    str(root),
                ],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
            parsed = {}
            if result.stdout.strip():
                try:
                    parsed = json.loads(result.stdout)
                except json.JSONDecodeError:
                    parsed = {}
            reason = parsed.get("exit_reason")
            if result.returncode != 0 or parsed.get("failed_transactions"):
                raise RuntimeError(
                    f"final archive failed for {root}: exit={result.returncode} "
                    f"reason={reason} stderr={result.stderr.strip()}"
                )
            if reason in BLOCKED_ARCHIVE_REASONS and parsed.get("backlog_files_after"):
                raise RuntimeError(
                    f"final archive blocked for {root}: {reason} "
                    f"backlog={parsed.get('backlog_files_after')}"
                )
            if archive_pending(root) >= pending:
                raise RuntimeError(
                    f"final archive made no progress for {root}: "
                    f"pending={pending} reason={reason}"
                )
        else:
            raise RuntimeError("final archive backlog did not drain")
    drop = Path("/etc/systemd/system/sbmdr-recorder@.service.d")
    drop.mkdir(exist_ok=True)
    (drop / "production.conf").write_text("[Service]\nRestart=on-failure\n")
    disabled = Path("/etc/logrotate.d/sbmdr.qualification-disabled")
    if disabled.exists():
        disabled.unlink()
    shutil.copyfile(release / "deployment/logrotate.conf", "/etc/logrotate.d/sbmdr")
    run("systemctl", "daemon-reload")
    run(
        "systemctl",
        "enable",
        "--now",
        "sbmdr-recorder@spot.service",
        "sbmdr-recorder@usdm.service",
        "sbmdr-archive.timer",
    )
    for market in ("spot", "usdm"):
        run("systemctl", "is-active", f"sbmdr-recorder@{market}.service")
        run("systemctl", "is-enabled", f"sbmdr-recorder@{market}.service")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--c3-report", type=Path, required=True)
    p.add_argument("--archive-root", type=Path, action="append", default=[])
    p.add_argument(
        "--cpu-limit-percent", type=float, default=120,
        help="CPU ceiling in percent of one core; use the owner-approved scope",
    )
    p.add_argument(
        "--finish-only",
        action="store_true",
        help="Resume only the final tail archive and service enablement from a PASS report",
    )
    p.add_argument(
        "--qualify-only",
        action="store_true",
        help="Install and run C4-C qualification, but do not enable production (C5)",
    )
    p.add_argument("--qualification-report", type=Path)
    p.add_argument("--release", type=Path)
    a = p.parse_args()
    if a.finish_only:
        assert a.qualification_report and a.release, (
            "--finish-only requires --qualification-report and --release"
        )
        qualification = json.loads(a.qualification_report.read_text())
        assert qualification["status"] == "PASS", "qualification report is not PASS"
        binary = a.release / "release/sbmdr-c0"
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == qualification[
            "binary_sha256"
        ], "release binary differs from the qualification report"
        installed = json.loads((a.source / "results/deployment.json").read_text())
        assert Path(installed["release"]) == a.release, "release root mismatch"
        assert Path(installed["evidence"]) == a.qualification_report.parent, (
            "qualification report does not belong to the installed evidence root"
        )
        finish_production(a.release, installed["roots"])
        print(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "release": str(a.release),
                    "qualification": str(a.qualification_report),
                },
                indent=2,
            )
        )
        return
    state = a.source / "results/promotion-progress.json"
    report = {
        "status": "WAIT_C3",
        "version": a.version,
        "cpu_limit_percent": a.cpu_limit_percent,
    }

    def save():
        state.write_text(json.dumps(report, indent=2) + "\n")

    save()
    try:
        while True:
            if a.c3_report.exists():
                c3 = json.loads(a.c3_report.read_text())
                if c3["status"] == "PASS":
                    break
                if c3["status"] == "FAILED":
                    raise RuntimeError("C3 failed; promotion stopped")
            time.sleep(30)
        report["status"] = "WAIT_SANITIZERS"
        save()
        for mode in ("asan", "tsan"):
            path = a.source / f"results/ctest-{mode}-pinned.exit"
            while not path.exists():
                time.sleep(30)
            if path.read_text().strip() != "0":
                raise RuntimeError(f"{mode} failed")
            shutil.copyfile(path, a.source / f"results/ctest-{mode}.exit")
        for market in ("spot", "usdm"):
            consumer = json.loads(
                (a.source / f"results/normalize-{market}-sources.json").read_text()
            )
            assert consumer["source_adapter"] == "sbmdr-combined-ws.v1"
            assert consumer["valid_rows"] > 0 and consumer["invalid_rows"] == 0
        faults = json.loads(
            (a.source / "results/archive-faults-final.json").read_text()
        )
        assert len(faults) == 2 and all(
            f["source_preserved_after_failure"] and f["retry_completed"] for f in faults
        )
        assert (
            json.loads((a.source / "results/history-smoke.json").read_text())["status"]
            == "IMPORTED"
        )
        env = {
            **os.environ,
            "PYTHONPATH": str(a.source / "vendor/python-baseline/src"),
        }
        # Release system space only through the existing verified archive transaction.
        for root in [Path(v["root"]) for v in c3["markets"].values()] + a.archive_root:
            target = (
                Path("/srv/recorder-data/S-BMDR") / f"{root.parent.name}-{root.name}"
            )
            run(
                sys.executable,
                a.source / "prototype/operations.py",
                "register",
                root,
                "--target",
                target,
                env=env,
            )
            for _ in range(100):
                with sqlite3.connect(root / "state/catalog.sqlite") as db:
                    pending = db.execute(
                        "SELECT count(*) FROM chunks WHERE state='SEALED'"
                    ).fetchone()[0]
                    pending += db.execute(
                        "SELECT count(*) FROM archive_transactions WHERE state!='LOCAL_DELETED'"
                    ).fetchone()[0]
                if not pending:
                    break
                run(
                    sys.executable,
                    a.source / "prototype/operations.py",
                    "archive",
                    root,
                    env=env,
                )
            else:
                raise RuntimeError("C3 archive backlog did not drain")
        report["status"] = "INSTALLING"
        save()
        run(
            sys.executable,
            a.source / "deployment/install.py",
            "--source",
            a.source,
            "--version",
            a.version,
            "--c3-report",
            a.c3_report,
        )
        installed = json.loads((a.source / "results/deployment.json").read_text())
        release = Path(installed["release"])
        evidence = Path(installed["evidence"])
        roots = installed["roots"]
        base = Path(roots["spot"]).parent
        assert Path(roots["usdm"]).parent == base
        report.update(
            status="C4_C5_RUNNING",
            release=str(release),
            roots=roots,
            evidence=str(evidence),
            start_utc=datetime.now(UTC).isoformat(),
        )
        save()
        run("systemctl", "start", "sbmdr-archive.timer")
        run(
            release / "venv/bin/python",
            release / "deployment/qualify.py",
            "--release",
            release,
            "--config",
            "/etc/sbmdr/config.json",
            "--root",
            base,
            "--evidence",
            evidence,
            "--seconds",
            "86410",
            "--services",
            "--rss-limit-mib",
            "384",
            "--cpu-limit-percent",
            str(a.cpu_limit_percent),
            "--max-drop-percent",
            "0.10",
            "--queue-limit-mib",
            "128",
            env={**os.environ, "PYTHONPATH": str(release / "python/src")},
        )
        qualification = json.loads((evidence / "progress.json").read_text())
        assert qualification["status"] == "PASS"
        if a.qualify_only:
            report.update(
                status="QUALIFIED",
                qualification=str(evidence / "progress.json"),
                qualification_finish_utc=datetime.now(UTC).isoformat(),
                queue_limit_mib=128,
            )
            save()
            print(
                json.dumps(
                    {
                        "status": "QUALIFIED",
                        "release": str(release),
                        "roots": roots,
                        "evidence": str(evidence),
                        "qualification": str(evidence / "progress.json"),
                    },
                    indent=2,
                )
            )
            return
        report["status"] = "FINAL_ARCHIVE"
        save()
        finish_production(release, roots)
        report.update(
            status="COMPLETE",
            qualification=str(evidence / "progress.json"),
            production_start_utc=datetime.now(UTC).isoformat(),
        )
        save()
    except BaseException as error:
        report.update(status="FAILED", error=repr(error))
        save()
        raise


if __name__ == "__main__":
    main()
