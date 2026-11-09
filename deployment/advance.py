"""Gate the authorized C3 -> C4/C5 -> permanent service transition on real evidence."""

import argparse
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


def finish_production(release, roots):
    """Archive the verified tail and enable the already qualified services."""
    for root in roots.values():
        for _ in range(100):
            with sqlite3.connect(Path(root) / "state/catalog.sqlite") as db:
                pending = db.execute(
                    "SELECT count(*) FROM chunks WHERE state='SEALED'"
                ).fetchone()[0]
                pending += db.execute(
                    "SELECT count(*) FROM archive_transactions WHERE state!='LOCAL_DELETED'"
                ).fetchone()[0]
            if not pending:
                break
            run(
                "runuser",
                "-u",
                "bmdr",
                "--",
                release / "venv/bin/python",
                release / "prototype/operations.py",
                "archive",
                root,
                env={**os.environ, "PYTHONPATH": str(release / "python/src")},
            )
        else:
            raise RuntimeError("final archive backlog did not drain")
    drop = Path("/etc/systemd/system/sbmdr-recorder@.service.d")
    drop.mkdir(exist_ok=True)
    (drop / "production.conf").write_text("[Service]\nRestart=on-failure\n")
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
    a = p.parse_args()
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
        env = {**os.environ, "PYTHONPATH": str(a.source / "python/src")}
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
            "256",
            "--cpu-limit-percent",
            str(a.cpu_limit_percent),
            env={**os.environ, "PYTHONPATH": str(release / "python/src")},
        )
        qualification = json.loads((evidence / "progress.json").read_text())
        assert qualification["status"] == "PASS"
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
