"""Offline acceptance-boundary checks for the F4 tooling."""

import hashlib
import json
import sys
import tempfile
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "vendor/python-baseline/src"))
sys.path.insert(0, str(REPO / "prototype"))
sys.path.insert(0, str(REPO / "deployment"))

import archive_tick
import operations
import qualify

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def expect(exception, call, message):
    try:
        call()
    except exception:
        return
    raise AssertionError(f"expected {message}")


def chunk(chunk_id, records=10, state_row=True):
    manifest = {
        "chunk_id": chunk_id,
        "record_count": records,
        "uncompressed_bytes": records * 10,
        "stored_bytes": records * 5,
        "uncompressed_sha256": "a" * 64,
        "stored_sha256": "b" * 64,
    }
    return types.SimpleNamespace(chunk_id=chunk_id, manifest=manifest)


def catalog_row(item, state="SEALED"):
    return {
        "state": state,
        "record_count": item.manifest["record_count"],
        "uncompressed_bytes": item.manifest["uncompressed_bytes"],
        "stored_bytes": item.manifest["stored_bytes"],
        "uncompressed_sha256": item.manifest["uncompressed_sha256"],
        "stored_sha256": item.manifest["stored_sha256"],
    }


def test_chunk_sets():
    item = chunk("c1")
    summary = operations.reconcile_chunk_sets([item], {"c1": catalog_row(item)})
    assert summary["catalog_chunk_count"] == summary["manifest_chunk_count"] == 1
    expect(
        ValueError,
        lambda: operations.reconcile_chunk_sets([], {"c1": catalog_row(item)}),
        "a Catalog chunk without a manifest",
    )
    expect(
        ValueError,
        lambda: operations.reconcile_chunk_sets([item], {}),
        "a manifest without a Catalog chunk",
    )
    expect(
        ValueError,
        lambda: operations.reconcile_chunk_sets(
            [item], {"c1": catalog_row(item, state="SEALING")}
        ),
        "a non-terminal chunk",
    )
    broken = catalog_row(item)
    broken["record_count"] += 1
    expect(
        ValueError,
        lambda: operations.reconcile_chunk_sets([item], {"c1": broken}),
        "a record-count mismatch",
    )
    broken = catalog_row(item)
    broken["stored_sha256"] = "c" * 64
    expect(
        ValueError,
        lambda: operations.reconcile_chunk_sets([item], {"c1": broken}),
        "a hash mismatch",
    )
    return "chunk/manifest set, terminal state and identity checks"


def drop_inputs(ws=0, rest=0):
    recorder = {
        "dropped_total": ws + rest,
        "ws_dropped_total": ws,
        "rest_dropped_total": rest,
    }
    readback = {
        "ws_records": 1000,
        "ingress_drops": {
            "events": 1 if ws + rest else 0,
            "tail_events": 0,
            "dropped_ws": ws,
            "dropped_rest": rest,
            "dropped_total": ws + rest,
            "first_utc_ns": 1,
            "last_utc_ns": 2,
        },
    }
    return recorder, readback


def test_closed_capture():
    import sqlite3

    with tempfile.TemporaryDirectory(prefix="sbmdr-closed-") as tmp:
        root = Path(tmp)
        layout = ensure_storage_layout(root)
        with Catalog(layout.catalog) as catalog:
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="run",
                collector_version="test",
            )
            with writer:
                for index, flags in enumerate(((), ("ingress_drop", "sequence_gap"))):
                    writer.append(
                        EventEnvelope(
                            market="spot",
                            symbol="BTCUSDT",
                            stream="diff_depth",
                            module="binance.public.websocket.v1",
                            connection_id="run-ws-public",
                            collector_instance_id="run",
                            collector_version="test",
                            receive_time_utc_ns=index + 1,
                            receive_monotonic_ns=index + 1,
                            raw_payload=json.dumps({"_test_id": index}).encode(),
                            capture_flags=flags,
                        )
                    )
                writer.seal()
        with sqlite3.connect(layout.catalog) as db:
            db.execute(
                "INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,"
                "evidence_json) VALUES('drop-1','INGRESS_DROP',3,?)",
                (
                    json.dumps(
                        {
                            "market": "spot",
                            "symbol": "BTCUSDT",
                            "stream": "diff_depth",
                            "rest": False,
                            "dropped": 1,
                            "tail": False,
                            "first_drop_utc_ns": 2,
                            "last_drop_utc_ns": 3,
                        }
                    ),
                ),
            )
            db.commit()
        with Catalog(layout.catalog, read_only=True) as catalog:
            with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
                tables = {
                    row[0]
                    for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            report = operations._verify(layout, catalog, {}, tables)
        assert report["status"] == "PASS"
        assert report["records"] == report["ws_records"] == 2
        assert report["rest_records"] == 0
        assert report["ingress_drops"]["dropped_ws"] == 1
        assert report["fast_path"]["decoded_chunks"] == 1
        # A whole missing manifest must fail the set reconciliation.
        next(layout.manifests.glob("*.manifest.json")).unlink()
        with Catalog(layout.catalog, read_only=True) as catalog:
            expect(
                ValueError,
                lambda: operations._verify(layout, catalog, {}, tables),
                "a Catalog chunk with no manifest",
            )
    return "real sealed chunk, gap decode and missing-manifest rejection"


def test_drops():
    recorder, readback = drop_inputs()
    status = qualify.check_drops("spot", recorder, readback, 0.10)
    assert status["drop_percent"] == 0
    recorder, readback = drop_inputs(ws=1)
    readback["ingress_drops"]["dropped_ws"] = 0
    readback["ingress_drops"]["dropped_total"] = 0
    expect(
        AssertionError,
        lambda: qualify.check_drops("spot", recorder, readback, 0.10),
        "a recorder WS drop absent from Catalog",
    )
    recorder, readback = drop_inputs(ws=3)
    expect(
        AssertionError,
        lambda: qualify.check_drops("spot", recorder, readback, 0.10),
        "a drop ratio above the limit",
    )
    recorder, readback = drop_inputs(rest=1)
    readback["ingress_drops"]["dropped_rest"] = 99
    expect(
        AssertionError,
        lambda: qualify.check_drops("spot", recorder, readback, 0.10),
        "a REST drop mismatch",
    )
    recorder, readback = drop_inputs()
    readback["ws_records"] = 0
    expect(
        AssertionError,
        lambda: qualify.check_drops("spot", recorder, readback, 0.10),
        "an empty WS denominator",
    )
    return "WS/REST split, Catalog reconciliation, denominator and ratio"


def connection_report(long_lived=True):
    def ranges(prefix):
        span = [0, 83_000 * 10**9] if long_lived else [0, 100 * 10**9]
        return {
            f"{prefix}-ws-public": span,
            f"{prefix}-ws-public-1": [span[1], span[1] + 10**9],
        }

    def market(market, base):
        key = f"{market}/BTCUSDT/diff_depth"
        connections = [f"{base}-ws-public", f"{base}-ws-public-1"]
        return {
            "connection_monotonic_ranges_ns": ranges(base),
            "connections": {key: connections},
            "bridge_connections": {key: [connections[-1]]},
        }

    usdm = market("um_perpetual", "run")
    usdm["connection_monotonic_ranges_ns"]["run-ws-market"] = (
        [0, 83_000 * 10**9] if long_lived else [0, 100 * 10**9]
    )
    usdm["connection_monotonic_ranges_ns"]["run-ws-market-1"] = [
        ([83_000 * 10**9] if long_lived else [100 * 10**9])[0],
        ([83_000 * 10**9] if long_lived else [100 * 10**9])[0] + 10**9,
    ]
    return {
        "markets": {
            "spot": {"readback": market("spot", "run")},
            "usdm": {"readback": usdm},
        }
    }


def write_events(evidence, groups):
    for market, names in {
        "spot": ["spot-public"],
        "usdm": ["usdm-public", "usdm-market"],
    }.items():
        rows = []
        for name in names:
            if name in groups:
                rows.append(
                    {
                        "event": "WS_RECONNECT",
                        "group": name,
                        "reason": "23h_renewal",
                        "elapsed_seconds": 83_000,
                        "accepted": 10,
                    }
                )
        (evidence / f"{market}.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )


def test_connection_groups():
    symbols = {"spot": ["BTCUSDT"], "usdm": ["BTCUSDT"]}
    with tempfile.TemporaryDirectory(prefix="sbmdr-groups-") as tmp:
        evidence = Path(tmp)
        report = connection_report()
        write_events(evidence, {"spot-public", "usdm-public", "usdm-market"})
        qualify.check_connection_groups(report, symbols, evidence)
        assert report["connection_groups"]["usdm:usdm-market"]["renewal_triggered"]
        # One group loses its renewal event: the whole 24h check must fail.
        write_events(evidence, {"spot-public", "usdm-public"})
        expect(
            AssertionError,
            lambda: qualify.check_connection_groups(
                connection_report(), symbols, evidence
            ),
            "a missing usdm-market renewal",
        )
        # A short-lived group truthfully records that the trigger was not reached.
        short = connection_report(long_lived=False)
        write_events(evidence, set())
        qualify.check_connection_groups(short, symbols, evidence)
        assert not short["connection_groups"]["spot:spot-public"]["renewal_triggered"]
        assert "note" in short["connection_groups"]["spot:spot-public"]
    return "per-group renewal, truthful not-reached note and depth re-bridge"


def test_archive_blocked():
    assert archive_tick.classify(
        0, {"exit_reason": "TARGET_ABSENT", "backlog_files_after": 5}
    )["failed"]
    idle = archive_tick.classify(
        0, {"exit_reason": "TARGET_ABSENT", "backlog_files_after": 0}
    )
    assert idle["blocked"] and not idle["failed"]
    assert archive_tick.classify(
        0, {"exit_reason": "BACKLOG_EMPTY", "backlog_files_after": 0}
    )["failed"] is False
    assert archive_tick.classify(1, {"exit_reason": "ERROR"})["failed"]
    return "blocked target with backlog fails, idle target does not"


def test_verify_only():
    original = qualify.verify_stopped
    original_child = qualify.SystemdChild
    with tempfile.TemporaryDirectory(prefix="sbmdr-verify-only-") as tmp:
        base = Path(tmp)
        release = base / "release-root"
        (release / "release").mkdir(parents=True)
        binary = release / "release/sbmdr-c0"
        binary.write_bytes(b"offline-verify-only-fixture")
        config = base / "config.json"
        config.write_text(json.dumps({"spot": ["BTCUSDT"], "usdm": ["BTCUSDT"]}))
        root = base / "root"
        evidence = base / "evidence"
        evidence.mkdir()
        report = {
            "status": "PASS",
            "start_utc": "2026-11-11T00:00:00+00:00",
            "seconds_required": 1,
            "config": json.loads(config.read_text()),
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "markets": {},
        }
        for market in ("spot", "usdm"):
            market_root = root / market
            market_root.mkdir(parents=True)
            report["markets"][market] = {"root": str(market_root)}
            (evidence / f"{market}.json").write_text(
                json.dumps({"wall_seconds": 1, "stop_reason": "SIGNAL"})
            )
            (evidence / f"{market}.jsonl").write_text("")
        (evidence / "progress.json").write_text(json.dumps(report))
        marker = base / "verify-called"
        calls = {}

        def fake_verify(a, report, progress_path=None):
            calls["progress_path"] = progress_path
            marker.write_text("yes")

        def forbidden_child(market):
            raise AssertionError("verify-only started a service")

        qualify.verify_stopped = fake_verify
        qualify.SystemdChild = forbidden_child
        argv = sys.argv
        try:
            sys.argv = [
                "qualify.py",
                "--release",
                str(release),
                "--config",
                str(config),
                "--root",
                str(root),
                "--evidence",
                str(evidence),
                "--seconds",
                "1",
                "--verify-only",
            ]
            qualify.main()
        finally:
            qualify.verify_stopped = original
            qualify.SystemdChild = original_child
            sys.argv = argv
        assert marker.exists(), "verify-only did not call verify_stopped"
        assert calls["progress_path"].name == "verify-only-progress.json"
        written = json.loads((evidence / "verify-only.json").read_text())
        assert written["verification"]["mode"] == "verify-only"
        assert json.loads((evidence / "progress.json").read_text())["start_utc"] == (
            "2026-11-11T00:00:00+00:00"
        )
        # A different release must be rejected before any verification.
        bad = dict(report)
        bad["binary_sha256"] = "0" * 64
        (evidence / "progress.json").write_text(json.dumps(bad))
        argv = sys.argv
        try:
            sys.argv = argv[:1] + [
                "--release",
                str(release),
                "--config",
                str(config),
                "--root",
                str(root),
                "--evidence",
                str(evidence),
                "--seconds",
                "1",
                "--verify-only",
            ]
            expect(
                AssertionError,
                qualify.main,
                "a mismatched release identity",
            )
        finally:
            sys.argv = argv
    return "verify-only reuses stopped evidence and never starts a recorder"


def main():
    checks = [
        test_chunk_sets(),
        test_closed_capture(),
        test_drops(),
        test_connection_groups(),
        test_archive_blocked(),
        test_verify_only(),
    ]
    print(json.dumps({"status": "PASS", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
