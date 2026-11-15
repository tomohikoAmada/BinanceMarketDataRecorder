"""Unit and integration tests for M5-A read-only export publisher and SQLite backup bounds."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time

import pytest

ROOT = Path(__file__).resolve().parents[2]
for _path in (ROOT / "vendor/python-baseline/src", ROOT / "prototype"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from binance_market_data_recorder.storage.catalog import Catalog  # noqa: E402
from binance_market_data_recorder.storage.layout import StorageLayout  # noqa: E402

_PUBLISHER_SPEC = importlib.util.spec_from_file_location(
    "publish_mcp_export",
    Path(__file__).resolve().parents[1] / "publish_mcp_export.py",
)
assert _PUBLISHER_SPEC is not None and _PUBLISHER_SPEC.loader is not None
publish_mcp_export = importlib.util.module_from_spec(_PUBLISHER_SPEC)
sys.modules[_PUBLISHER_SPEC.name] = publish_mcp_export
_PUBLISHER_SPEC.loader.exec_module(publish_mcp_export)
CatalogBackupTimeoutError = publish_mcp_export.CatalogBackupTimeoutError
bounded_catalog_backup = publish_mcp_export.bounded_catalog_backup


def _create_partition(
    parent: Path,
    *,
    name: str,
    partition_id: str,
    state: str,
    start_ns: int,
    end_ns: int,
    is_cold: bool = False,
) -> Path:
    root = parent / name if is_cold else parent / "partitions" / name
    for sub in ("state", "data/active", "data/sealed", "data/manifests"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (root / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": partition_id,
                "created_at_utc_ns": start_ns,
                "receive_time_utc_range_ns": {"min": start_ns, "max": end_ns},
                "state": state,
            }
        ),
        encoding="utf-8",
    )
    layout = StorageLayout.from_root(root)
    with Catalog(layout.catalog) as cat:
        cat._connection.commit()
    return root


def _insert_chunk_and_archive(
    catalog_path: Path,
    *,
    chunk_id: str,
    chunk_state: str,
    created_ns: int,
    archive_target_dir: Path | None = None,
    storage_id: str | None = None,
    archive_state: str | None = None,
    market: str = "spot",
    stream: str = "aggTrade",
    target_rel_path: str | None = None,
    create_target_file: bool = True,
    payload: bytes = b"dummy-zst-payload",
) -> None:
    sha = hashlib.sha256(payload).hexdigest()
    conn = sqlite3.connect(catalog_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            INSERT INTO chunks(
                chunk_id, state, partial_path, sealed_path, manifest_path,
                record_count, uncompressed_bytes, stored_bytes,
                uncompressed_sha256, stored_sha256, created_at_utc_ns, updated_at_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chunk_id,
                chunk_state,
                None,
                None,
                None,
                42,
                len(payload) * 2,
                len(payload),
                sha,
                sha,
                created_ns,
                created_ns + 1,
            ),
        )
        if archive_state is not None and storage_id is not None and target_rel_path is not None:
            ext_manifest_rel = f"manifests/{chunk_id}.archive-manifest.json"
            conn.execute(
                """
                INSERT OR IGNORE INTO storage_targets(
                    storage_id, volume_uuid, volume_name, filesystem_type,
                    relative_path, marker_nonce, registered_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    storage_id,
                    "vol-test-0001",
                    "testvol",
                    "ext4",
                    archive_target_dir.name if archive_target_dir else market,
                    "nonce-1",
                    created_ns,
                ),
            )
            conn.execute(
                """
                INSERT INTO archive_transactions(
                    transaction_id, chunk_id, storage_id, state, market, stream,
                    source_relative_path, source_manifest_relative_path, source_manifest_sha256,
                    target_relative_path, target_temp_relative_path, external_manifest_relative_path,
                    stored_bytes, stored_sha256, created_at_utc_ns, updated_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"tx-{chunk_id}",
                    chunk_id,
                    storage_id,
                    archive_state,
                    market,
                    stream,
                    target_rel_path,
                    ext_manifest_rel,
                    sha,
                    target_rel_path,
                    f".{chunk_id}.tmp",
                    ext_manifest_rel,
                    len(payload),
                    sha,
                    created_ns,
                    created_ns + 2,
                ),
            )
            if archive_target_dir is not None and create_target_file:
                target_file = archive_target_dir / target_rel_path
                target_file.parent.mkdir(parents=True, exist_ok=True)
                target_file.write_bytes(payload)
                manifest_file = archive_target_dir / ext_manifest_rel
                manifest_file.parent.mkdir(parents=True, exist_ok=True)
                manifest_file.write_text(
                    json.dumps({"chunk_id": chunk_id, "artifact_relative_path": target_rel_path}),
                    encoding="utf-8",
                )
        conn.commit()
    finally:
        conn.close()


def _write_configs(
    base: Path,
    *,
    roots: dict[str, object],
    cold_roots: dict[str, object],
    archive_targets: dict[str, object],
    legacy_roots: dict[str, object] | None = None,
) -> tuple[Path, Path, Path, Path]:
    cfg_dir = base / "etc-sbmdr"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    roots_path = cfg_dir / "roots.json"
    cold_path = cfg_dir / "metadata-roots.json"
    archive_path = cfg_dir / "archive-targets.json"
    legacy_path = cfg_dir / "legacy-roots.json"
    roots_path.write_text(json.dumps(roots), encoding="utf-8")
    cold_path.write_text(json.dumps(cold_roots), encoding="utf-8")
    archive_path.write_text(json.dumps(archive_targets), encoding="utf-8")
    if legacy_roots is not None:
        legacy_path.write_text(json.dumps(legacy_roots), encoding="utf-8")
    return roots_path, cold_path, archive_path, legacy_path


def test_catalog_backup_bounded_timeout_and_concurrent_writer(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.sqlite"
    with Catalog(db_path) as writer:
        # Insert enough operational events so multiple 1-page backup steps occur.
        with writer._transaction() as conn:
            for idx in range(200):
                conn.execute(
                    """
                    INSERT INTO operational_events(
                        event_id, event_type, occurred_at_utc_ns, evidence_json
                    ) VALUES (?, 'SERVICE_STARTED', ?, '{}')
                    """,
                    (f"evt-{idx}", 1_000 + idx),
                )

        errors: list[BaseException] = []
        stop_flag = threading.Event()

        def _concurrent_writer() -> None:
            try:
                idx = 200
                while not stop_flag.is_set() and idx < 260:
                    with writer._transaction() as conn:
                        conn.execute(
                            """
                            INSERT INTO operational_events(
                                event_id, event_type, occurred_at_utc_ns, evidence_json
                            ) VALUES (?, 'SERVICE_STARTED', ?, '{}')
                            """,
                            (f"evt-{idx}", 1_000 + idx),
                        )
                    idx += 1
                    time.sleep(0.002)
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=_concurrent_writer, daemon=True)
        thread.start()
        try:
            dst_path = tmp_path / "backup.sqlite"
            with Catalog.open_live_read_only(db_path) as reader:
                dst_conn = sqlite3.connect(dst_path)
                try:
                    bounded_catalog_backup(reader, dst_conn, pages=8, timeout_seconds=5.0)
                    dst_conn.commit()
                finally:
                    dst_conn.close()
        finally:
            stop_flag.set()
            thread.join(timeout=2.0)

        assert not errors
        check = sqlite3.connect(dst_path)
        try:
            assert check.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
            count = check.execute("SELECT COUNT(*) FROM operational_events").fetchone()[0]
            assert count >= 200
        finally:
            check.close()

        # Verify bounded timeout raises CatalogBackupTimeoutError when budget expires mid-copy.
        with Catalog.open_live_read_only(db_path) as reader:
            timeout_dst = sqlite3.connect(tmp_path / "timeout.sqlite")
            try:
                def _slow_progress(_status: int, _remaining: int, _total: int) -> None:
                    time.sleep(0.02)

                with pytest.raises(CatalogBackupTimeoutError):
                    bounded_catalog_backup(
                        reader,
                        timeout_dst,
                        pages=1,
                        progress=_slow_progress,
                        sleep=0.0,
                        timeout_seconds=0.01,
                    )
            finally:
                timeout_dst.close()


def test_publish_discovers_dynamic_roots_and_chunk_readiness(tmp_path: Path) -> None:
    active_spot = tmp_path / "active-custom/spot"
    cold_spot = tmp_path / "cold-custom/spot"
    legacy_usdm = tmp_path / "legacy-run/usdm"
    archive_spot = tmp_path / "archive-custom/spot-target"
    archive_usdm = tmp_path / "archive-custom/usdm-target"
    export_root = tmp_path / "mcp-export"

    cold_part = _create_partition(
        cold_spot,
        name="2026-11-13-spot",
        partition_id="2026-11-13-spot",
        state="closed",
        start_ns=1000,
        end_ns=2000,
        is_cold=True,
    )
    active_part = _create_partition(
        active_spot,
        name="2026-11-14-spot",
        partition_id="2026-11-14-spot",
        state="open",
        start_ns=2000,
        end_ns=3000,
        is_cold=False,
    )
    legacy_part = _create_partition(
        legacy_usdm,
        name="2026-11-12-usdm",
        partition_id="2026-11-12-usdm",
        state="closed",
        start_ns=500,
        end_ns=1000,
        is_cold=False,
    )

    # Cold spot chunk: verified .bmdr.zst present -> READY
    _insert_chunk_and_archive(
        cold_part / "state/catalog.sqlite",
        chunk_id="chunk-cold-bmdr",
        chunk_state="LOCAL_DELETED",
        created_ns=1100,
        archive_target_dir=archive_spot,
        storage_id="sid-spot-1",
        archive_state="LOCAL_DELETED",
        market="spot",
        target_rel_path="raw/abcd1234.bmdr.zst",
        create_target_file=True,
    )
    # Active spot: 1 ready .raw.zst, 1 active chunk (pending), 1 sealed unarchived (pending),
    # and 1 LOCAL_DELETED whose file is missing from archive target (pending!).
    _insert_chunk_and_archive(
        active_part / "state/catalog.sqlite",
        chunk_id="chunk-active-ready-raw",
        chunk_state="LOCAL_DELETED",
        created_ns=2100,
        archive_target_dir=archive_spot,
        storage_id="sid-spot-1",
        archive_state="VERIFIED",
        market="spot",
        target_rel_path="raw/ef5678.raw.zst",
        create_target_file=True,
    )
    _insert_chunk_and_archive(
        active_part / "state/catalog.sqlite",
        chunk_id="chunk-active-open",
        chunk_state="ACTIVE",
        created_ns=2200,
    )
    _insert_chunk_and_archive(
        active_part / "state/catalog.sqlite",
        chunk_id="chunk-sealed-unarchived",
        chunk_state="SEALED",
        created_ns=2300,
    )
    _insert_chunk_and_archive(
        active_part / "state/catalog.sqlite",
        chunk_id="chunk-missing-artifact",
        chunk_state="LOCAL_DELETED",
        created_ns=2400,
        archive_target_dir=archive_spot,
        storage_id="sid-spot-1",
        archive_state="LOCAL_DELETED",
        market="spot",
        target_rel_path="raw/missing.bmdr.zst",
        create_target_file=False,
    )
    # Insert quarantine & gap rows in active spot partition.
    conn = sqlite3.connect(active_part / "state/catalog.sqlite")
    try:
        conn.execute(
            """
            INSERT INTO quarantined_artifacts(artifact_id, relative_path, reason, sha256, quarantined_at_utc_ns)
            VALUES ('q-1', 'data/sealed/bad.zst', 'corrupt_checksum', 'deadbeef', 2500)
            """
        )
        conn.execute(
            """
            INSERT INTO source_availability_gaps(gap_id, source_id, started_at_utc_ns, ended_at_utc_ns, evidence_json)
            VALUES ('gap-1', 'spot:BTCUSDT:aggTrade', 2250, 2260, '{"reason":"ws_reconnect"}')
            """
        )
        conn.commit()
    finally:
        conn.close()

    # Legacy root usdm chunk: verified .bmdr.zst present -> READY
    _insert_chunk_and_archive(
        legacy_part / "state/catalog.sqlite",
        chunk_id="chunk-legacy-usdm",
        chunk_state="LOCAL_DELETE_PENDING",
        created_ns=600,
        archive_target_dir=archive_usdm,
        storage_id="sid-usdm-legacy",
        archive_state="LOCAL_DELETE_PENDING",
        market="um_perpetual",
        stream="depth",
        target_rel_path="raw/legacy99.bmdr.zst",
        create_target_file=True,
    )

    roots_cfg, cold_cfg, archive_cfg, legacy_cfg = _write_configs(
        tmp_path,
        roots={"spot": str(active_spot)},
        cold_roots={"spot": str(cold_spot)},
        archive_targets={"spot": str(archive_spot), "usdm": str(archive_usdm)},
        legacy_roots={"usdm": [str(legacy_usdm)]},
    )

    configs = publish_mcp_export.discover_market_configs(
        roots_config=roots_cfg,
        metadata_roots_config=cold_cfg,
        archive_targets_config=archive_cfg,
        legacy_roots_config=legacy_cfg,
    )
    nginx_out = tmp_path / "nginx-export.conf"
    latest = publish_mcp_export.publish_export(
        export_root=export_root,
        configs=configs,
        nginx_conf_out=nginx_out,
    )

    assert latest["partition_count"] == 3
    assert latest["counts"] == {
        "total": 6,
        "ready": 3,
        "pending": 3,
        "quarantined": 1,
        "gaps": 1,
    }
    assert latest["manifest_url_path"].startswith("export/revisions/")

    manifest = json.loads((export_root / latest["manifest_rel_path"]).read_text("utf-8"))
    by_pid = {p["partition_id"]: p for p in manifest["partitions"]}

    # Verify exact relative paths (.bmdr.zst vs .raw.zst) preserved without hardcoding extension.
    spot_active_chunks = {
        json.loads(line)["chunk_id"]: json.loads(line)
        for line in (
            export_root / by_pid["2026-11-14-spot"]["files"]["chunks_jsonl"]["url_path"].removeprefix("export/")
        ).read_text("utf-8").splitlines()
    }
    assert spot_active_chunks["chunk-active-ready-raw"]["ready"] is True
    assert spot_active_chunks["chunk-active-ready-raw"]["archive_url_path"] == (
        "archive/sid-spot-1/raw/ef5678.raw.zst"
    )
    assert spot_active_chunks["chunk-active-open"]["ready"] is False
    assert spot_active_chunks["chunk-active-open"]["pending_reason"] == "active_chunk"
    assert spot_active_chunks["chunk-sealed-unarchived"]["ready"] is False
    assert spot_active_chunks["chunk-missing-artifact"]["ready"] is False
    assert spot_active_chunks["chunk-missing-artifact"]["pending_reason"] == "archive_artifact_missing"

    spot_cold_chunks = {
        json.loads(line)["chunk_id"]: json.loads(line)
        for line in (
            export_root / by_pid["2026-11-13-spot"]["files"]["chunks_jsonl"]["url_path"].removeprefix("export/")
        ).read_text("utf-8").splitlines()
    }
    assert spot_cold_chunks["chunk-cold-bmdr"]["archive_url_path"] == (
        "archive/sid-spot-1/raw/abcd1234.bmdr.zst"
    )

    # Check quarantine + gap visibility
    avail = json.loads(
        (
            export_root
            / by_pid["2026-11-14-spot"]["files"]["availability_json"]["url_path"].removeprefix("export/")
        ).read_text("utf-8")
    )
    assert avail["quarantined_artifacts"][0]["artifact_id"] == "q-1"
    assert avail["gaps"][0]["gap_id"] == "gap-1"

    # Check Nginx config exposes ONLY /export/ and /archive/<sid>/(raw|manifests)/ safely
    nginx_text = nginx_out.read_text("utf-8")
    assert "autoindex off;" in nginx_text
    assert "gzip off;" in nginx_text
    assert "limit_except GET HEAD { deny all; }" in nginx_text
    assert 'add_header Cache-Control "no-store" always;' in nginx_text
    assert "/archive/sid-spot-1/raw/" in nginx_text
    assert "/archive/sid-usdm-legacy/manifests/" in nginx_text
    assert str(active_spot) not in nginx_text
    assert str(legacy_usdm) not in nginx_text


def test_cold_partition_reuse_r5a_invalidation_and_timeout_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active_spot = tmp_path / "active/spot"
    cold_spot = tmp_path / "cold/spot"
    archive_spot = tmp_path / "archive/spot"
    export_root = tmp_path / "mcp-export"

    cold_part = _create_partition(
        cold_spot,
        name="2026-11-13-spot",
        partition_id="2026-11-13-spot",
        state="closed",
        start_ns=1000,
        end_ns=2000,
        is_cold=True,
    )
    active_part = _create_partition(
        active_spot,
        name="2026-11-14-spot",
        partition_id="2026-11-14-spot",
        state="open",
        start_ns=2000,
        end_ns=3000,
        is_cold=False,
    )
    _insert_chunk_and_archive(
        cold_part / "state/catalog.sqlite",
        chunk_id="cold-1",
        chunk_state="LOCAL_DELETED",
        created_ns=1100,
        archive_target_dir=archive_spot,
        storage_id="sid-1",
        archive_state="LOCAL_DELETED",
        target_rel_path="raw/cold1.bmdr.zst",
        create_target_file=True,
    )
    # Remove WAL on cold catalog to mirror cold-migrated state.
    for ext in ("-wal", "-shm"):
        p = Path(f"{cold_part / 'state/catalog.sqlite'}{ext}")
        if p.exists():
            p.unlink()

    _insert_chunk_and_archive(
        active_part / "state/catalog.sqlite",
        chunk_id="act-1",
        chunk_state="ACTIVE",
        created_ns=2100,
    )

    roots_cfg, cold_cfg, archive_cfg, legacy_cfg = _write_configs(
        tmp_path,
        roots={"spot": str(active_spot)},
        cold_roots={"spot": str(cold_spot)},
        archive_targets={"spot": str(archive_spot)},
    )
    configs = publish_mcp_export.discover_market_configs(
        roots_config=roots_cfg,
        metadata_roots_config=cold_cfg,
        archive_targets_config=archive_cfg,
        legacy_roots_config=legacy_cfg,
    )

    # Tick 1: both backed up.
    r1 = publish_mcp_export.publish_export(
        export_root=export_root, configs=configs, retain_revisions=2
    )
    assert r1["telemetry"]["backed_up_partitions"] == 2
    assert r1["telemetry"]["reused_partitions"] == 0

    # Tick 2: cold partition unchanged -> reused! Active partition always backed up.
    r2 = publish_mcp_export.publish_export(
        export_root=export_root, configs=configs, retain_revisions=2
    )
    assert r2["telemetry"]["backed_up_partitions"] == 1
    assert r2["telemetry"]["reused_partitions"] == 1

    # Simulate backup timeout mid-publish -> latest.json MUST stay on r2 and staging cleaned up.
    def _boom_backup(self, destination, **kwargs):  # type: ignore[no-untyped-def]
        raise CatalogBackupTimeoutError("simulated timeout")

    with monkeypatch.context() as mp:
        mp.setattr(publish_mcp_export, "bounded_catalog_backup", _boom_backup)
        with pytest.raises(CatalogBackupTimeoutError):
            publish_mcp_export.publish_export(
                export_root=export_root, configs=configs, retain_revisions=2
            )
    latest_after_fail = json.loads((export_root / "latest.json").read_text("utf-8"))
    assert latest_after_fail["revision_id"] == r2["revision_id"]
    assert not list(export_root.glob(".staging-*"))

    # Simulate R5-a metadata repair updating cold partition.json -> forces re-backup + new revision!
    pjson = cold_part / "partition.json"
    pdata = json.loads(pjson.read_text("utf-8"))
    pdata["receive_time_utc_range_ns"]["min"] = 900
    pjson.write_text(json.dumps(pdata), encoding="utf-8")

    r3 = publish_mcp_export.publish_export(
        export_root=export_root, configs=configs, retain_revisions=2
    )
    assert r3["telemetry"]["backed_up_partitions"] == 2
    assert r3["telemetry"]["reused_partitions"] == 0

    # Retain=2 should keep r2 and r3 and prune r1!
    revs = sorted(p.name for p in (export_root / "revisions").iterdir())
    assert revs == sorted([r2["revision_id"], r3["revision_id"]])
