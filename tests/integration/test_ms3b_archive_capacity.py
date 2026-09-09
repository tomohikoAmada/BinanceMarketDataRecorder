from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive import (
    ArchiveError,
    ArchiveManager,
    ArchiveTarget,
    archive_drain,
)
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.capacity import (
    VPS_PRODUCTION_V1,
    evaluate_capacity,
)
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.emergency import (
    DiskEmergencyCoordinator,
    EmergencyActions,
)
from binance_market_data_recorder.storage.forecast import (
    GIB,
    StorageForecaster,
    space_severity,
)
from binance_market_data_recorder.storage.layout import StorageLayout, ensure_storage_layout
from binance_market_data_recorder.storage.macos import StorageRegistry, VolumeInfo
from tests.archive_support import FixedVolumes, prepare_archive
from tests.integration.test_ms3b_multi_product_load import (
    PROFILE_D_IDENTITIES,
    _event,
    _Identity,
    _spool,
)


class _NoVolumes:
    def inventory(self) -> list[VolumeInfo]:
        return []


def _seal_archive_candidate(
    layout: StorageLayout, catalog: Catalog, identity: _Identity, ordinal: int
) -> dict[str, object]:
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market=identity.market,
        symbol=identity.symbol,
        stream=identity.stream,
        collector_instance_id=f"archive-{identity.market}-{identity.symbol}",
        collector_version="0.1.0+ms3b-test",
        durability_interval_seconds=0,
        created_at_utc_ns=1_700_000_000_000_000_000 + ordinal,
    )
    writer.append(_event(identity, 1))
    writer.close()
    return seal_partial(writer.path, layout=layout, catalog=catalog)


def test_archive_drain_finishes_one_sealed_candidate_for_each_profile_d_identity(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path / "archive")
    with Catalog(layout.catalog) as catalog:
        mountpoint = tmp_path / "archive" / "external-volume"
        target_root = mountpoint / "QuantData" / "BinanceRecorder"
        target_root.mkdir(parents=True)
        volume = VolumeInfo(
            disk_id="disk-ms3b",
            volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
            name="MS3-B Test Archive",
            filesystem_type="apfs",
            mountpoint=mountpoint,
            writable=True,
            internal=False,
            removable=True,
            total_bytes=100 * GIB,
            free_bytes=90 * GIB,
            observed_at_utc_ns=1,
        )
        registration = StorageRegistry(
            catalog=catalog, volumes=FixedVolumes(volume)
        ).register(target_root)
        target_row = catalog.storage_targets()[0]
        candidates = [
            _seal_archive_candidate(
                layout,
                catalog,
                _Identity(market, symbol, stream),
                ordinal,
            )
            for ordinal, (market, symbol, stream) in enumerate(
                PROFILE_D_IDENTITIES, start=1
            )
        ]
        active_identity = _Identity("spot", "BTCUSDT", "diff_depth")
        active_writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=active_identity.market,
            symbol=active_identity.symbol,
            stream=active_identity.stream,
            collector_instance_id="archive-active",
            collector_version="0.1.0+ms3b-test",
            durability_interval_seconds=0,
        )
        active_writer.append(_event(active_identity, 1))
        active_path = active_writer.path
        active_chunk_id = str(active_writer.header.chunk_id)
        target = ArchiveTarget(
            storage_id=str(registration["storage_id"]),
            volume_uuid=volume.volume_uuid,
            registered_relative_path=str(target_row["relative_path"]),
            marker_nonce=str(target_row["marker_nonce"]),
            root=target_root,
        )
        before = catalog.archive_aggregate(target.storage_id)
        assert before["unassigned_sealed_files"] == 42
        assert len(candidates) == 42

        try:
            result = archive_drain(
                layout=layout,
                catalog=catalog,
                storage_id=target.storage_id,
                max_runtime_seconds=60,
                max_files=42,
                volumes=FixedVolumes(volume),
            )
            assert result["processed_files"] == 42
            assert result["successful_transactions"] == 42
            assert result["backlog_files_after"] == 0
            assert catalog.state(active_chunk_id) is ChunkState.ACTIVE
            assert active_path.is_file()

            transactions = catalog.archive_transactions(storage_id=target.storage_id)
            assert len(transactions) == 42
            assert len({str(row["chunk_id"]) for row in transactions}) == 42
            assert len({str(row["target_relative_path"]) for row in transactions}) == 42
            assert all(row["state"] == "LOCAL_DELETED" for row in transactions)
            assert catalog.archive_transaction_for_chunk(active_chunk_id) is None

            observed_identities: set[tuple[str, str, str]] = set()
            hash_mismatches = 0
            identity_mismatches = 0
            for transaction in transactions:
                manifest_path = layout.root / str(
                    transaction["source_manifest_relative_path"]
                )
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                identity = (
                    str(manifest["market"]),
                    str(manifest["symbol"]),
                    str(manifest["stream"]),
                )
                observed_identities.add(identity)
                expected = set(PROFILE_D_IDENTITIES)
                if identity not in expected or str(transaction["chunk_id"]) != str(
                    manifest["chunk_id"]
                ):
                    identity_mismatches += 1
                external = target.root / str(transaction["target_relative_path"])
                if hashlib.sha256(external.read_bytes()).hexdigest() != str(
                    manifest["stored_sha256"]
                ):
                    hash_mismatches += 1
            assert observed_identities == set(PROFILE_D_IDENTITIES)
            assert identity_mismatches == 0
            assert hash_mismatches == 0
            assert catalog.archive_aggregate(target.storage_id)["backlog_files"] == 0
        finally:
            # Leave the active artifact present for the ineligibility proof;
            # tmp_path cleanup owns the test-only residual after the Catalog
            # connection and descriptor are closed.
            active_writer.abort()


def test_archive_failure_retry_is_idempotent_and_unavailable_target_retains_source(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path / "fault", chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def fail_after_copy(point: str, _path: Path | None) -> None:
            if point == "after_copy_fsync":
                raise RuntimeError("bounded copy failure")

        manager.fault_hook = fail_after_copy
        with pytest.raises(ArchiveError, match="bounded copy failure"):
            manager.run_once()
        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        assert transaction is not None
        source = prepared.layout.root / str(transaction["source_relative_path"])
        assert source.is_file()
        source_digest = hashlib.sha256(source.read_bytes()).hexdigest()

        manager.fault_hook = None
        retried = manager.run_once()
        assert retried.state == "LOCAL_DELETED"
        assert not source.exists()
        final = prepared.target.root / str(transaction["target_relative_path"])
        assert hashlib.sha256(final.read_bytes()).hexdigest() == source_digest
        assert catalog.state(prepared.chunk_ids[1]) is ChunkState.SEALED
        assert len(catalog.archive_transactions()) == 1

    unavailable = prepare_archive(tmp_path / "unavailable")
    with Catalog(unavailable.layout.catalog) as catalog:
        row = catalog.chunk(unavailable.chunk_ids[0])
        assert row is not None
        source = unavailable.layout.root / str(row["sealed_path"])
        result = archive_drain(
            layout=unavailable.layout,
            catalog=catalog,
            storage_id=unavailable.target.storage_id,
            max_runtime_seconds=5,
            max_files=1,
            volumes=_NoVolumes(),
        )
        assert result["exit_reason"] == "TARGET_ABSENT"
        assert result["backlog_files_before"] == 1
        assert result["backlog_files_after"] == 1
        assert source.is_file()
        assert catalog.state(unavailable.chunk_ids[0]) is ChunkState.SEALED


def test_capacity_growth_is_aggregate_and_topology_decomposition_invariant(
    tmp_path: Path,
) -> None:
    total = 100 * GIB
    free_values = [80 * GIB - ordinal * GIB for ordinal in range(8)]
    now = 7 * 3_600 * 1_000_000_000

    def record_history(
        path: Path, backlog_values: list[int]
    ) -> dict[str, object]:
        with Catalog(path) as catalog:
            forecaster = StorageForecaster(catalog=catalog, data_root=path.parent)
            for ordinal, (free, backlog) in enumerate(
                zip(free_values, backlog_values, strict=True)
            ):
                catalog.record_space_sample(
                    sample_id=f"sample-{ordinal}",
                    scope_id="internal",
                    storage_id=None,
                    observed_at_utc_ns=ordinal * 3_600 * 1_000_000_000,
                    total_bytes=total,
                    free_bytes=free,
                    archive_backlog_bytes=backlog,
                    oldest_unarchived_at_utc_ns=(
                        ordinal * 3_600 * 1_000_000_000 if backlog else None
                    ),
                    severity=space_severity(total, free),
                )
            result = forecaster.forecast(
                "internal",
                now_utc_ns=now,
                capacity_profile=VPS_PRODUCTION_V1,
            )
            assert result["capacity_state"] == "CRITICAL"
            return result

    first = record_history(tmp_path / "capacity-a.sqlite", [0] * 8)
    second = record_history(
        tmp_path / "capacity-b.sqlite",
        [
            1 * GIB,
            3 * GIB,
            5 * GIB,
            7 * GIB,
            9 * GIB,
            11 * GIB,
            13 * GIB,
            15 * GIB,
        ],
    )
    assert first["capacity_state"] == second["capacity_state"]
    assert first["threshold_bytes"] == second["threshold_bytes"]
    assert first["net_growth"] == second["net_growth"]
    assert first["capacity_profile"] == VPS_PRODUCTION_V1.profile_id
    assert "product_count" not in first


def test_capacity_state_is_global_and_hard_reserve_covers_all_42_identities(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path / "hard-reserve")
    with Catalog(layout.catalog) as catalog:
        spools: list[StreamSpool] = []
        identities = [_Identity(*item) for item in PROFILE_D_IDENTITIES]
        for identity in identities:
            spool = _spool(layout, catalog, identity)
            spool.enqueue(_event(identity, 1))
            assert spool.drain_one()
            spools.append(spool)

        calls: list[str] = []
        gap_identities: list[tuple[str, str, str]] = []

        def seal_active() -> None:
            for spool in spools:
                spool.close_and_seal()
            calls.append("seal")

        decision = evaluate_capacity(
            profile=VPS_PRODUCTION_V1,
            scope_id="internal",
            total_bytes=100 * GIB,
            free_bytes=10 * GIB,
            hard_reserve_eta={"status": "NOT_APPROACHING"},
            now_utc_ns=1,
        )
        coordinator = DiskEmergencyCoordinator(
            catalog=catalog,
            actions=EmergencyActions(
                suspend_non_core=lambda: calls.append("suspend"),
                prioritize_verified_archive=lambda: calls.append("archive"),
                seal_active=seal_active,
                stop_collectors=lambda: calls.append("stop"),
                open_gap=lambda _at: gap_identities.extend(
                    (item.market, item.symbol, item.stream) for item in identities
                ),
            ),
            rotation_bytes=128 * 1024**2,
        )
        result = coordinator.apply(
            total_bytes=100 * GIB,
            free_bytes=10 * GIB,
            observed_at_utc_ns=1,
            capacity_decision=decision,
        )
        assert result["capacity_state"] == "HARD_RESERVE"
        assert calls == ["suspend", "archive", "seal", "stop"]
        assert len(catalog.operational_events(event_type="DISK_EMERGENCY_STOP")) == 1
        assert len(gap_identities) == 42
        assert len(set(gap_identities)) == 42
        assert set(gap_identities) == set(PROFILE_D_IDENTITIES)
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 42
        assert len(catalog.chunks_in_states(ChunkState.ACTIVE)) == 0

        document = StorageForecaster(
            catalog=catalog, data_root=layout.root
        ).document(["internal"], now_utc_ns=1, capacity_profile=VPS_PRODUCTION_V1)
        assert len(cast(list[dict[str, Any]], document["targets"])) == 1


def test_external_low_space_does_not_allocate_or_discard_internal_raw(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path / "low-space")
    low_space_volume = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared.target.root.parents[1],
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * GIB,
        free_bytes=5 * GIB,
        observed_at_utc_ns=2,
    )
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None
        source = prepared.layout.root / str(row["sealed_path"])
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=5,
            max_files=1,
            volumes=FixedVolumes(low_space_volume),
        )
        assert result["exit_reason"] == "TARGET_LOW_SPACE"
        assert result["backlog_files_before"] == 1
        assert result["backlog_files_after"] == 1
        assert source.is_file()
        assert catalog.archive_transactions() == []
