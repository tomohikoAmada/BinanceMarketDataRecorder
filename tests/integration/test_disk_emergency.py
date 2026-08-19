from __future__ import annotations

from pathlib import Path
from typing import cast

from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.capacity import (
    VPS_PRODUCTION_V1,
    evaluate_capacity,
)
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.emergency import (
    DiskEmergencyCoordinator,
    EmergencyActions,
)
from binance_market_data_recorder.storage.forecast import GIB
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


def test_hard_reserve_seals_stops_emits_event_and_opens_gap(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    calls: list[str] = []
    gaps: list[int] = []
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="emergency-test",
            collector_version="0.1.0+test",
            queue_capacity=2,
            rotation=RotationPolicy(seconds=60, bytes=128 * 1024**2),
            durability_interval_seconds=0,
            max_frame_bytes=16 * 1024**2,
        )
        spool.enqueue(event(1))
        assert spool.drain_one()
        assert catalog.chunks_in_states(ChunkState.ACTIVE)

        def seal() -> None:
            spool.close_and_seal()
            calls.append("seal")

        def open_gap(at: int) -> None:
            calls.append("gap")
            gaps.append(at)

        coordinator = DiskEmergencyCoordinator(
            catalog=catalog,
            actions=EmergencyActions(
                suspend_non_core=lambda: calls.append("suspend"),
                prioritize_verified_archive=lambda: calls.append("archive"),
                seal_active=seal,
                stop_collectors=lambda: calls.append("stop"),
                open_gap=open_gap,
            ),
            rotation_bytes=128 * 1024**2,
        )
        result = coordinator.apply(
            total_bytes=100 * GIB,
            free_bytes=4 * GIB,
            observed_at_utc_ns=123,
        )

        assert calls == ["suspend", "archive", "seal", "stop", "gap"]
        assert gaps == [123]
        assert result["collector_stop"] is True
        assert not catalog.chunks_in_states(ChunkState.ACTIVE)
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 1
        events = catalog.operational_events(event_type="DISK_EMERGENCY_STOP")
        assert len(events) == 1
        assert events[0]["occurred_at_utc_ns"] == 123
        evidence = cast(dict[str, object], events[0]["evidence"])
        assert evidence["free_bytes"] == 4 * GIB

        repeated = coordinator.apply(
            total_bytes=100 * GIB,
            free_bytes=3 * GIB,
            observed_at_utc_ns=124,
        )
        assert cast(list[str], repeated["actions"])[-3:] == [
            "SEAL_ACTIVE",
            "STOP_COLLECTORS",
            "ALREADY_STOPPED",
        ]
        assert calls == [
            "suspend",
            "archive",
            "seal",
            "stop",
            "gap",
            "suspend",
            "archive",
            "seal",
            "stop",
        ]
        assert len(catalog.operational_events(event_type="DISK_EMERGENCY_STOP")) == 1


def test_emergency_above_hard_reserve_prioritizes_without_stopping(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        coordinator = DiskEmergencyCoordinator(
            catalog=catalog,
            actions=EmergencyActions(
                suspend_non_core=lambda: calls.append("suspend"),
                prioritize_verified_archive=lambda: calls.append("archive"),
                seal_active=lambda: calls.append("seal"),
                stop_collectors=lambda: calls.append("stop"),
                open_gap=lambda _at: calls.append("gap"),
            ),
            rotation_bytes=128 * 1024**2,
        )
        result = coordinator.apply(
            total_bytes=1_000 * GIB,
            free_bytes=30 * GIB,
            observed_at_utc_ns=1,
        )
        assert result["severity"] == "EMERGENCY"
        assert result["collector_stop"] is False
        assert calls == ["suspend", "archive"]
        assert not catalog.operational_events(event_type="DISK_EMERGENCY_STOP")


def test_vps_eta_emergency_does_not_stop_above_actual_hard_reserve(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        coordinator = DiskEmergencyCoordinator(
            catalog=catalog,
            actions=EmergencyActions(
                suspend_non_core=lambda: calls.append("suspend"),
                prioritize_verified_archive=lambda: calls.append("archive"),
                seal_active=lambda: calls.append("seal"),
                stop_collectors=lambda: calls.append("stop"),
                open_gap=lambda _at: calls.append("gap"),
            ),
            rotation_bytes=128 * 1024**2,
        )
        derived = evaluate_capacity(
            profile=VPS_PRODUCTION_V1,
            scope_id="internal",
            total_bytes=40 * GIB,
            free_bytes=15 * GIB,
            hard_reserve_eta={
                "status": "FORECAST",
                "utc_ns": 24 * 3_600_000_000_000,
            },
            now_utc_ns=0,
        )
        result = coordinator.apply(
            total_bytes=40 * GIB,
            free_bytes=15 * GIB,
            observed_at_utc_ns=123,
            capacity_decision=derived,
        )

        assert result["capacity_state"] == "EMERGENCY"
        assert calls == ["suspend", "archive"]
        assert result["collector_stop"] is False
        assert not catalog.operational_events(event_type="DISK_EMERGENCY_STOP")


def test_vps_actual_hard_reserve_repeats_safety_callbacks_without_duplicate_event(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    gaps: list[int] = []
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        coordinator = DiskEmergencyCoordinator(
            catalog=catalog,
            actions=EmergencyActions(
                suspend_non_core=lambda: calls.append("suspend"),
                prioritize_verified_archive=lambda: calls.append("archive"),
                seal_active=lambda: calls.append("seal"),
                stop_collectors=lambda: calls.append("stop"),
                open_gap=lambda at: gaps.append(at),
            ),
            rotation_bytes=128 * 1024**2,
        )
        for observed_at in (123, 124):
            derived = evaluate_capacity(
                profile=VPS_PRODUCTION_V1,
                scope_id="internal",
                total_bytes=40 * GIB,
                free_bytes=10 * GIB,
                hard_reserve_eta={"status": "NOT_APPROACHING"},
                now_utc_ns=observed_at,
            )
            result = coordinator.apply(
                total_bytes=40 * GIB,
                free_bytes=10 * GIB,
                observed_at_utc_ns=observed_at,
                capacity_decision=derived,
            )
            if observed_at == 123:
                assert cast(list[str], result["actions"])[-2:] == [
                    "DISK_EMERGENCY_STOP",
                    "OPEN_GAP",
                ]
            else:
                assert cast(list[str], result["actions"])[-1] == "ALREADY_STOPPED"

        assert calls == [
            "suspend",
            "archive",
            "seal",
            "stop",
            "suspend",
            "archive",
            "seal",
            "stop",
        ]
        assert gaps == [123]
        assert len(catalog.operational_events(event_type="DISK_EMERGENCY_STOP")) == 1
