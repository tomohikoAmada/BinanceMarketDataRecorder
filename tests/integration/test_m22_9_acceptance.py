from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal, cast

import pytest

from binance_market_data_recorder.archive import ArchiveError, ArchiveManager
from binance_market_data_recorder.audit import reconnect_boundaries as reconnect_audit
from binance_market_data_recorder.audit.reconnect_boundaries import (
    EXPLICIT_SEQUENCE_GAP,
    UNMARKED_RECONNECT,
    audit_data_root,
    incremental_audit_data_root,
    strict_manifest_inventory,
)
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.service.acceptance import (
    _sample_chain,
    resume_observer,
    sha256_bytes,
)
from binance_market_data_recorder.service.readiness import DeploymentReadinessResult
from binance_market_data_recorder.spool.seal import (
    SealError,
    seal_partial,
    validate_sealed_artifact,
)
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.archive_support import prepare_archive
from tests.unit.test_historical_reconnect_audit import (
    build_fixture,
    seal_chunk,
    usdm_envelope,
)
from tests.unit.test_m22_9_acceptance import _observer
from tools.audit_reconnect_boundaries import audit_data_root as historical_audit_data_root


def test_installed_strict_audit_reuses_real_raw_manifest_fixture(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    result = incremental_audit_data_root(tmp_path)
    payload = result
    assert result["schema_version"] == "m22.9-reconnect-audit.v2"
    assert payload["manifest_inventory"]["count"] == 4
    assert payload["summary"]["explicit_gap"] == 2
    assert payload["summary"]["unmarked_reconnect"] == 2
    assert payload["summary"]["unknown"] == 1
    assert EXPLICIT_SEQUENCE_GAP in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }
    assert UNMARKED_RECONNECT in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }


@pytest.mark.parametrize("body", [b"{", b"[]\n", b'{"manifest_schema_version":"future"}\n'])
def test_strict_inventory_never_skips_malformed_manifest(tmp_path: Path, body: bytes) -> None:
    manifests = tmp_path / "data" / "manifests"
    manifests.mkdir(parents=True)
    (manifests / "bad.manifest.json").write_bytes(body)
    with pytest.raises(SealError):
        strict_manifest_inventory(tmp_path)


def test_acceptance_source_contains_no_direct_catalog_sql(tmp_path: Path) -> None:
    source = Path("src/binance_market_data_recorder/service/acceptance.py").read_text()
    assert "sqlite3.connect" not in source
    assert "cursor.execute" not in source
    assert "SELECT " not in source


def _seal_stream(
    root: Path,
    *,
    market: Literal["spot", "um_perpetual"],
    stream: str,
    connections: list[str],
    receive_start: int,
) -> None:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=market,
            symbol="BTCUSDT",
            stream=stream,
            collector_instance_id="shared-engine-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        for ordinal, connection in enumerate(connections):
            writer.append(
                EventEnvelope(
                    market=market,
                    symbol="BTCUSDT",
                    stream=stream,
                    module="shared-engine-test",
                    connection_id=connection,
                    collector_instance_id="shared-engine-test",
                    collector_version="0.1.0+test",
                    receive_time_utc_ns=receive_start + ordinal,
                    receive_monotonic_ns=receive_start + ordinal,
                    raw_payload=b"{}",
                )
            )
        writer.close()
        seal_partial(writer.path, layout=layout, catalog=catalog)


def test_interleaved_market_stream_chunks_never_form_cross_stream_reconnects(
    tmp_path: Path,
) -> None:
    _seal_stream(
        tmp_path,
        market="spot",
        stream="diff_depth",
        connections=["spot-depth"],
        receive_start=100,
    )
    _seal_stream(
        tmp_path,
        market="spot",
        stream="agg_trade",
        connections=["spot-trade"],
        receive_start=101,
    )
    _seal_stream(
        tmp_path,
        market="um_perpetual",
        stream="book_ticker",
        connections=["um-book"],
        receive_start=102,
    )
    result = incremental_audit_data_root(tmp_path)
    assert result["summary"]["transitions_total"] == 0


def test_rest_per_request_connection_ids_are_not_reconnect_boundaries(tmp_path: Path) -> None:
    _seal_stream(
        tmp_path,
        market="spot",
        stream="depth_snapshot",
        connections=["request-a", "request-b", "request-c"],
        receive_start=200,
    )
    result = incremental_audit_data_root(tmp_path)
    assert result["summary"]["transitions_total"] == 0


def test_historical_tool_and_installed_acceptance_share_exact_engine(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    assert historical_audit_data_root is audit_data_root
    historical = historical_audit_data_root(tmp_path)
    assert historical == audit_data_root(tmp_path)
    acceptance = incremental_audit_data_root(tmp_path)

    def classifications(document: dict[str, object]) -> list[tuple[object, ...]]:
        streams = document["streams"]
        assert isinstance(streams, list)
        return sorted(
            (
                stream["market"],
                stream["stream"],
                transition["boundary_kind"],
                transition["old_chunk_id"],
                transition["new_chunk_id"],
                transition["kind"],
            )
            for stream in streams
            if isinstance(stream, dict)
            for transition in stream["transitions"]
            if isinstance(transition, dict)
        )

    assert classifications(historical) == classifications(acceptance)


def test_catalog_row_committed_after_inventory_boundary_is_deferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row committed after the filesystem inventory belongs to the next sample."""

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog):
        pass
    original_inventory = reconnect_audit.strict_manifest_inventory
    inserted = False

    def inventory_then_commit(
        data_root: Path,
        *,
        market: str | None = None,
        stream: str | None = None,
        deep_scan: bool = True,
    ) -> tuple[list[reconnect_audit.ChunkScan], dict[str, Any]]:
        nonlocal inserted
        chunks, inventory = original_inventory(
            data_root, market=market, stream=stream, deep_scan=deep_scan
        )
        if not inserted:
            with Catalog(layout.catalog) as catalog:
                seal_chunk(layout, catalog, [usdm_envelope("boundary-commit", 1)])
            inserted = True
        return chunks, inventory

    monkeypatch.setattr(
        reconnect_audit, "strict_manifest_inventory", inventory_then_commit
    )
    result = incremental_audit_data_root(tmp_path)

    assert result["manifest_inventory"]["count"] == 0
    assert result["catalog_findings"] == []


def test_manifest_published_with_post_boundary_catalog_row_is_deferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog):
        pass
    original_inventory = reconnect_audit.strict_manifest_inventory
    inserted = False

    def commit_then_inventory(
        data_root: Path,
        *,
        market: str | None = None,
        stream: str | None = None,
        deep_scan: bool = True,
    ) -> tuple[list[reconnect_audit.ChunkScan], dict[str, Any]]:
        nonlocal inserted
        if not inserted:
            with Catalog(layout.catalog) as catalog:
                seal_chunk(layout, catalog, [usdm_envelope("deferred", 1)])
            inserted = True
        return original_inventory(
            data_root, market=market, stream=stream, deep_scan=deep_scan
        )

    monkeypatch.setattr(
        reconnect_audit, "strict_manifest_inventory", commit_then_inventory
    )
    result = incremental_audit_data_root(tmp_path)

    continuation = result["continuation"]
    assert isinstance(continuation, dict)
    assert continuation["manifest_members"] == {}
    assert result["summary"]["chunks_scanned"] == 0
    assert result["catalog_findings"] == []


def test_manifest_published_from_pre_boundary_active_row_is_deferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="boundary-seal-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        writer.append(usdm_envelope("deferred-active", 1))
        writer.close()
    original_inventory = reconnect_audit.strict_manifest_inventory
    sealed = False

    def seal_then_inventory(
        data_root: Path,
        *,
        market: str | None = None,
        stream: str | None = None,
        deep_scan: bool = True,
    ) -> tuple[list[reconnect_audit.ChunkScan], dict[str, Any]]:
        nonlocal sealed
        if not sealed:
            with Catalog(layout.catalog) as catalog:
                seal_partial(writer.path, layout=layout, catalog=catalog)
            sealed = True
        return original_inventory(
            data_root, market=market, stream=stream, deep_scan=deep_scan
        )

    monkeypatch.setattr(reconnect_audit, "strict_manifest_inventory", seal_then_inventory)
    first = incremental_audit_data_root(tmp_path)

    assert first["continuation"]["manifest_members"] == {}
    assert first["catalog_findings"] == []
    second = incremental_audit_data_root(tmp_path, continuation=first["continuation"])
    assert second["summary"]["chunks_scanned"] == 1
    assert len(second["continuation"]["manifest_members"]) == 1


def test_pending_local_delete_is_authorized_while_archive_manager_is_held(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    observations: list[dict[str, object]] = []
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )

        def observe_after_unlink(point: str, _path: Path | None) -> None:
            if point != "after_local_unlink":
                return
            transaction = catalog.archive_transactions()[0]
            assert transaction["state"] == "LOCAL_DELETE_PENDING"
            observed = incremental_audit_data_root(
                prepared.layout.root,
                archive_root_resolver=lambda: {
                    prepared.target.storage_id: prepared.target.root
                },
            )
            observations.append(observed)
            assert observed["catalog_findings"] == []
            assert observed["raw_loss"] == [
                {
                    "chunk_id": prepared.chunk_ids[0],
                    "manifest_path": observed["raw_loss"][0]["manifest_path"],
                    "classification": "AUTHORIZED_LOCAL_DELETE",
                    "has_sequence_gap_marker": "false",
                }
            ]

        manager.fault_hook = observe_after_unlink
        assert manager.run_once().state == "LOCAL_DELETED"

    assert len(observations) == 1


def test_local_raw_disappearing_during_validation_reopens_pending_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = prepare_archive(tmp_path)
    original_validate = validate_sealed_artifact
    unlinked = False
    observations: list[dict[str, object]] = []

    def unlink_during_validation(
        sealed: Path, manifest: dict[str, object]
    ) -> None:
        nonlocal unlinked
        if not unlinked:
            sealed.unlink()
            unlinked = True
        original_validate(sealed, manifest)

    monkeypatch.setattr(
        "binance_market_data_recorder.audit.reconnect_boundaries.validate_sealed_artifact",
        unlink_during_validation,
    )
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )

        def observe_before_local_delete(point: str, _path: Path | None) -> None:
            if point != "before_local_delete":
                return
            transaction = catalog.archive_transactions()[0]
            assert transaction["state"] == "LOCAL_DELETE_PENDING"
            observations.append(
                incremental_audit_data_root(
                    prepared.layout.root,
                    archive_root_resolver=lambda: {
                        prepared.target.storage_id: prepared.target.root
                    },
                )
            )

        manager.fault_hook = observe_before_local_delete
        assert manager.run_once().state == "LOCAL_DELETED"

    assert unlinked
    loss = observations[0]["raw_loss"]
    assert isinstance(loss, list) and isinstance(loss[0], dict)
    assert loss[0]["classification"] == "AUTHORIZED_LOCAL_DELETE"


def test_pre_authorization_local_absence_fails_closed(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    observations: list[dict[str, object]] = []
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )

        def unlink_before_authorization(point: str, _path: Path | None) -> None:
            if point != "after_reserve":
                return
            transaction = catalog.archive_transactions()[0]
            source = prepared.layout.root / str(transaction["source_relative_path"])
            source.unlink()
            observations.append(
                incremental_audit_data_root(
                    prepared.layout.root,
                    archive_root_resolver=lambda: {
                        prepared.target.storage_id: prepared.target.root
                    },
                )
            )

        manager.fault_hook = unlink_before_authorization
        with pytest.raises(ArchiveError):
            manager.run_once()

    loss = observations[0]["raw_loss"]
    assert isinstance(loss, list) and isinstance(loss[0], dict)
    assert loss[0]["classification"] == "UNKNOWN"


def test_tampered_external_artifact_fails_closed_during_pending_local_delete(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )

        def corrupt_after_unlink(point: str, _path: Path | None) -> None:
            if point != "after_local_unlink":
                return
            transaction = catalog.archive_transactions()[0]
            external = prepared.target.root / str(transaction["target_relative_path"])
            original = external.read_bytes()
            external.write_bytes(b"tampered")
            try:
                observed = incremental_audit_data_root(
                    prepared.layout.root,
                    archive_root_resolver=lambda: {
                        prepared.target.storage_id: prepared.target.root
                    },
                )
            finally:
                external.write_bytes(original)
            assert observed["raw_loss"][0]["classification"] == "UNKNOWN"

        manager.fault_hook = corrupt_after_unlink
        assert manager.run_once().state == "LOCAL_DELETED"


def test_incremental_audit_is_read_only_for_catalog_and_recorder_tree(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    before_catalog = {
        path.name: path.read_bytes()
        for path in prepared.layout.state.iterdir()
        if path.name.startswith("catalog.sqlite")
    }
    before_tree = {
        path.relative_to(prepared.layout.root): path.read_bytes()
        for path in prepared.layout.root.rglob("*")
        if path.is_file()
    }
    incremental_audit_data_root(
        prepared.layout.root,
        archive_root_resolver=lambda: {
            prepared.target.storage_id: prepared.target.root
        },
    )
    after_catalog = {
        path.name: path.read_bytes()
        for path in prepared.layout.state.iterdir()
        if path.name.startswith("catalog.sqlite")
    }
    after_tree = {
        path.relative_to(prepared.layout.root): path.read_bytes()
        for path in prepared.layout.root.rglob("*")
        if path.is_file()
    }
    assert after_catalog == before_catalog
    assert after_tree == before_tree


def test_catalog_acceptance_v2_rolling_lifecycle_matches_producer(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    observer.start()
    clock.utc += 1
    clock.boot += 1
    observer.sample()

    gap_started = clock.utc + 1
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="integration-gap-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=gap_started,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "integration-gap",
                "gap_started_at_utc_ns": gap_started,
                "original_connection_id": "connection-a",
                "original_generation": 1,
            },
            symbol="BTCUSDT",
        )
    clock.utc += 1
    clock.boot += 1
    _open_path, _open_sha, open_sample = observer.sample()

    gap_ended = clock.utc + 1
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="integration-gap-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=gap_ended,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "integration-gap",
                "gap_ended_at_utc_ns": gap_ended,
                "new_connection_id": "connection-b",
                "new_generation": 2,
            },
            symbol="BTCUSDT",
        )
    clock.utc += 1
    clock.boot += 1
    _closed_path, _closed_sha, closed_sample = observer.sample()

    open_transition = open_sample["catalog_transition"]
    assert isinstance(open_transition, dict)
    assert len(open_transition["started"]) == 1
    assert len(open_transition["current_open"]) == 1
    closed_transition = closed_sample["catalog_transition"]
    assert isinstance(closed_transition, dict)
    assert len(closed_transition["completed"]) == 1
    assert closed_transition["current_open"] == []

    state = _sample_chain(
        observer.evidence_root,
        start=json.loads(
            (observer.evidence_root / "stage-start.json").read_text(encoding="utf-8")
        ),
        start_sha=sha256_bytes((observer.evidence_root / "stage-start.json").read_bytes()),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.catalog_open == {}
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "integration-gap")
    }


@pytest.mark.parametrize(
    "ended_at_utc_ns",
    [150, 200],
    ids=["inversion", "equal-timestamp"],
)
def test_catalog_non_monotonic_producer_evidence_binds_acceptance_blocker(
    tmp_path: Path, ended_at_utc_ns: int
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    clock.utc = 100
    observer.start()
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="non-monotonic-gap-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=200,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "non-monotonic-gap",
                "gap_started_at_utc_ns": 200,
                "original_connection_id": "connection-a",
                "original_generation": 1,
            },
            symbol="BTCUSDT",
        )
        catalog.record_operational_event(
            event_id="non-monotonic-gap-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=ended_at_utc_ns,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "non-monotonic-gap",
                "gap_ended_at_utc_ns": ended_at_utc_ns,
                "new_connection_id": "connection-b",
                "new_generation": 2,
            },
            symbol="BTCUSDT",
        )

    clock.utc = 250
    clock.boot += 1
    _sample_path, _sample_sha, sample = observer.sample()
    transition = cast(dict[str, object], sample["catalog_transition"])
    completed = cast(list[dict[str, object]], transition["completed"])
    assert len(completed) == 1
    assert completed[0]["market"] == "um_perpetual"
    assert completed[0]["symbol"] == "BTCUSDT"
    assert completed[0]["stream"] == "book_ticker"
    assert completed[0]["gap_id"] == "non-monotonic-gap"
    assert completed[0]["started_at_utc_ns"] == 200
    assert completed[0]["ended_at_utc_ns"] == ended_at_utc_ns
    findings = cast(list[object], sample["blocking_findings"])
    assert "unsafe_wall_clock_backward" in findings
    assert sample["result"] == "INCOMPLETE"
    details = cast(dict[str, object], sample["new_finding_details"])
    assert details["unsafe_wall_clock_backward"] == {
        "market": "um_perpetual",
        "symbol": "BTCUSDT",
        "stream": "book_ticker",
        "gap_id": "non-monotonic-gap",
        "started_at_utc_ns": 200,
        "ended_at_utc_ns": ended_at_utc_ns,
    }

    state = _sample_chain(
        observer.evidence_root,
        start=json.loads(
            (observer.evidence_root / "stage-start.json").read_text(encoding="utf-8")
        ),
        start_sha=sha256_bytes(
            (observer.evidence_root / "stage-start.json").read_bytes()
        ),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.catalog_open == {}
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "non-monotonic-gap")
    }


def test_non_monotonic_stage_start_resumes_and_preserves_finding_authority(
    tmp_path: Path,
) -> None:
    observer, clock, manager = _observer(tmp_path)
    clock.utc = 100
    with Catalog(observer.data_root / "state" / "catalog.sqlite") as catalog:
        catalog.record_operational_event(
            event_id="stage-start-non-monotonic-gap-start",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=100,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "stage-start-non-monotonic-gap",
                "gap_started_at_utc_ns": 100,
                "original_connection_id": "connection-a",
                "original_generation": 1,
            },
            symbol="BTCUSDT",
        )
        catalog.record_operational_event(
            event_id="stage-start-non-monotonic-gap-complete",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=100,
            evidence={
                "market": "um_perpetual",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "gap_id": "stage-start-non-monotonic-gap",
                "gap_ended_at_utc_ns": 100,
                "new_connection_id": "connection-b",
                "new_generation": 2,
            },
            symbol="BTCUSDT",
        )

    _start_path, _start_sha, start = observer.start()
    assert start["blocking_findings"] == ["unsafe_wall_clock_backward"]
    assert start["result"] == "INCOMPLETE"
    assert start["new_finding_details"] == {
        "unsafe_wall_clock_backward": {
            "market": "um_perpetual",
            "symbol": "BTCUSDT",
            "stream": "book_ticker",
            "gap_id": "stage-start-non-monotonic-gap",
            "started_at_utc_ns": 100,
            "ended_at_utc_ns": 100,
        }
    }
    original_t0_utc = observer.t0_utc_ns
    original_t0_boottime = observer.t0_boottime_ns

    resumed = resume_observer(
        observer.evidence_root,
        data_root=observer.data_root,
        identity=observer.identity,
        manager=manager,  # type: ignore[arg-type]
        evaluator=observer.evaluator,
        clock=clock,
        disk_usage=observer.disk_usage,
    )
    resumed.identity_verifier = observer.identity_verifier
    assert resumed.t0_utc_ns == original_t0_utc == 100
    assert resumed.t0_boottime_ns == original_t0_boottime
    assert resumed.t0_boot_id == start["boot_id"]
    assert resumed.frozen_process == start["systemd_process_incarnation"]
    assert resumed.frozen_service_instance_id == start["service_instance_id"]
    assert resumed.ever_blocking_findings == {"unsafe_wall_clock_backward"}
    assert resumed.next_sample_ordinal == 0
    assert resumed.last_sample_sha256 is None

    clock.utc = 101
    clock.boot += 1
    _sample_path, _sample_sha, sample = resumed.sample()
    assert sample["blocking_findings"] == ["unsafe_wall_clock_backward"]
    assert sample["new_finding_details"] == {}
    assert sample["result"] == "INCOMPLETE"


def test_acceptance_catalog_evidence_uses_frozen_utc_boundary(
    tmp_path: Path,
) -> None:
    observer, clock, _manager = _observer(tmp_path)
    _start_path, _start_sha, _start = observer.start()
    assert observer.t0_utc_ns is not None
    observation_boundary = observer.t0_utc_ns + 10

    class WriteLifecycleBeforeCatalogRead:
        def __init__(self, catalog_path: Path, boundary: int) -> None:
            self.catalog_path = catalog_path
            self.boundary = boundary
            self.written = False

        def evaluate(self) -> DeploymentReadinessResult:
            if not self.written:
                self.written = True
                with Catalog(self.catalog_path) as catalog:
                    catalog.record_operational_event(
                        event_id="boundary-race-start",
                        event_type="STREAM_DISCONTINUITY_STARTED",
                        occurred_at_utc_ns=self.boundary + 1,
                        evidence={
                            "market": "um_perpetual",
                            "symbol": "BTCUSDT",
                            "stream": "book_ticker",
                            "gap_id": "boundary-race",
                            "gap_started_at_utc_ns": self.boundary + 1,
                            "original_connection_id": "connection-a",
                            "original_generation": 1,
                        },
                        symbol="BTCUSDT",
                    )
                    catalog.record_operational_event(
                        event_id="boundary-race-complete",
                        event_type="STREAM_DISCONTINUITY_COMPLETED",
                        occurred_at_utc_ns=self.boundary + 2,
                        evidence={
                            "market": "um_perpetual",
                            "symbol": "BTCUSDT",
                            "stream": "book_ticker",
                            "gap_id": "boundary-race",
                            "gap_ended_at_utc_ns": self.boundary + 2,
                            "new_connection_id": "connection-b",
                            "new_generation": 2,
                        },
                        symbol="BTCUSDT",
                    )
            return DeploymentReadinessResult("READY", (), {"authoritative": True})

    observer.evaluator = WriteLifecycleBeforeCatalogRead(
        observer.data_root / "state" / "catalog.sqlite", observation_boundary
    )  # type: ignore[assignment]
    clock.utc = observation_boundary
    clock.boot += 1
    _open_path, _open_sha, first = observer.sample()
    assert first["observed_at_utc_ns"] == observation_boundary
    first_transition = cast(dict[str, object], first["catalog_transition"])
    assert first_transition["started"] == []
    assert first_transition["completed"] == []
    assert first_transition["current_open"] == []

    clock.utc = observation_boundary + 3
    clock.boot += 1
    _closed_path, _closed_sha, second = observer.sample()
    second_transition = cast(dict[str, object], second["catalog_transition"])
    completed = cast(list[dict[str, object]], second_transition["completed"])
    current_open = cast(list[dict[str, object]], second_transition["current_open"])
    assert len(completed) == 1
    assert completed[0]["gap_id"] == "boundary-race"
    assert current_open == []
    assert {
        item["gap_id"] for item in completed
    }.isdisjoint({item["gap_id"] for item in current_open})
    ended_at = cast(int, completed[0]["ended_at_utc_ns"])
    observed_at = cast(int, second["observed_at_utc_ns"])
    assert ended_at <= observed_at

    state = _sample_chain(
        observer.evidence_root,
        start=json.loads(
            (observer.evidence_root / "stage-start.json").read_text(encoding="utf-8")
        ),
        start_sha=sha256_bytes(
            (observer.evidence_root / "stage-start.json").read_bytes()
        ),
        identity=observer.identity,
        require_eligible=False,
    )
    assert state.catalog_open == {}
    assert state.catalog_current_interval_keys == {
        ("um_perpetual", "BTCUSDT", "book_ticker", "boundary-race")
    }
