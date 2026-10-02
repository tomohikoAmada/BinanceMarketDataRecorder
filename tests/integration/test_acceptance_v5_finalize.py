from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_finalize import (
    baseline,
    finalize,
    verify_audit,
    verify_completed_v5_stage,
)
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_unit
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_acceptance_v5_catalog import insert_event
from tests.unit.test_acceptance_v5_io import fixture, stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope


def raw_unit(task: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
    return qualify_unit(task)


def test_real_baseline_to_online_target_to_terminal(tmp_path: Path) -> None:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    # Replace only the online fixture's synthetic predecessor with a full audit.
    root = tmp_path / "full-baseline"
    baseline_path, _sha, audit = baseline(
        data_root=observer.data_root,
        evidence_root=root,
        identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    assert audit["result"] == "PASS_CANDIDATE"
    assert audit["formal_duration_credit_ns"] == 0
    assert (
        verify_audit(root=root, identity=observer.identity, archive_roots={}, raw_unit=raw_unit)[0]
        == audit
    )
    observer.predecessor_path = baseline_path
    observer.start()
    for _ in range(23):
        advance(clock, 300)
        observer.sample()
    advance(clock, 300)
    observer.finalize()
    _path, final_sha, final = finalize(
        evidence_root=observer.evidence_root,
        identity=observer.identity,
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    assert final["result"] == "PASS_CANDIDATE"
    assert final["eligible_for_next_stage"] is True
    assert verify_completed_v5_stage(
        observer.evidence_root,
        observer.identity,
        archive_roots={},
        raw_unit=raw_unit,
    ) == (final, final_sha)


def test_baseline_raw_integrity_and_adversarial_root(tmp_path: Path) -> None:
    observer, _clock, _evaluator = observer_fixture(tmp_path)
    fixture(observer.data_root)
    root = tmp_path / "full-baseline"
    _path, _sha, audit = baseline(
        data_root=observer.data_root,
        evidence_root=root,
        identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    assert audit["counts_by_family"]["manifest"] == 1
    assert audit["counts_by_family"]["chunk_transition"] == 3
    assert audit["raw_bytes_verified"] > 0
    verify_audit(root=root, identity=observer.identity, archive_roots={}, raw_unit=raw_unit)
    shard = root / "shards" / "shard-00000000.json"
    body = shard.read_bytes()
    shard.write_bytes(body.replace(b'"record_count":1', b'"record_count":2'))
    with pytest.raises(AcceptanceError):
        verify_audit(root=root, identity=observer.identity, archive_roots={}, raw_unit=raw_unit)


def test_multiple_shards_and_interrupted_baseline_resume(tmp_path: Path) -> None:
    observer, _clock, _evaluator = observer_fixture(tmp_path)
    with Catalog(ensure_storage_layout(observer.data_root).catalog) as catalog:
        for ordinal in range(600):
            insert_event(catalog._connection, f"event-{ordinal}", ordinal)
    kwargs: dict[str, Any] = dict(
        data_root=observer.data_root,
        evidence_root=tmp_path / "many-shards",
        identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )

    def interrupt(ordinal: int) -> None:
        if ordinal == 1:
            raise RuntimeError("first shard interrupt")

    with pytest.raises(RuntimeError, match="first shard interrupt"):
        baseline(**kwargs, checkpoint=interrupt)
    _path, _sha, audit = baseline(**kwargs, resume=True)
    assert audit["shards"]["shard_count"] == 2
    assert audit["counts_by_family"]["operational_event"] == 600
    assert audit["result"] == "PASS_CANDIDATE"


@pytest.mark.parametrize("mode", ["open", "completed", "malformed"])
def test_full_baseline_reconstructs_real_discontinuity_pair(
    tmp_path: Path, mode: str
) -> None:
    observer, _clock, _evaluator = observer_fixture(tmp_path)
    layout = ensure_storage_layout(observer.data_root)
    gap = dict(market="um_perpetual", symbol="BTCUSDT", stream="book_ticker", gap_id="gap-a")
    completed = mode != "open"
    start = {**gap, "original_connection_id": "old",
             "original_generation": "invalid" if mode == "malformed" else 0,
             "gap_started_at_utc_ns": 1_000_000_001}
    with Catalog(layout.catalog) as catalog:
        seal_chunk(layout, catalog, [usdm_envelope("old", 1)])
        catalog.record_operational_event(
            event_id="gap-start", event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1_000_000_001, evidence=start, symbol="BTCUSDT",
        )
        if completed:
            catalog.record_operational_event(
                event_id="gap-complete", event_type="STREAM_DISCONTINUITY_COMPLETED",
                occurred_at_utc_ns=1_000_000_002, symbol="BTCUSDT",
                evidence={**gap, "new_connection_id": "new", "new_generation": 1,
                          "gap_ended_at_utc_ns": 1_000_000_002},
            )
            seal_chunk(layout, catalog, [usdm_envelope("new", 2, ("sequence_gap",))])
    root = tmp_path / "discontinuity-baseline"
    _path, _sha, audit = baseline(
        data_root=observer.data_root, evidence_root=root, identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]], archive_roots={}, probe=stopped,
        identity_verifier=lambda _identity: None, raw_unit=raw_unit, boot_id="boot-a",
    )
    if mode == "malformed":
        assert audit["result"] == "FAIL"
        assert "operational_discontinuity_authority_malformed" in audit["blocking_findings"]
    else:
        assert audit["result"] == "PASS_CANDIDATE"
        assert audit["blocking_findings"] == []
    assert audit["counts_by_family"]["operational_event"] == 1 + int(completed)
    assert audit["counts_by_family"]["manifest"] == 1 + int(completed)
    assert audit["formal_duration_credit_ns"] == 0
    pending = audit["continuation_seed"]["open_discontinuities"]
    assert set(pending) == (set() if completed else {"um_perpetual:BTCUSDT:book_ticker"})
    if not completed:
        assert pending["um_perpetual:BTCUSDT:book_ticker"]["event_id"] == "gap-start"
    assert verify_audit(root=root, identity=observer.identity, archive_roots={},
                        raw_unit=raw_unit)[0] == audit
    assert verify_audit(root=root, identity=observer.identity, archive_roots={},
                        historical_control_only=True)[0] == audit
