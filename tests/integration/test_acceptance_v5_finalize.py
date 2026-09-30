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
