from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError, canonical_json
from binance_market_data_recorder.service.acceptance_v5_finalize import (
    baseline,
    finalize,
    verify_audit,
    verify_completed_v5_stage,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_acceptance_v5_finalize import raw_unit
from tests.unit.test_acceptance_v5_io import fixture, stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture


def prepared(tmp_path: Path) -> tuple[Any, Any, Path, Path]:
    observer, clock, _evaluator = observer_fixture(tmp_path)
    manifest, _relative = fixture(observer.data_root)
    path, _sha, audit = baseline(
        data_root=observer.data_root,
        evidence_root=tmp_path / "baseline-full",
        identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    assert audit["result"] == "PASS_CANDIDATE"
    observer.predecessor_path = path
    observer.start()
    for _ in range(23):
        advance(clock, 300)
        observer.sample()
    advance(clock, 300)
    observer.finalize()
    return observer, clock, manifest, path.parent


@pytest.mark.parametrize(
    "fault", ["manifest-byte", "manifest-delete", "manifest-path", "catalog", "raw-loss"]
)
def test_terminal_rejects_frozen_historical_corruption(tmp_path: Path, fault: str) -> None:
    observer, _clock, manifest, _baseline = prepared(tmp_path)
    body = json.loads(manifest.read_bytes())
    if fault == "manifest-byte":
        # Valid JSON/semantics, different immutable exact bytes.
        manifest.write_bytes(manifest.read_bytes() + b" ")
    elif fault == "manifest-delete":
        manifest.unlink()
    elif fault == "manifest-path":
        manifest.rename(manifest.with_name("replacement.manifest.json"))
    elif fault == "catalog":
        with Catalog(ensure_storage_layout(observer.data_root).catalog) as catalog:
            catalog._connection.execute("UPDATE chunks SET record_count=record_count+1")
    else:
        (observer.data_root / body["relative_path"]).unlink()
    _path, _sha, final = finalize(
        evidence_root=observer.evidence_root,
        identity=observer.identity,
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    assert final["result"] == "FAIL"
    assert final["eligible_for_next_stage"] is False
    with pytest.raises(AcceptanceError, match="not eligible"):
        verify_completed_v5_stage(
            observer.evidence_root, observer.identity, archive_roots={}, raw_unit=raw_unit
        )


def test_audit_interruption_resume_reuses_exact_prefix(tmp_path: Path) -> None:
    observer, _clock, _manifest, _baseline = prepared(tmp_path)

    def interrupt(_ordinal: int) -> None:
        raise RuntimeError("injected interruption after durable shard")

    kwargs = dict(
        evidence_root=observer.evidence_root,
        identity=observer.identity,
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    with pytest.raises(RuntimeError, match="injected interruption"):
        finalize(**kwargs, checkpoint=interrupt)
    root = observer.evidence_root / "terminal-audit"
    prefix = (root / "shards" / "shard-00000000.json").read_bytes()
    assert not (observer.evidence_root / "stage-final.json").exists()
    assert list(root.glob("audit-interruption-*.json"))
    _path, digest, final = finalize(**kwargs, resume=True)
    assert final["eligible_for_next_stage"] is True
    assert (root / "shards" / "shard-00000000.json").read_bytes() == prefix
    assert finalize(**kwargs, resume=True)[1] == digest


@pytest.mark.parametrize(
    "fault",
    [
        "root-digest",
        "summary",
        "private-snapshot",
        "catalog-backup",
        "missing-shard",
        "duplicate-shard",
        "shard-byte",
        "online-chain",
        "stage-target",
        "quiescence-corpus",
        "raw-proof",
        "stage-final",
    ],
)
def test_independent_verifier_rejects_rewritten_authority(tmp_path: Path, fault: str) -> None:
    observer, _clock, _manifest, _baseline = prepared(tmp_path)
    finalize(
        evidence_root=observer.evidence_root,
        identity=observer.identity,
        archive_roots={},
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    root = observer.evidence_root / "terminal-audit"
    shard = root / "shards" / "shard-00000000.json"
    if fault in {"root-digest", "summary"}:
        path = root / "audit-root.json"
        document = json.loads(path.read_bytes())
        if fault == "root-digest":
            document["shards"]["root_digest"] = "f" * 64
        else:
            document["raw_bytes_verified"] += 1
        path.write_bytes(canonical_json(document))
    elif fault == "private-snapshot":
        next((root / "manifest-objects").glob("*.manifest")).write_bytes(b"corrupt")
    elif fault == "catalog-backup":
        (root / "catalog.sqlite").write_bytes(b"corrupt")
    elif fault == "missing-shard":
        shard.unlink()
    elif fault == "duplicate-shard":
        shard.with_name("shard-duplicate.json").write_bytes(shard.read_bytes())
    elif fault in {"online-chain", "stage-target", "quiescence-corpus", "stage-final"}:
        path = {
            "online-chain": observer.evidence_root / "sample-00000000.json",
            "stage-target": observer.evidence_root / "stage-target.json",
            "quiescence-corpus": root / "quiescence-corpus.json",
            "stage-final": observer.evidence_root / "stage-final.json",
        }[fault]
        document = json.loads(path.read_bytes())
        document["run_id"] = "tampered-run"
        path.write_bytes(canonical_json(document))
    elif fault == "raw-proof":
        document = json.loads(shard.read_bytes())
        manifest = next(record for record in document["records"] if record["family"] == "manifest")
        manifest["authority"]["proof"]["local"]["stored_bytes"] += 1
        shard.write_bytes(canonical_json(document))
    else:
        shard.write_bytes(shard.read_bytes().replace(b'"record_count":1', b'"record_count":2'))
    with pytest.raises(AcceptanceError):
        verify_completed_v5_stage(
            observer.evidence_root, observer.identity, archive_roots={}, raw_unit=raw_unit
        )


def test_baseline_completed_resume_is_immutable(tmp_path: Path) -> None:
    observer, _clock, _manifest, root = prepared(tmp_path)
    original = (root / "audit-root.json").read_bytes()
    baseline(
        data_root=observer.data_root,
        evidence_root=root,
        identity=observer.identity,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        resume=True,
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    assert (root / "audit-root.json").read_bytes() == original
    verify_audit(root=root, identity=observer.identity, archive_roots={}, raw_unit=raw_unit)


def test_finalization_rereads_live_raw_before_publishing_root(tmp_path: Path) -> None:
    observer, _clock, manifest, _baseline = prepared(tmp_path)
    local_raw = observer.data_root / json.loads(manifest.read_bytes())["relative_path"]
    reads = 0

    def mutate_after_producer(task: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        nonlocal reads
        reads += 1
        if reads == 2:
            # The producer already completed its exact read. Its pinned proof
            # must not replace the independent pre-publication live reread.
            assert not (observer.evidence_root / "terminal-audit" / "audit-root.json").exists()
            local_raw.unlink()
        return raw_unit(task, **kwargs)

    with pytest.raises(AcceptanceError, match="independent exact corpus replay"):
        finalize(
            evidence_root=observer.evidence_root,
            identity=observer.identity,
            archive_roots={},
            probe=stopped,
            identity_verifier=lambda _identity: None,
            raw_unit=mutate_after_producer,
        )
    assert reads == 2
    assert not (observer.evidence_root / "terminal-audit" / "audit-root.json").exists()
    assert not (observer.evidence_root / "stage-final.json").exists()
