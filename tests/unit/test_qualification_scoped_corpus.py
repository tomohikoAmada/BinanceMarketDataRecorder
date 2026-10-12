from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit
from binance_market_data_recorder.service.acceptance_v5_io import (
    V5_SCHEMA_VERSION,
    publish,
    snapshot_manifest,
)
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_unit
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope
from tools.qualification_scoped_corpus import capture_boundary, freeze_selected_corpus


def fixture(tmp_path: Path) -> tuple[Any, dict[str, Any], dict[str, Any], dict[str, Any]]:
    layout = ensure_storage_layout(tmp_path / "data")
    with Catalog(layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        old = seal_chunk(layout, catalog, [usdm_envelope("old", 1)])
        catalog._connection.execute("UPDATE chunks SET state='LOCAL_DELETED'")
        catalog._connection.commit()
        boundary = capture_boundary(
            layout.root, inventory_root=tmp_path / "start", utc_ns=10, boot_id="boot-a"
        )
        new = seal_chunk(layout, catalog, [usdm_envelope("new", 2), usdm_envelope("new", 3)])
    return layout, old, new, boundary


def audit(
    tmp_path: Path, layout: Any, boundary: dict[str, Any]
) -> tuple[Any, dict[str, Any], list[str]]:
    identity = _identity(tmp_path)
    evidence = tmp_path / "evidence"
    _path, anchor = publish(
        evidence,
        "baseline-preflight.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "run_id": "new-capture",
            "configured_products": [["um_perpetual", "BTCUSDT"]],
            "deployment_identity": identity.document(),
            "owner_selected_scope": boundary,
        },
    )
    _path, digest, corpus = freeze_selected_corpus(
        data_root=layout.root,
        root=evidence,
        identity=identity,
        boundary=boundary,
        anchor_sha256=anchor,
        boot_id="boot-a",
        probe=stopped,
    )
    opened: list[str] = []

    def raw_unit(task: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        opened.append(task["manifest"]["chunk_id"])
        return qualify_unit(task)

    _path, _digest, result = run_audit(
        root=evidence,
        corpus=corpus,
        corpus_sha=digest,
        identity=identity,
        anchor_sha=anchor,
        predecessor=None,
        stage_root=None,
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
        resume=False,
        probe=stopped,
        raw_unit=raw_unit,
    )
    verify_audit(
        root=evidence,
        identity=identity,
        archive_roots={},
        raw_unit=raw_unit,
        historical_control_only=True,
    )
    return corpus, result, opened


def test_only_new_saved_frames_are_fully_audited(tmp_path: Path) -> None:
    layout, old, new, boundary = fixture(tmp_path)
    with sqlite3.connect(layout.catalog) as db:
        old_path, old_manifest = db.execute(
            "SELECT sealed_path,manifest_path FROM chunks WHERE chunk_id=?", (old["chunk_id"],)
        ).fetchone()
        original_ids = db.execute(
            "SELECT transition_id FROM chunk_transitions WHERE chunk_id=?", (new["chunk_id"],)
        ).fetchall()
    (layout.root / old_path).write_bytes(b"intentionally unusable old Raw")
    (layout.root / old_manifest).write_bytes(b"intentionally unusable old manifest")
    corpus, result, opened = audit(tmp_path, layout, boundary)
    assert result["result"] == "PASS_CANDIDATE"
    assert (
        opened == [new["chunk_id"]] * 2
    )  # producer + independent LIVE; completed verify replays controls
    assert corpus["owner_selected_scope"]["selected_counts"]["frames"] == 2
    with sqlite3.connect(tmp_path / "evidence/catalog.sqlite") as db:
        assert db.execute("SELECT chunk_id FROM chunks").fetchall() == [(new["chunk_id"],)]
        assert (
            db.execute(
                "SELECT transition_id FROM chunk_transitions ORDER BY transition_id"
            ).fetchall()
            == original_ids
        )
    with sqlite3.connect(layout.catalog) as db:
        assert db.execute("SELECT count(*) FROM chunks").fetchone()[0] == 2


@pytest.mark.parametrize(
    "anomaly", ["missing-create", "missing-prior", "missing-manifest", "corrupt-new"]
)
def test_incomplete_or_corrupt_new_scope_cannot_pass(tmp_path: Path, anomaly: str) -> None:
    layout, old, new, boundary = fixture(tmp_path)
    with sqlite3.connect(layout.catalog) as db:
        if anomaly == "missing-create":
            db.execute(
                "DELETE FROM chunk_transitions WHERE chunk_id=? AND from_state IS NULL",
                (new["chunk_id"],),
            )
        elif anomaly == "missing-prior":
            db.execute("DELETE FROM chunk_transitions WHERE chunk_id=?", (old["chunk_id"],))
            db.execute("DELETE FROM chunks WHERE chunk_id=?", (old["chunk_id"],))
        elif anomaly == "missing-manifest":
            db.execute("UPDATE chunks SET manifest_path=NULL WHERE chunk_id=?", (new["chunk_id"],))
        else:
            raw = db.execute(
                "SELECT sealed_path FROM chunks WHERE chunk_id=?", (new["chunk_id"],)
            ).fetchone()[0]
            (layout.root / raw).write_bytes(b"broken new Raw")
    if anomaly == "corrupt-new":
        _corpus, result, _opened = audit(tmp_path, layout, boundary)
        assert result["result"] == "FAIL" and result["blocking_findings"]
    else:
        with pytest.raises(AcceptanceError):
            audit(tmp_path, layout, boundary)


def test_existing_unfinished_chunk_cannot_be_outside_scope(tmp_path: Path) -> None:
    layout, _old, _new, _boundary = fixture(tmp_path)
    with pytest.raises(AcceptanceError, match="drained"):
        capture_boundary(
            layout.root, inventory_root=tmp_path / "start", utc_ns=10, boot_id="boot-a"
        )


def test_boot_change_cannot_freeze_selected_scope(tmp_path: Path) -> None:
    layout, _old, _new, boundary = fixture(tmp_path)
    with pytest.raises(AcceptanceError, match="boot"):
        freeze_selected_corpus(
            data_root=layout.root,
            root=tmp_path / "evidence",
            identity=_identity(tmp_path),
            boundary=boundary,
            anchor_sha256="a" * 64,
            boot_id="other",
            probe=stopped,
        )


def test_post_start_event_cannot_import_pre_start_gap_companion(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path / "data")
    gap = dict(market="um_perpetual", symbol="BTCUSDT", stream="book_ticker", gap_id="a")
    with Catalog(layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        catalog.record_operational_event(
            event_id="old-gap",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=1_000_000_001,
            symbol="BTCUSDT",
            evidence={
                **gap,
                "original_connection_id": "old",
                "original_generation": 0,
                "gap_started_at_utc_ns": 1_000_000_001,
            },
        )
        boundary = capture_boundary(
            layout.root, inventory_root=tmp_path / "start", utc_ns=10, boot_id="boot-a"
        )
        catalog.record_operational_event(
            event_id="new-gap",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=1_000_000_002,
            symbol="BTCUSDT",
            evidence={
                **gap,
                "new_connection_id": "new",
                "new_generation": 1,
                "gap_ended_at_utc_ns": 1_000_000_002,
            },
        )
        seal_chunk(layout, catalog, [usdm_envelope("new", 2)])
    _corpus, result, _opened = audit(tmp_path, layout, boundary)
    assert result["result"] == "FAIL"
    assert "operational_discontinuity_authority_malformed" in result["blocking_findings"]
    with sqlite3.connect(tmp_path / "evidence/catalog.sqlite") as db:
        assert db.execute("SELECT event_id FROM operational_events").fetchall() == [("new-gap",)]


def test_zero_saved_frames_cannot_establish_percentage(tmp_path: Path) -> None:
    layout, _old, new, boundary = fixture(tmp_path)
    with sqlite3.connect(layout.catalog) as db:
        db.execute("UPDATE chunks SET record_count=0 WHERE chunk_id=?", (new["chunk_id"],))
    with pytest.raises(AcceptanceError, match="sealed/drained"):
        audit(tmp_path, layout, boundary)


def test_metadata_mutation_during_selected_freeze_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import qualification_scoped_corpus as module

    layout, _old, new, boundary = fixture(tmp_path)
    original = snapshot_manifest

    def changed(*args: Any, **kwargs: Any) -> dict[str, Any]:
        result = original(*args, **kwargs)
        with sqlite3.connect(layout.catalog) as db:
            db.execute(
                "UPDATE chunks SET stored_bytes=stored_bytes+1 WHERE chunk_id=?", (new["chunk_id"],)
            )
        return result

    monkeypatch.setattr(module, "snapshot_manifest", changed)
    with pytest.raises(AcceptanceError, match="metadata changed"):
        audit(tmp_path, layout, boundary)


def test_owner_result_requires_dual_live_and_exact_completed_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.service import acceptance_v5_finalize as released
    from tools import qualification_scoped_corpus as module

    layout, _old, new, boundary = fixture(tmp_path)
    opened: list[str] = []
    original_freeze = module.freeze_selected_corpus
    original_run, original_verify = released.run_audit, released.verify_audit

    def raw_unit(task: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        opened.append(task["manifest"]["chunk_id"])
        return qualify_unit(task)

    def freeze(**kwargs: Any) -> Any:
        return original_freeze(**kwargs, probe=stopped)

    def run(**kwargs: Any) -> Any:
        return original_run(**kwargs, probe=stopped, raw_unit=raw_unit)

    def verify(**kwargs: Any) -> Any:
        kwargs["raw_unit"] = raw_unit
        return original_verify(**kwargs)

    monkeypatch.setattr(module, "freeze_selected_corpus", freeze)
    monkeypatch.setattr(released, "run_audit", run)
    monkeypatch.setattr(released, "verify_audit", verify)
    result = module.audit_selected_capture(
        root=tmp_path / "evidence",
        data_root=layout.root,
        identity=_identity(tmp_path),
        boundary=boundary,
        run_id="owner-new",
        target_sha256="a" * 64,
        boot_id="boot-a",
        products=[["um_perpetual", "BTCUSDT"]],
        archive_roots={},
    )
    assert result["run_id"] == "owner-new" and result["selected_counts"]["frames"] == 2
    assert result["live_passes"] == 2 and opened == [new["chunk_id"]] * 2
    assert result["canonical_v5_final_eligible"] is False
    assert result["audit_sha256"] == result["completed_control_verification_sha256"]
