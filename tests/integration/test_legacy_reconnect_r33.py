"""M21.4.11-R3.3: legacy reconnect authority hardening (PR #11 R3.3).

Regression tests for the independent exact-head R3.2 review findings:

- REV-001 (P1): historical parent authority can disappear from the searched
  universe, making "no possible parent" an unsound positive legitimacy
  proof.  The legacy no-parent absence proof is removed; intents generated
  by the R3.3+ runtime carry a durable schema/provenance version whose
  runtime prevention contract makes a fresh ABSENT versioned intent safely
  materializable (REQ-103); unversioned legacy candidates without positive
  proof are AMBIGUOUS; malformed lifecycle authority is surfaced as
  explicit global predecision blockers, never as absence evidence.
- REV-002 (P1): the authority digest must bind the COMPLETE immutable
  candidate evidence the operator reviewed (chunk_id + seal intent +
  verified_frames).
- REV-003 (P1): the documented authority ownership/mode must be readable
  by the configured production service (User=orangepi Group=orangepi) and
  not writable by it.
- REV-004 (P2): the read-only preflight must never create storage layout.

No production-specific UUIDs, timestamps, or authority files are
hard-coded anywhere.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.spool.legacy_reconnect import (
    LegacyClassificationAuthority,
    LegacyReconnectConflictError,
    evaluate_legacy_reconnect_decisions,
)
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    recover_storage,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_orphan_extension_intent import (
    _legacy_orphan_fixture,
    _seal_zero_record_marker,
    _started_events,
)
from tests.integration.test_reconnect_boundary_integrity import (
    _durable_intent,
    _record_gap,
    _seal_chunk_with_intent,
    book_ticker,
    sealing_intent,
)


def _no_opener(*_args: Any, **_kwargs: Any) -> Any:  # pragma: no cover
    raise AssertionError("opener must not be used")


def _versioned_intent(**kwargs: Any) -> dict[str, Any]:
    """A `_durable_intent` carrying the current R3.3 runtime schema."""
    return _durable_intent(intent_schema="reconnect-seal-intent.v2", **kwargs)


def _write_v3_authority(root: Path, entries: list[dict[str, Any]]) -> None:
    document = {
        "schema": "legacy-reconnect-classification.v3",
        "classifications": entries,
    }
    (root / "legacy_reconnect_classifications.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8"
    )


def _evidence_digest(catalog: Catalog, chunk_id: str) -> str:
    from binance_market_data_recorder.spool.legacy_reconnect import (
        classification_evidence_sha256,
    )
    from binance_market_data_recorder.storage.catalog import ChunkState

    evidence = catalog.latest_transition_evidence(
        chunk_id, ChunkState.SEALING
    )
    assert evidence is not None
    return classification_evidence_sha256(
        chunk_id=chunk_id,
        intent=sealing_intent(catalog, chunk_id),
        verified_frames=evidence.get("verified_frames"),
    )


def _classification_entry(
    root: Path, chunk_id: str, *, classification: str
) -> dict[str, Any]:
    with Catalog(ensure_storage_layout(root).catalog, read_only=True) as catalog:
        intent = sealing_intent(catalog, chunk_id)
        return {
            "gap_id": str(intent["gap_id"]),
            "market": str(intent["market"]),
            "stream": str(intent["stream"]),
            "chunk_id": chunk_id,
            "classification_evidence_sha256": _evidence_digest(catalog, chunk_id),
            "classification": classification,
            "note": "test-reviewed classification",
        }


def _set_sealing_evidence_verified_frames(
    catalog: Catalog, chunk_id: str, intent: dict[str, Any], frames: int
) -> None:
    payload = json.dumps(
        {"verified_frames": frames, "seal_intent": dict(intent)},
        sort_keys=True,
        separators=(",", ":"),
    )
    cursor = catalog._connection.execute(
        "UPDATE chunk_transitions SET evidence_json = ? "
        "WHERE chunk_id = ? AND to_state = 'SEALING'",
        (payload, chunk_id),
    )
    assert cursor.rowcount == 1


def _record_malformed_lifecycle(
    catalog: Catalog,
    *,
    event_id: str,
    event_type: str,
    market: object = "um_perpetual",
    stream: object = "book_ticker",
    gap_id: object = "malformed-gap",
    original_connection_id: object = "conn-x",
    original_generation: object = 0,
    new_connection_id: object = "conn-y",
    new_generation: object = 1,
) -> None:
    evidence: dict[str, Any] = {
        "market": market,
        "symbol": "BTCUSDT",
        "stream": stream,
        "gap_id": gap_id,
        "reason": "unexpected_disconnect",
        "interval_classification": "UNRELIABLE",
        "gap_started_at_utc_ns": 1_000_000_000,
        "original_connection_id": original_connection_id,
        "original_generation": original_generation,
    }
    if event_type == "STREAM_DISCONTINUITY_COMPLETED":
        evidence["gap_ended_at_utc_ns"] = 2_000_000_000
        evidence["new_connection_id"] = new_connection_id
        evidence["new_generation"] = new_generation
    catalog.record_operational_event(
        event_id=event_id,
        event_type=event_type,
        occurred_at_utc_ns=1_000_000_000,
        evidence=evidence,
        symbol="BTCUSDT",
    )


def _report(root: Path) -> Any:
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LEGACY_CLASSIFICATION_FILENAME,
    )

    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog, read_only=True) as catalog:
        return evaluate_legacy_reconnect_decisions(
            catalog=catalog,
            authority=LegacyClassificationAuthority.load(
                layout.root / LEGACY_CLASSIFICATION_FILENAME
            ),
        )


def _run_preflight_cli(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> tuple[int, dict[str, Any]]:
    import binance_market_data_recorder.cli as cli_module

    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    exit_code = cli_module.main(["recovery", "legacy-reconnect-preflight"])
    payload = json.loads(capsys.readouterr().out)
    return exit_code, payload


# ---------------------------------------------------------------------------
# REV-001: remove the legacy no-parent absence proof; version new intents.
# ---------------------------------------------------------------------------


def test_versioned_intent_only_crash_materializes_exactly_once(
    tmp_path: Path,
) -> None:
    """R3.3-001 (REQ-103): a versioned genuine intent-only crash (STARTED
    absent, zero frames, no competing OPEN authority) materializes exactly
    one STARTED, and repeated recovery never duplicates it."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _versioned_intent(
            gap_id="genuine-v2-g1",
            reason="planned_rotation",
            connection_id="conn-v2",
            generation=3,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    recovered = Catalog(layout.catalog)
    first_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "genuine-v2-g1"
        for action in first_actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        assert [
            event["evidence"]["gap_id"] for event in _started_events(catalog)
        ] == ["genuine-v2-g1"]

    recovered = Catalog(layout.catalog)
    second_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in second_actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        assert [
            event["evidence"]["gap_id"] for event in _started_events(catalog)
        ] == ["genuine-v2-g1"]


def test_unversioned_zero_frame_without_positive_proof_is_ambiguous(
    tmp_path: Path,
) -> None:
    """R3.3-002 (REV-001 RED): an UNVERSIONED zero-frame intent whose
    durable identity cannot positively prove legitimacy must NEVER become
    PROVEN_LEGITIMATE through "no parent found".  It is AMBIGUOUS and
    startup fails closed."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="legacy-g1",
            reason="planned_rotation",
            connection_id="conn-legacy",
            generation=0,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    report = _report(tmp_path)
    assert report.candidate_count == 1
    decision = report.decisions[0]
    assert decision.automatic != "proven_legitimate"
    assert decision.automatic == "ambiguous"
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(
        RecoveryConflictError, match="RECOVERY_LEGACY_ORPHAN_AMBIGUOUS"
    ):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_unversioned_frame_bearing_intent_still_proven_legitimate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R3.3-003 (legacy positive proof A): trustworthy
    ``verified_frames > 0`` remains a sound positive legitimacy proof for
    unversioned candidates."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="legacy-frames-g1",
            reason="planned_rotation",
            connection_id="conn-frames",
            generation=7,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_chunk_with_intent(
            layout,
            catalog,
            monkeypatch,
            intent=intent,
            frame_payload=book_ticker(7),
        )

    report = _report(tmp_path)
    assert report.decisions[0].automatic == "proven_legitimate"
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "legacy-frames-g1"
        for action in actions
    )


def test_unversioned_completing_connection_still_proven_legitimate(
    tmp_path: Path,
) -> None:
    """R3.3-004 (legacy positive proof B): the exact completing-connection
    proof remains a sound positive legitimacy proof for unversioned
    candidates."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        genuine = _durable_intent(
            gap_id="genuine-g2",
            reason="planned_rotation",
            connection_id="conn-parent-2",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, genuine, stream=genuine["stream"]
        )

    report = _report(tmp_path)
    assert report.decisions[0].automatic == "proven_legitimate"


def test_unknown_future_intent_schema_fails_closed(tmp_path: Path) -> None:
    """R3.3-005: an intent carrying an UNKNOWN future schema version must
    fail closed, never be treated as unversioned."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="future-g1",
            reason="planned_rotation",
            connection_id="conn-future",
            generation=0,
            started_at_utc_ns=5_000_000_000,
            intent_schema="reconnect-seal-intent.v9",
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    with pytest.raises(
        RecoveryConflictError, match="RECOVERY_SEAL_INTENT_UNSUPPORTED_SCHEMA"
    ):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_malformed_intent_schema_type_fails_closed(tmp_path: Path) -> None:
    """R3.3-006: a non-text intent schema field fails closed."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="bad-schema-g1",
            reason="planned_rotation",
            connection_id="conn-bad",
            generation=0,
            started_at_utc_ns=5_000_000_000,
        )
        intent["intent_schema"] = 123
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    with pytest.raises(
        RecoveryConflictError, match="RECOVERY_SEAL_INTENT_UNSUPPORTED_SCHEMA"
    ):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_spot_and_usdm_runtime_intents_carry_current_schema(
    tmp_path: Path,
) -> None:
    """R3.3-007 (SPOT/USD-M parity): both collectors emit the current
    ``reconnect-seal-intent.v2`` provenance on fresh AND pure-extension
    intents."""
    from tests.integration.test_orphan_extension_intent import (
        _make_spot_collector,
    )
    from tests.integration.test_reconnect_boundary_integrity import (
        make_collector,
    )

    async def usdm_staged() -> dict[str, Any]:
        collector, catalog, _spool = make_collector(
            tmp_path, opener=_no_opener
        )
        try:
            collector._boundary_connection_id = "conn-a"
            collector._boundary_detected_at_utc_ns = 123
            collector._generation = 2
            collector._pending_gap = None
            collector._recovery_marker_enqueued = False
            fresh = collector._build_seal_intent("planned_rotation", None)
            collector._pending_gap = {
                "gap_id": "parent-g1",
                "reason": "unexpected_disconnect",
                "original_connection_id": "conn-p",
                "original_generation": 0,
                "gap_started_at_utc_ns": 1,
            }
            extension = collector._build_seal_intent("session_restart", None)
            return {
                "fresh": cast(dict[str, object], fresh),
                "extension": cast(dict[str, object], extension),
            }
        finally:
            catalog.close()

    usdm = asyncio.run(usdm_staged())
    assert usdm["fresh"]["intent_schema"] == "reconnect-seal-intent.v2"
    assert usdm["extension"]["intent_schema"] == "reconnect-seal-intent.v2"
    assert usdm["extension"]["gap_id"] == "parent-g1"

    spot_collector, spot_catalog, _spot_spool = _make_spot_collector(
        tmp_path, opener=_no_opener
    )
    try:
        spot_collector._boundary_connection_id = "conn-a"
        spot_collector._boundary_detected_at_utc_ns = 123
        spot_collector._generation = 2
        spot_collector._pending_gap = None
        spot_collector._recovery_marker_enqueued = False
        fresh = spot_collector._build_seal_intent("planned_rotation", None)
        spot_collector._pending_gap = {
            "gap_id": "parent-g1",
            "reason": "unexpected_disconnect",
            "original_connection_id": "conn-p",
            "original_generation": 0,
            "gap_started_at_utc_ns": 1,
        }
        extension = spot_collector._build_seal_intent("session_restart", None)
    finally:
        spot_catalog.close()
    assert fresh is not None and fresh["intent_schema"] == (
        "reconnect-seal-intent.v2"
    )
    assert extension is not None and extension["intent_schema"] == (
        "reconnect-seal-intent.v2"
    )
    assert extension["gap_id"] == "parent-g1"


# ---------------------------------------------------------------------------
# REV-001: malformed historical lifecycle authority.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field,value",
    [
        ("gap_id", None),
        ("gap_id", ""),
        ("market", None),
        ("stream", None),
    ],
    ids=["missing_gap_id", "empty_gap_id", "missing_market", "missing_stream"],
)
def test_unkeyable_lifecycle_authority_never_grants_absence_legitimacy(
    tmp_path: Path, field: str, value: object
) -> None:
    """R3.3-010 (REV-001 RED): a malformed/unkeyable lifecycle row must
    widen uncertainty, never reduce the parent universe.  An unversioned
    zero-frame candidate beside it is NEVER automatically
    PROVEN_LEGITIMATE; the malformed row is surfaced as an explicit
    degraded-authority blocker."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        kwargs: dict[str, Any] = {"event_id": f"malformed-{field}"}
        kwargs[field] = value
        _record_malformed_lifecycle(
            catalog, event_type="STREAM_DISCONTINUITY_STARTED", **kwargs
        )
        candidate = _durable_intent(
            gap_id="cand-g1",
            reason="planned_rotation",
            connection_id="conn-cand",
            generation=9,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, candidate, stream=candidate["stream"]
        )

    report = _report(tmp_path)
    decision = next(
        decision
        for decision in report.decisions
        if decision.candidate.gap_id == "cand-g1"
    )
    assert decision.automatic != "proven_legitimate"
    assert decision.automatic == "ambiguous"
    assert report.degraded_authority_count == 1
    assert report.degraded_authority[0]["reason"] == f"missing_{field}"
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


@pytest.mark.parametrize(
    "variant",
    ["malformed_generation", "malformed_connection_identity"],
)
def test_degraded_closed_pair_blocks_predecision(
    tmp_path: Path, variant: str
) -> None:
    """R3.3-011: a STARTED/COMPLETED pair keyed by the same gap_id whose
    identity fields are malformed is surfaced as an explicit degraded
    closed pair (a predecision blocker), and an unversioned zero-frame
    candidate beside it stays AMBIGUOUS."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        started: dict[str, Any]
        completed: dict[str, Any]
        if variant == "malformed_generation":
            started = {"original_generation": "seven"}
            completed = {"new_generation": 1}
        else:
            started = {"original_connection_id": 7}
            completed = {"new_connection_id": "conn-y"}
        _record_malformed_lifecycle(
            catalog,
            event_type="STREAM_DISCONTINUITY_STARTED",
            event_id="degraded-start",
            gap_id="degraded-pair-g1",
            **started,
        )
        _record_malformed_lifecycle(
            catalog,
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            event_id="degraded-complete",
            gap_id="degraded-pair-g1",
            **completed,
        )
        candidate = _durable_intent(
            gap_id="cand-g1",
            reason="planned_rotation",
            connection_id="conn-cand",
            generation=9,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, candidate, stream=candidate["stream"]
        )

    report = _report(tmp_path)
    decision = next(
        decision
        for decision in report.decisions
        if decision.candidate.gap_id == "cand-g1"
    )
    assert decision.automatic != "proven_legitimate"
    assert decision.automatic == "ambiguous"
    assert report.degraded_authority_count == 1
    degraded = report.degraded_authority[0]
    assert degraded["gap_id"] == "degraded-pair-g1"
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_preflight_surfaces_malformed_history_traceably(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.3-012 (preflight output): malformed lifecycle authority appears
    in the machine-readable preflight output with event identity and
    reason, never merely as a count."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        _record_malformed_lifecycle(
            catalog,
            event_type="STREAM_DISCONTINUITY_STARTED",
            event_id="malformed-trace",
            gap_id=None,
        )

    exit_code, payload = _run_preflight_cli(monkeypatch, capsys, tmp_path)
    assert exit_code == 2
    assert payload["first_corrected_startup_eligible"] is False
    assert payload["degraded_authority_count"] == 1
    assert payload["degraded_authority"][0]["event_id"] == "malformed-trace"
    assert payload["degraded_authority"][0]["reason"] == "missing_gap_id"
    assert payload["degraded_authority"][0]["blocks_classification"] is True


# ---------------------------------------------------------------------------
# REV-002: classification evidence digest covers the full decision evidence.
# ---------------------------------------------------------------------------


def test_digest_matrix_every_decision_field_invalidates_binding() -> None:
    """R3.3-020 (REV-002 RED): the classification evidence digest must
    change when ANY immutable candidate-side decision field changes:
    chunk_id, every seal-intent identity field, the intent schema, and
    verified_frames."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        classification_evidence_sha256,
    )

    base = _durable_intent(
        gap_id="g1",
        reason="planned_rotation",
        connection_id="conn-1",
        generation=1,
        started_at_utc_ns=100,
    )
    chunk_id = "11111111-2222-3333-4444-555555555555"
    base_digest = classification_evidence_sha256(
        chunk_id=chunk_id, intent=base, verified_frames=0
    )

    variations: list[tuple[str, object, dict[str, Any]]] = [
        ("chunk_id", "22222222-3333-4444-5555-666666666666", {}),
        ("gap_id", "g2", {"gap_id": "g2"}),
        ("market", "spot", {"market": "spot"}),
        ("stream", "agg_trade", {"stream": "agg_trade"}),
        ("reason", "session_restart", {"reason": "session_restart"}),
        (
            "original_connection_id",
            "conn-2",
            {"original_connection_id": "conn-2"},
        ),
        ("original_generation", 2, {"original_generation": 2}),
        ("gap_started_at_utc_ns", 200, {"gap_started_at_utc_ns": 200}),
        (
            "intent_schema",
            "reconnect-seal-intent.v2",
            {"intent_schema": "reconnect-seal-intent.v2"},
        ),
        ("verified_frames", 1, {}),
    ]
    for name, expected, intent_updates in variations:
        variant_intent = {**base, **intent_updates}
        variant_frames: object = 0
        variant_chunk = chunk_id
        if name == "verified_frames":
            variant_frames = expected
        if name == "chunk_id":
            variant_chunk = str(expected)
        digest = classification_evidence_sha256(
            chunk_id=variant_chunk,
            intent=variant_intent,
            verified_frames=variant_frames,
        )
        assert digest != base_digest, f"{name} change must invalidate binding"
        assert len(digest) == 64 and all(
            character in "0123456789abcdef" for character in digest
        )


def _verified_frames_binding_fixture(
    root: Path, *, authority_for_frames: int
) -> tuple[str, Path]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="binding-g1",
            reason="planned_rotation",
            connection_id="conn-binding",
            generation=1,
            started_at_utc_ns=1_000_000_000,
        )
        chunk_id = _seal_zero_record_marker(
            root, catalog, intent, stream=intent["stream"]
        )
    with Catalog(layout.catalog) as catalog:
        stored = sealing_intent(catalog, chunk_id)
        if authority_for_frames != 0:
            _set_sealing_evidence_verified_frames(
                catalog, chunk_id, stored, authority_for_frames
            )
    _write_v3_authority(
        root,
        [_classification_entry(root, chunk_id, classification="extension_orphan")],
    )
    with Catalog(layout.catalog) as catalog:
        stored = sealing_intent(catalog, chunk_id)
        if authority_for_frames == 0:
            _set_sealing_evidence_verified_frames(catalog, chunk_id, stored, 1)
        else:
            _set_sealing_evidence_verified_frames(catalog, chunk_id, stored, 0)
    return chunk_id, layout.catalog


def test_verified_frames_change_invalidates_authority_binding(
    tmp_path: Path,
) -> None:
    """R3.3-021 (REV-002 RED): an authority binding created against
    ``verified_frames == 0`` must become STALE when the immutable
    persisted evidence changes to ``verified_frames == 1``; the old
    classification is never consumed."""
    chunk_id, catalog_path = _verified_frames_binding_fixture(
        tmp_path, authority_for_frames=0
    )
    report = _report(tmp_path)
    decision = next(
        decision
        for decision in report.decisions
        if decision.candidate.chunk_id == chunk_id
    )
    assert decision.classification is None
    assert report.stale_authority_count == 1
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(
            layout=ensure_storage_layout(tmp_path),
            catalog=Catalog(catalog_path),
        )


def test_verified_frames_reverse_direction_invalidates_authority_binding(
    tmp_path: Path,
) -> None:
    """R3.3-022: the reverse direction of the verified_frames binding:
    authority created against ``verified_frames == 1`` goes STALE when
    the evidence becomes ``verified_frames == 0``."""
    chunk_id, catalog_path = _verified_frames_binding_fixture(
        tmp_path, authority_for_frames=1
    )
    report = _report(tmp_path)
    decision = next(
        decision
        for decision in report.decisions
        if decision.candidate.chunk_id == chunk_id
    )
    assert decision.authority_state == "STALE"
    assert decision.classification is None
    assert report.stale_authority_count == 1
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(
            layout=ensure_storage_layout(tmp_path),
            catalog=Catalog(catalog_path),
        )


def test_v3_authority_rejects_old_v2_and_v1_schemas(tmp_path: Path) -> None:
    """R3.3-023: authority schema v1 and v2 (never deployed) are rejected;
    only the current v3 schema is accepted."""
    from binance_market_data_recorder.spool.legacy_reconnect import (
        LegacyClassificationAuthority,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _versioned_intent(
            gap_id="schema-g1",
            reason="planned_rotation",
            connection_id="conn-schema",
            generation=3,
            started_at_utc_ns=5_000_000_000,
        )
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )
    authority_path = layout.root / "legacy_reconnect_classifications.json"
    for schema in (
        "legacy-reconnect-classification.v1",
        "legacy-reconnect-classification.v2",
    ):
        authority_path.write_text(
            json.dumps({"schema": schema, "classifications": []}),
            encoding="utf-8",
        )
        with pytest.raises(
            LegacyReconnectConflictError,
            match="RECOVERY_LEGACY_CLASSIFICATION_MALFORMED",
        ):
            from binance_market_data_recorder.spool.legacy_reconnect import (
                LEGACY_CLASSIFICATION_FILENAME,
            )

            LegacyClassificationAuthority.load(
                layout.root / LEGACY_CLASSIFICATION_FILENAME
            )


def test_authority_never_overrides_later_proven_context(
    tmp_path: Path,
) -> None:
    """R3.3-024 (context staleness): an authority that resolved an
    AMBIGUOUS candidate in an earlier pass must NOT override a decision
    later PROVEN from changed historical context.  Startup recomputes the
    automatic decision first and the contradiction fails closed."""
    _parent, _orphan, orphan_chunk_id, _digest = _legacy_orphan_fixture(
        tmp_path
    )
    _write_v3_authority(
        tmp_path,
        [
            _classification_entry(
                tmp_path, orphan_chunk_id, classification="extension_orphan"
            )
        ],
    )
    layout = ensure_storage_layout(tmp_path)
    recovered = Catalog(layout.catalog)
    first_actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in first_actions
    )

    with Catalog(layout.catalog) as catalog:
        later_parent = _durable_intent(
            gap_id="later-parent-g3",
            reason="unexpected_disconnect",
            connection_id="conn-later",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, later_parent, completed=False)
        _record_gap(
            catalog,
            later_parent,
            completed=True,
            new_connection_id="conn-attempt",
            new_generation=1,
            gap_ended_at_utc_ns=4_000_000_000,
        )

    report = _report(tmp_path)
    decision = next(
        decision
        for decision in report.decisions
        if decision.candidate.gap_id == "orphan-g2"
    )
    assert decision.automatic == "proven_legitimate"
    assert report.contradiction_count == 1
    assert report.first_corrected_startup_eligible is False
    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_versioned_legit_plus_legacy_ambiguous_blocks_all_then_resolves(
    tmp_path: Path,
) -> None:
    """R3.3-025 (global predecision): a versioned legitimate candidate A
    plus an unresolved legacy ambiguous candidate B must fail the GLOBAL
    Phase A pass without materializing A's STARTED.  After B receives an
    exact valid authority, the same recovery materializes A and resolves
    B."""
    layout = ensure_storage_layout(tmp_path)
    orphan_chunk_id = ""
    with Catalog(layout.catalog) as catalog:
        versioned = _versioned_intent(
            gap_id="versioned-a",
            reason="planned_rotation",
            connection_id="conn-v2-a",
            generation=7,
            started_at_utc_ns=5_000_000_000,
            stream="agg_trade",
        )
        _seal_zero_record_marker(
            tmp_path, catalog, versioned, stream=versioned["stream"]
        )
        parent = _durable_intent(
            gap_id="parent-g1",
            reason="unexpected_disconnect",
            connection_id="conn-parent",
            generation=0,
            started_at_utc_ns=1_000_000_000,
        )
        _record_gap(catalog, parent, completed=False)
        _record_gap(
            catalog,
            parent,
            completed=True,
            new_connection_id="conn-parent-2",
            new_generation=1,
            gap_ended_at_utc_ns=3_000_000_000,
        )
        orphan = _durable_intent(
            gap_id="ambig-b",
            reason="session_restart",
            connection_id="conn-attempt",
            generation=1,
            started_at_utc_ns=2_000_000_000,
        )
        orphan_chunk_id = _seal_zero_record_marker(
            tmp_path, catalog, orphan, stream=orphan["stream"]
        )

    with pytest.raises(RecoveryConflictError, match="RECOVERY_LEGACY_PREDECISION"):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))
    with Catalog(layout.catalog, read_only=True) as catalog:
        assert [
            event["evidence"]["gap_id"] for event in _started_events(catalog)
        ] == ["parent-g1"]

    _write_v3_authority(
        tmp_path,
        [
            _classification_entry(
                tmp_path, orphan_chunk_id, classification="extension_orphan"
            )
        ],
    )
    recovered = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        and action.detail == "versioned-a"
        for action in actions
    )
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "ambig-b"
        for action in actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        assert [
            event["evidence"]["gap_id"] for event in _started_events(catalog)
        ] == ["parent-g1", "versioned-a"]


# ---------------------------------------------------------------------------
# REV-003: production permission contract.
# ---------------------------------------------------------------------------


def test_authority_permission_contract_matches_service_identity() -> None:
    """R3.4-030 (REV-003-R3.3-001): the authority trust boundary must be
    real at PATHNAME level, not only at file-mode level.

    The documented production contract is validated against the systemd
    service identity (User=orangepi Group=orangepi):

    - FILE root:orangepi 0640: service can read, cannot write contents,
      world cannot read;
    - PARENT root:orangepi 0750: service can traverse, cannot write the
      directory — a directory write bit is exactly what would allow the
      service to unlink/rename/replace the authority pathname;
    - the R3.3 location (authority inside the service-owned data root,
      owner orangepi mode 0750) is shown to grant the service that
      replace capability, which is why R3.4 moved the authority into the
      root-controlled configuration namespace.
    """
    from binance_market_data_recorder.service.systemd import SystemdManager
    from binance_market_data_recorder.spool.legacy_reconnect import (
        CLASSIFICATION_AUTHORITY_GROUP,
        CLASSIFICATION_AUTHORITY_MODE,
        CLASSIFICATION_AUTHORITY_OWNER,
        CLASSIFICATION_AUTHORITY_PARENT_GROUP,
        CLASSIFICATION_AUTHORITY_PARENT_MODE,
        CLASSIFICATION_AUTHORITY_PARENT_OWNER,
        classification_authority_permissions,
        directory_permissions,
    )

    assert CLASSIFICATION_AUTHORITY_OWNER == "root"
    assert CLASSIFICATION_AUTHORITY_GROUP == "orangepi"
    assert CLASSIFICATION_AUTHORITY_MODE == 0o640
    assert CLASSIFICATION_AUTHORITY_PARENT_OWNER == "root"
    assert CLASSIFICATION_AUTHORITY_PARENT_GROUP == "orangepi"
    assert CLASSIFICATION_AUTHORITY_PARENT_MODE == 0o750

    file_access = classification_authority_permissions(
        owner=CLASSIFICATION_AUTHORITY_OWNER,
        group=CLASSIFICATION_AUTHORITY_GROUP,
        mode=CLASSIFICATION_AUTHORITY_MODE,
        user="orangepi",
        user_group="orangepi",
    )
    assert file_access == {"readable": True, "writable": False}

    parent_access = directory_permissions(
        owner=CLASSIFICATION_AUTHORITY_PARENT_OWNER,
        group=CLASSIFICATION_AUTHORITY_PARENT_GROUP,
        mode=CLASSIFICATION_AUTHORITY_PARENT_MODE,
        user="orangepi",
        user_group="orangepi",
    )
    assert parent_access["traversable"] is True
    assert parent_access["writable"] is False
    service_can_replace = parent_access["writable"]
    assert service_can_replace is False

    world_file = classification_authority_permissions(
        owner=CLASSIFICATION_AUTHORITY_OWNER,
        group=CLASSIFICATION_AUTHORITY_GROUP,
        mode=CLASSIFICATION_AUTHORITY_MODE,
        user="somebody-else",
        user_group="somebody-else",
    )
    world_parent = directory_permissions(
        owner=CLASSIFICATION_AUTHORITY_PARENT_OWNER,
        group=CLASSIFICATION_AUTHORITY_PARENT_GROUP,
        mode=CLASSIFICATION_AUTHORITY_PARENT_MODE,
        user="somebody-else",
        user_group="somebody-else",
    )
    assert world_file == {"readable": False, "writable": False}
    assert world_parent == {
        "readable": False,
        "writable": False,
        "traversable": False,
    }

    # Counter-contract: the R3.3 location put the authority inside the
    # service-owned data root (owner orangepi, mode 0750).  The service
    # principal therefore held the parent-directory write bit and could
    # unlink/rename/replace the authority pathname despite file 0640.
    data_root_parent = directory_permissions(
        owner="orangepi",
        group="orangepi",
        mode=0o750,
        user="orangepi",
        user_group="orangepi",
    )
    assert data_root_parent["writable"] is True

    manager = SystemdManager(
        data_root=Path("/var/lib/binance-market-data-recorder"),
        config_file=Path("/etc/binance-market-data-recorder/recorder.toml"),
        user="orangepi",
        group="orangepi",
    )
    unit = manager.unit()
    assert "User=orangepi" in unit
    assert "Group=orangepi" in unit


# ---------------------------------------------------------------------------
# REV-004: preflight must be intrinsically read-only.
# ---------------------------------------------------------------------------


def _tree(root: Path) -> list[str]:
    return sorted(str(path.relative_to(root)) for path in root.rglob("*"))


def test_preflight_never_creates_missing_layout_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.3-040 (REV-004 RED): the read-only preflight must not (re)create
    missing storage-layout directories, even when the Catalog exists."""
    _legacy_orphan_fixture(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    missing = layout.sealed
    shutil.rmtree(missing)
    assert not missing.exists()
    before = _tree(tmp_path)

    exit_code, payload = _run_preflight_cli(monkeypatch, capsys, tmp_path)

    assert exit_code == 2
    assert payload["first_corrected_startup_eligible"] is False
    assert not missing.exists()
    assert _tree(tmp_path) == before


def test_preflight_ineligible_exits_nonzero_with_full_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.3-041: an ineligible preflight emits the full machine-readable
    JSON report and exits with a documented nonzero status so automation
    cannot ignore ineligibility."""
    _legacy_orphan_fixture(tmp_path)
    exit_code, payload = _run_preflight_cli(monkeypatch, capsys, tmp_path)
    assert exit_code == 2
    assert payload["first_corrected_startup_eligible"] is False
    assert payload["unclassified_ambiguous_count"] == 1
    assert payload["candidate_count"] == 1


def test_preflight_eligible_exits_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.3-042: a fully classified preflight exits zero and reports
    eligible=true through the same shared engine."""
    _parent, _orphan, orphan_chunk_id, _digest = _legacy_orphan_fixture(
        tmp_path
    )
    _write_v3_authority(
        tmp_path,
        [
            _classification_entry(
                tmp_path, orphan_chunk_id, classification="extension_orphan"
            )
        ],
    )
    exit_code, payload = _run_preflight_cli(monkeypatch, capsys, tmp_path)
    assert exit_code == 0
    assert payload["first_corrected_startup_eligible"] is True
