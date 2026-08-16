"""M21.4.11-R3.4: authority path trust boundary and two P2 closures.

Focused correction on top of the accepted R3.3 design (the R3.3 legacy
reconnect algorithm is otherwise unchanged):

- REV-003-R3.3-001 (P1): the operator classification authority must live
  in the ROOT-CONTROLLED configuration namespace (next to the loaded
  Recorder configuration file), never inside the service-writable data
  root.  Even though file mode 0640 denies content writes, the service
  principal owns the data-root directory and could unlink/rename/replace
  the authority pathname; the trust boundary must be real at pathname
  level.  Production: ``/etc/binance-market-data-recorder/
  legacy_reconnect_classifications.json`` (parent root:orangepi 0750).
- R3.3-SCHEMA-001 (P2): an explicit ``intent_schema: null`` used to be
  treated like a missing key (legacy) because ``.get()`` returned None;
  only a MISSING key is legacy now — any present value that is not the
  exact current schema fails closed.
- R3.3-DOC-001 (P2): stale Ubuntu 72h status wording corrected in
  ``docs/ubuntu_rk3588_operations.md`` (the deployed artifact
  ``f659895…`` passed the formal 72h observational gate; the corrected
  artifact is NOT deployed and production validation is PENDING).

No production-specific UUIDs or authority files are hard-coded anywhere.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from binance_market_data_recorder.spool.legacy_reconnect import (
    LEGACY_CLASSIFICATION_FILENAME,
    LegacyClassificationAuthority,
    classification_authority_path,
)
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    recover_storage,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_legacy_reconnect_r33 import (
    _classification_entry,
    _write_v3_authority,
)
from tests.integration.test_orphan_extension_intent import (
    _legacy_orphan_fixture,
    _seal_zero_record_marker,
    _started_events,
)
from tests.integration.test_reconnect_boundary_integrity import _durable_intent

# ---------------------------------------------------------------------------
# REV-003-R3.3-001: authority path trust boundary.
# ---------------------------------------------------------------------------


def test_authority_path_derives_from_config_namespace() -> None:
    """R3.4-001: the production authority lives next to the loaded
    Recorder configuration, never inside the service-writable data
    root; the data-root location is only the config-less fallback."""
    production_config = Path(
        "/etc/binance-market-data-recorder/recorder.toml"
    )
    production_data = Path("/var/lib/binance-market-data-recorder")
    assert classification_authority_path(
        config_file=production_config, data_root=production_data
    ) == Path(
        "/etc/binance-market-data-recorder"
    ) / LEGACY_CLASSIFICATION_FILENAME

    interactive_data = Path("/home/user/.local/share/BinanceMarketDataRecorder")
    assert classification_authority_path(
        config_file=None, data_root=interactive_data
    ) == interactive_data / LEGACY_CLASSIFICATION_FILENAME


def test_preflight_and_startup_share_the_config_namespace_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """R3.4-002: preflight and startup load the SAME authority path.

    With an explicit config file, both use the config-directory
    authority (here classifying the orphan as ``extension_orphan``); a
    DIFFERENT authority deliberately left in the data root (classifying
    the same orphan as ``legitimate_req103``) must be ignored by both,
    proving the config-namespace rule end-to-end."""
    import binance_market_data_recorder.cli as cli_module

    data_root = tmp_path / "data-root"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "recorder.toml"
    config_file.write_text(
        f'[recorder]\ndata_root = "{data_root}"\n', encoding="utf-8"
    )

    _parent, _orphan, orphan_chunk_id, _digest = _legacy_orphan_fixture(
        data_root
    )
    _write_v3_authority(
        config_dir,
        [
            _classification_entry(
                data_root, orphan_chunk_id, classification="extension_orphan"
            )
        ],
    )
    _write_v3_authority(
        data_root,
        [
            _classification_entry(
                data_root, orphan_chunk_id, classification="legitimate_req103"
            )
        ],
    )

    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(data_root))
    exit_code = cli_module.main(
        ["--config", str(config_file), "recovery", "legacy-reconnect-preflight"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["first_corrected_startup_eligible"] is True
    candidates = payload["candidates"]
    orphan_decision = next(
        candidate
        for candidate in candidates
        if candidate["gap_id"] == "orphan-g2"
    )
    assert orphan_decision["final_decision"] == "classified_extension_orphan"

    layout = ensure_storage_layout(data_root)
    recovered = Catalog(layout.catalog)
    actions = recover_storage(
        layout=layout,
        catalog=recovered,
        authority_path=config_dir / LEGACY_CLASSIFICATION_FILENAME,
    )
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )
    assert not any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )
    with Catalog(layout.catalog, read_only=True) as catalog:
        assert [
            event["evidence"]["gap_id"] for event in _started_events(catalog)
        ] == ["parent-g1"]


def test_recover_storage_authority_default_is_data_root(tmp_path: Path) -> None:
    """R3.4-003: without an explicit authority_path, startup recovery
    keeps the config-less fallback (data-root authority) for
    interactive/test operation."""
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
    actions = recover_storage(layout=layout, catalog=recovered)
    recovered.close()
    assert any(
        action.action == "extension_orphan_ignored"
        and action.detail == "orphan-g2"
        for action in actions
    )


def test_authority_path_loader_reads_explicit_path_only(tmp_path: Path) -> None:
    """R3.4-004: the authority loader reads the explicit path and reports
    a missing authority at that path as empty (not a data-root
    fallback)."""
    _legacy_orphan_fixture(tmp_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    authority = LegacyClassificationAuthority.load(
        config_dir / LEGACY_CLASSIFICATION_FILENAME
    )
    assert authority.entries() == ()


# ---------------------------------------------------------------------------
# R3.3-SCHEMA-001: intent_schema null.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "schema_value",
    [None, 123, "", "reconnect-seal-intent.v9"],
    ids=["null", "integer", "empty_string", "unknown_future"],
)
def test_present_but_invalid_intent_schema_fails_closed(
    tmp_path: Path, schema_value: object
) -> None:
    """R3.4-010 (R3.3-SCHEMA-001 RED): a PRESENT intent_schema key whose
    value is not exactly ``reconnect-seal-intent.v2`` (including an
    explicit null) fails closed; only a missing key is legacy."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="bad-schema-g1",
            reason="planned_rotation",
            connection_id="conn-bad",
            generation=0,
            started_at_utc_ns=5_000_000_000,
        )
        intent["intent_schema"] = schema_value
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    with pytest.raises(
        RecoveryConflictError, match="RECOVERY_SEAL_INTENT_UNSUPPORTED_SCHEMA"
    ):
        recover_storage(layout=layout, catalog=Catalog(layout.catalog))


def test_missing_intent_schema_remains_legacy_handling(tmp_path: Path) -> None:
    """R3.4-011: a MISSING intent_schema key remains unversioned legacy
    (conservative policy: ambiguous without positive proof), never
    versioned and never rejected."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        intent = _durable_intent(
            gap_id="legacy-missing-g1",
            reason="planned_rotation",
            connection_id="conn-legacy",
            generation=0,
            started_at_utc_ns=5_000_000_000,
        )
        assert "intent_schema" not in intent
        _seal_zero_record_marker(
            tmp_path, catalog, intent, stream=intent["stream"]
        )

    from binance_market_data_recorder.spool.legacy_reconnect import (
        evaluate_legacy_reconnect_decisions,
    )

    with Catalog(layout.catalog, read_only=True) as catalog:
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog,
            authority=LegacyClassificationAuthority.load(
                layout.root / LEGACY_CLASSIFICATION_FILENAME
            ),
        )
    decision = report.decisions[0]
    assert decision.candidate.intent_schema is None
    assert decision.automatic == "ambiguous"
    assert report.first_corrected_startup_eligible is False
