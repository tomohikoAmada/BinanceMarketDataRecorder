from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder import cli
from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.acceptance import (
    AcceptanceError,
    sha256_bytes,
    verify_completed_stage,
)
from binance_market_data_recorder.service.acceptance_v5_finalize import (
    baseline,
    finalize,
    verify_audit,
    verify_completed_v5_stage,
)
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_unit
from binance_market_data_recorder.storage.acceptance_delta import DeltaSnapshot
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive
from tests.integration.test_acceptance_v5_finalize import raw_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.unit.test_deployment_identity import _identity
from tests.v5_support import production_readiness, publish_ready_state


@pytest.mark.parametrize("pending", [False, True])
def test_verified_archive_source_retirement_and_full_audit(tmp_path: Path, pending: bool) -> None:
    prepared = prepare_archive(tmp_path / "archive")
    with Catalog(prepared.layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        manager = ArchiveManager(layout=prepared.layout, catalog=catalog, target=prepared.target)
        if pending:

            def stop(point: str, _path: Path | None) -> None:
                if point == "before_local_delete":
                    raise RuntimeError("pause pending")

            manager.fault_hook = stop
            # ArchiveManager wraps fault-hook errors at its transaction boundary.
            with pytest.raises(RuntimeError):
                manager.run_once()
        else:
            manager.run_once()
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None
        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        assert transaction is not None
        assert transaction["state"] == ("LOCAL_DELETE_PENDING" if pending else "LOCAL_DELETED")
        (prepared.layout.root / str(row["sealed_path"])).unlink(missing_ok=True)
    identity = _identity(tmp_path / "identity")
    identity = replace(
        identity,
        systemd_effective={
            **identity.systemd_effective,
            "working_directory": str(prepared.layout.root),
        },
    )
    roots = {prepared.target.storage_id: prepared.target.root}
    path, _sha, audit = baseline(
        data_root=prepared.layout.root,
        evidence_root=tmp_path / "baseline",
        identity=identity,
        products=[["spot", "BTCUSDT"]],
        archive_roots=roots,
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    assert audit["result"] == "PASS_CANDIDATE"
    assert audit["archive_bytes_verified"] > 0
    assert audit["raw_bytes_verified"] == 0
    verify_audit(root=path.parent, identity=identity, archive_roots=roots, raw_unit=raw_unit)
    external_manifest = prepared.target.root / str(transaction["external_manifest_relative_path"])
    document = json.loads(external_manifest.read_bytes())
    document["transaction_id"] = "wrong-authority"
    external_manifest.write_text(json.dumps(document))
    with pytest.raises(AcceptanceError):
        verify_audit(root=path.parent, identity=identity, archive_roots=roots, raw_unit=raw_unit)


def test_post_snapshot_archive_retirement_is_not_mixed_with_old_rows(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        snapshot = DeltaSnapshot(catalog._connection)
        row = snapshot.exact("chunks", "chunk_id", prepared.chunk_ids[0])
        assert row is not None
        manifest_path = prepared.layout.root / row["manifest_path"]
        body = manifest_path.read_bytes()
        task: dict[str, Any] = {
            "data_root": str(prepared.layout.root),
            "manifest": json.loads(body),
            "manifest_sha256": sha256_bytes(body),
            "manifest_path": row["manifest_path"],
            "chunk": row,
            "archive": None,
        }
        ArchiveManager(layout=prepared.layout, catalog=catalog, target=prepared.target).run_once()
        with pytest.raises(AcceptanceError, match="unauthorized Raw absence"):
            qualify_unit(task)
        fresh = DeltaSnapshot(catalog._connection)
        tx = fresh.exact("archive_transactions", "chunk_id", prepared.chunk_ids[0])
        assert tx is not None
        task.update(
            chunk=fresh.exact("chunks", "chunk_id", prepared.chunk_ids[0]),
            archive=fresh.archive_lifecycle(tx["transaction_id"]),
            archive_root=str(prepared.target.root),
        )
        assert qualify_unit(task)["archive"] is not None


def test_completed_stage_survives_later_authorized_archive_retirement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = prepare_archive(tmp_path / "archive")
    observer, clock, evaluator = observer_fixture(tmp_path / "observer")
    evaluator.expected_products = frozenset({ProductKey("spot", "BTCUSDT")})
    identity = replace(
        observer.identity,
        systemd_effective={
            **observer.identity.systemd_effective,
            "working_directory": str(prepared.layout.root),
        },
    )
    roots = {prepared.target.storage_id: prepared.target.root}
    path, _sha, document = baseline(
        data_root=prepared.layout.root,
        evidence_root=tmp_path / "baseline",
        identity=identity,
        products=[["spot", "BTCUSDT"]],
        archive_roots=roots,
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
        boot_id="boot-a",
    )
    assert document["result"] == "PASS_CANDIDATE"
    observer = replace(
        observer, data_root=prepared.layout.root, identity=identity, predecessor_path=path
    )
    observer.evaluator = production_readiness(observer, clock)
    publish_ready_state(observer, clock)
    observer.start()
    for _ in range(23):
        advance(clock, 300)
        publish_ready_state(observer, clock)
        observer.sample()
    advance(clock, 300)
    publish_ready_state(observer, clock)
    observer.finalize()
    _path, final_sha, final = finalize(
        evidence_root=observer.evidence_root,
        identity=identity,
        archive_roots=roots,
        probe=stopped,
        identity_verifier=lambda _identity: None,
        raw_unit=raw_unit,
    )
    assert final["eligible_for_next_stage"] is True
    assert verify_completed_v5_stage(
        observer.evidence_root, identity, archive_roots=roots, raw_unit=raw_unit
    ) == (final, final_sha)
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None and row["state"] == "SEALED"
        local_path = prepared.layout.root / str(row["sealed_path"])
        assert local_path.is_file()
        ArchiveManager(layout=prepared.layout, catalog=catalog, target=prepared.target).run_once()
        retired = catalog.chunk(prepared.chunk_ids[0])
        assert retired is not None and retired["state"] == "LOCAL_DELETED"
        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        assert transaction is not None and transaction["state"] == "LOCAL_DELETED"
        archived_manifest = prepared.target.root / str(
            transaction["external_manifest_relative_path"]
        )
        assert archived_manifest.is_file()
    assert not local_path.exists()

    def no_live_raw(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("completed historical replay depended on a live Raw location")

    assert verify_completed_v5_stage(
        observer.evidence_root, identity, archive_roots={}, raw_unit=no_live_raw
    ) == (final, final_sha)
    monkeypatch.setattr(
        "binance_market_data_recorder.service.acceptance_v5_finalize.cancellable_unit", no_live_raw
    )
    assert verify_completed_stage(
        observer.evidence_root, identity, archive_root_resolver=no_live_raw
    ) == (final, final_sha)
    for name in list(os.environ):
        if name.startswith("BINANCE_MARKET_RECORDER_"):
            monkeypatch.delenv(name)
    config = tmp_path / "vps.toml"
    config.write_text(
        f'[recorder]\ncapacity_profile="vps-production-v1"\ndata_root="{prepared.layout.root}"\n'
        'spot_symbols=["BTCUSDT"]\nusdm_symbols=[]\n'
    )
    monkeypatch.setattr(cli, "load_deployment_identity", lambda _path: identity)
    monkeypatch.setattr(cli, "enforce_vps_paths", lambda _identity: None)
    monkeypatch.setattr(cli, "_acceptance_archive_root_resolver", no_live_raw)
    assert cli.main([
        "--config", str(config), "deployment", "acceptance", "verify",
        "--evidence-root", str(observer.evidence_root),
    ]) == 0
    objects = observer.evidence_root / "terminal-audit" / "manifest-objects"
    frozen = next(objects.glob("*.manifest"))
    frozen.write_bytes(b"corrupt historical control after retirement")
    with pytest.raises(AcceptanceError):
        verify_completed_stage(observer.evidence_root, identity)
