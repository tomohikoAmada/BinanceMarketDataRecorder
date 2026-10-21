from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError, canonical_json
from binance_market_data_recorder.service.acceptance_v5_corpus import catalog_authority
from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit
from binance_market_data_recorder.service.acceptance_v5_io import (
    V5_SCHEMA_VERSION,
    hash_file,
    publish,
)
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_qualification_scoped_corpus import cross_boundary_fixture, fixture
from tools import qualification_scoped_corpus
from tools import qualification_selected_resume as candidate
from tools.qualification_scoped_corpus import freeze_selected_corpus


def setup(tmp: Path, *, legacy_cross_boundary: bool = False) -> tuple[Any, ...]:
    old: dict[str, Any]
    new: dict[str, Any]
    if legacy_cross_boundary:
        layout, boundary = cross_boundary_fixture(tmp)
        old = new = {}
    else:
        layout, old, new, boundary = fixture(tmp)
    identity = _identity(tmp)
    source = tmp / "source"
    child = tmp / "child"
    target = "b" * 64
    start = "a" * 64
    run = "existing-owner-run"
    boot = "boot-a"
    _, anchor = publish(
        source,
        "baseline-preflight.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "owner-selected-preflight",
            "run_id": run,
            "configured_products": [["um_perpetual", "BTCUSDT"]],
            "deployment_identity": identity.document(),
            "owner_selected_scope": boundary,
            "owner_target_sha256": target,
            "canonical_v5_final_eligible": False,
        },
    )
    original_select = qualification_scoped_corpus.select_catalog

    def legacy_select(*args: Any, **kwargs: Any) -> Any:
        receipt = original_select(*args, **kwargs)
        with sqlite3.connect(source / "catalog.sqlite") as db:
            db.execute("DELETE FROM operational_event_sequence WHERE event_id='old-gap'")
            db.execute("DELETE FROM operational_events WHERE event_id='old-gap'")
            db.commit()
            db.row_factory = sqlite3.Row
            receipt["catalog_authority"] = catalog_authority(db)
        receipt["selected_catalog_sha256"] = hash_file(source / "catalog.sqlite")[1]
        return receipt

    context = (
        patch.object(qualification_scoped_corpus, "select_catalog", legacy_select)
        if legacy_cross_boundary
        else nullcontext()
    )
    with context:
        _, digest, corpus = freeze_selected_corpus(
            data_root=layout.root,
            root=source,
            identity=identity,
            boundary=boundary,
            anchor_sha256=anchor,
            boot_id=boot,
            probe=stopped,
        )
    receipt = hashlib.sha256((source / "owner-scope-receipt.json").read_bytes()).hexdigest()
    kwargs = dict(
        source=source,
        root=child,
        identity=identity,
        expected={"preflight": anchor, "receipt": receipt, "corpus": digest},
        start_sha256=start,
        target_sha256=target,
        run_id=run,
        boot_id=boot,
    )
    before = {
        str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in source.rglob("*")
        if p.is_file()
    }
    cert = stopped(layout.root)
    cert["units"]["binance-market-data-archive.timer"]["InvocationID"] = "current-fresh-timer"
    return layout, old, new, source, child, corpus, before, cert, kwargs


def test_causal_repair_preserves_original_sequence_schema_selection_and_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _layout, _, _, source, child, corpus, before, cert, kwargs = setup(
        tmp_path, legacy_cross_boundary=True
    )
    original = source / "source-catalog.sqlite"
    original_sha = hash_file(original)[1]
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    kwargs["operational_source"] = (original, original_sha)
    continued, _ = candidate.prepare_selected_continuation(**kwargs)
    assert (
        continued["owner_selected_scope"]["selected_counts"]
        == corpus["owner_selected_scope"]["selected_counts"]
    )
    with (
        sqlite3.connect(source / "catalog.sqlite") as old,
        sqlite3.connect(child / "catalog.sqlite") as repaired,
    ):
        assert (
            old.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
            == repaired.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
        )
        assert (
            old.execute("SELECT * FROM sqlite_sequence ORDER BY name").fetchall()
            == repaired.execute("SELECT * FROM sqlite_sequence ORDER BY name").fetchall()
        )
        assert repaired.execute(
            "SELECT * FROM operational_event_sequence ORDER BY event_seq"
        ).fetchall() == [(2, "old-gap"), (3, "new-gap")]
    opened = []

    def raw(task: dict[str, Any], **_: Any) -> Any:
        opened.append(task["manifest"]["chunk_id"])
        return qualify_unit(task)

    monkeypatch.setattr(
        candidate, "run_audit", lambda **kw: run_audit(**kw, probe=lambda _: cert, raw_unit=raw)
    )
    monkeypatch.setattr(candidate, "verify_audit", lambda **kw: verify_audit(**kw))
    result = candidate.run_selected_continuation(
        root=child, identity=kwargs["identity"], archive_roots={}
    )
    assert result["selected_counts"]["frames"] == 1 and len(opened) == 2
    assert hash_file(original)[1] == original_sha
    assert before == {
        str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in source.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("hardlink", [False, True])
def test_causal_repair_cannot_modify_its_source(tmp_path: Path, hardlink: bool) -> None:
    *_, kwargs = setup(tmp_path)
    original = kwargs["source"] / "source-catalog.sqlite"
    target = tmp_path / "alias.sqlite" if hardlink else original
    if hardlink:
        os.link(original, target)
    with pytest.raises(AcceptanceError, match="private"):
        candidate.repair_discontinuity_context(
            target, original, source_sha256=hash_file(original)[1], boundary=0
        )


def test_actual_prepare_and_run_preserve_selection_and_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _layout, _old, new, source, child, corpus, before, cert, kwargs = setup(tmp_path)
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    continued, _ = candidate.prepare_selected_continuation(**kwargs)
    assert (
        continued["catalog_sha256"] == corpus["catalog_sha256"]
        and continued["manifest_freeze"] == corpus["manifest_freeze"]
        and continued["owner_selected_scope"] == corpus["owner_selected_scope"]
    )
    assert not (child / "source-catalog.sqlite").exists()
    opened = []

    def actual_raw(task: dict[str, Any], **_: Any) -> dict[str, Any]:
        assert task["manifest"]["chunk_id"] == new["chunk_id"]
        opened.append(task["manifest"]["chunk_id"])
        return qualify_unit(task)

    def controlled(**kw: Any) -> Any:
        return run_audit(**kw, probe=lambda _: cert, raw_unit=actual_raw)

    def completed(**kw: Any) -> Any:
        def forbidden(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("third Raw pass")

        return verify_audit(**kw, raw_unit=forbidden)

    monkeypatch.setattr(candidate, "run_audit", controlled)
    monkeypatch.setattr(candidate, "verify_audit", completed)
    result = candidate.run_selected_continuation(
        root=child, identity=kwargs["identity"], archive_roots={}
    )
    assert (
        result["selected_counts"]["frames"] == 2
        and result["live_passes"] == 2
        and opened == [new["chunk_id"]] * 2
    )
    assert (
        result["continuation"]["new_capture"] is False
        and result["canonical_v5_final_eligible"] is False
    )
    assert before == {
        str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in source.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("key", ["preflight", "receipt", "corpus"])
def test_actual_prepare_rejects_expected_source_binding_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    *_, cert, kwargs = setup(tmp_path)
    kwargs["expected"][key] = "f" * 64
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    with pytest.raises(AcceptanceError, match="source binding"):
        candidate.prepare_selected_continuation(**kwargs)
    assert not kwargs["root"].exists()


@pytest.mark.parametrize("key", ["run_id", "target_sha256", "boot_id"])
def test_actual_prepare_rejects_wrong_owner_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    *_, cert, kwargs = setup(tmp_path)
    kwargs[key] = "wrong"
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    with pytest.raises(AcceptanceError, match="owner run"):
        candidate.prepare_selected_continuation(**kwargs)
    assert not kwargs["root"].exists()


def test_actual_prepare_rejects_existing_child(tmp_path: Path) -> None:
    *_, kwargs = setup(tmp_path)
    kwargs["root"].mkdir()
    with pytest.raises(AcceptanceError, match="exclusive"):
        candidate.prepare_selected_continuation(**kwargs)


def test_actual_prepare_rejects_foreign_mapping_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    *_, cert, kwargs = setup(tmp_path)
    (kwargs["source"] / "manifest-freeze/sidecar.json").write_text("{}\n")
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    with pytest.raises(AcceptanceError):
        candidate.prepare_selected_continuation(**kwargs)


@pytest.mark.parametrize("name", ["owner-scope-receipt.json", "baseline-preflight.json"])
@pytest.mark.parametrize("causal_repair", [False, True])
def test_actual_prepare_rejects_anchor_control_mutation_before_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, causal_repair: bool
) -> None:
    *_, cert, kwargs = setup(tmp_path)
    if causal_repair:
        original = kwargs["source"] / "source-catalog.sqlite"
        kwargs["operational_source"] = (original, hash_file(original)[1])
    monkeypatch.setattr(candidate, "quiescence_probe", lambda _: cert)
    original_hash = hash_file
    changed = False

    def alter(path: Path) -> tuple[int, str]:
        nonlocal changed
        if path == kwargs["source"] / "catalog.sqlite" and not changed:
            changed = True
            control = kwargs["source"] / name
            doc = json.loads(control.read_text())
            doc["unexpected_mutation"] = "must reject"
            control.write_bytes(canonical_json(doc))
        return original_hash(path)

    monkeypatch.setattr(candidate, "hash_file", alter)
    with pytest.raises(AcceptanceError):
        candidate.prepare_selected_continuation(**kwargs)
