from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError, canonical_json
from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit
from binance_market_data_recorder.service.acceptance_v5_io import (
    V5_SCHEMA_VERSION,
    hash_file,
    publish,
)
from binance_market_data_recorder.service.acceptance_v5_raw import qualify_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_qualification_scoped_corpus import fixture
from tools import qualification_selected_resume as candidate
from tools.qualification_scoped_corpus import freeze_selected_corpus


def setup(tmp: Path) -> tuple[Any, ...]:
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
def test_actual_prepare_rejects_anchor_control_mutation_before_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    *_, cert, kwargs = setup(tmp_path)
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
