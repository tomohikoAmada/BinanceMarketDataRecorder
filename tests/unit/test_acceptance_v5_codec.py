from __future__ import annotations

import copy
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError, canonical_json
from binance_market_data_recorder.service.acceptance_v5_codec import (
    compact_entry,
    delta_bytes,
    expanded_pages,
)
from binance_market_data_recorder.service.acceptance_v5_delta import (
    COMPACT_BATCH_POLICY,
    FAMILIES,
    empty_continuation,
    reference,
    replay_delta,
)
from tests.unit.test_acceptance_v5_delta import archive


def lifecycle(index: int) -> dict[str, Any]:
    bundle = archive()
    tx, chunk = f"tx-{index}", f"chunk-{index}"
    bundle["transaction"].update(transaction_id=tx, chunk_id=chunk)
    bundle["chunk"]["chunk_id"] = chunk
    for family, key in (("events", "event_id"), ("transitions", "transition_id")):
        for offset, row in enumerate(bundle[family]):
            row[key] = index * 4 + offset + 1
            row["idempotency_key"] = row["idempotency_key"].replace(":tx", f":{tx}")
            if family == "events":
                row["transaction_id"] = tx
            else:
                row["chunk_id"] = chunk
    return bundle


def entries(bundle: dict[str, Any], family: str) -> list[dict[str, Any]]:
    return [
        {
            "row": row,
            "companions": {"archive": bundle, "chunk": bundle["chunk"]},
            "unit": None,
            "status": "acknowledged",
        }
        for row in bundle["transitions" if family == "chunk" else "events"]
    ]


def encoded_document(pages: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    bundles: dict[str, dict[str, Any]] = {}
    encoded: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
    for family in FAMILIES:
        for entry in pages[family]:
            wire, additions = compact_entry(entry, bundles)
            bundles.update(additions)
            encoded[family].append(wire)
    return {
        "delta_policy": COMPACT_BATCH_POLICY,
        "delta_pages": encoded,
        "archive_companions": bundles,
    }


def test_shared_transaction_roundtrip_retains_all_rows_without_duplicate_bundles() -> None:
    pages: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
    for index in range(200):
        bundle = lifecycle(index)
        for family in ("chunk", "archive"):
            pages[family].extend(entries(bundle, family))
    document = encoded_document(pages)
    assert len(document["archive_companions"]) == 200
    assert expanded_pages(document) == pages
    original_bytes = sum(len(canonical_json(e)) for p in pages.values() for e in p)
    assert delta_bytes(document) < original_bytes / 3
    for family in ("chunk", "archive"):
        assert len(document["delta_pages"][family]) == 800


@pytest.mark.parametrize("mutation", ["missing", "wrong_identity", "inline", "unused"])
def test_shared_table_cannot_hide_or_replace_causal_authority(mutation: str) -> None:
    bundle = lifecycle(0)
    document = encoded_document(
        {"operational": [], "chunk": entries(bundle, "chunk"), "archive": []}
    )
    if mutation == "missing":
        document["archive_companions"].clear()
    elif mutation == "wrong_identity":
        document["archive_companions"]["tx-0"]["transaction"]["transaction_id"] = "other"
    elif mutation == "inline":
        document["delta_pages"]["chunk"][0]["companions"]["archive"] = bundle
    else:
        document["archive_companions"]["tx-1"] = lifecycle(1)
    with pytest.raises(AcceptanceError):
        expanded_pages(document)


def test_conflicting_same_transaction_is_not_deduplicated() -> None:
    original = lifecycle(0)
    pool = {"tx-0": original}
    different = copy.deepcopy(original)
    different["events"][0]["evidence_json"] = '{"changed":true}'
    with pytest.raises(AcceptanceError, match="conflicting shared"):
        compact_entry(entries(different, "archive")[0], pool)


def burst() -> tuple[dict[str, Any], dict[str, int], dict[str, list[dict[str, Any]]]]:
    # Earlier chunk replay is 200 rows ahead of archive. One new finite chunk
    # batch references 880 more archive rows, followed by 1024 archive rows in
    # this very same observation. Transient references reach 1080; final is 56.
    prior = empty_continuation({"chunk": 200, "archive": 0, "operational": 0})
    pages: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
    for index in range(270):
        bundle = lifecycle(index)
        if index < 50:
            prior["pending_causal_references"].extend(
                reference(
                    "archive", row, transaction_id=f"tx-{index}", chunk_id=f"chunk-{index}",
                    source_family="chunk", source_id=index * 4 + 4,
                )
                for row in bundle["events"]
            )
        else:
            pages["chunk"].extend(entries(bundle, "chunk"))
        pages["archive"].extend(entries(bundle, "archive"))
    pages["archive"] = pages["archive"][:1024]
    return prior, {"chunk": 1080, "archive": 1080, "operational": 0}, pages


def test_same_observation_discharges_transient_causal_cap_without_widening_final_cap() -> None:
    prior, high_water, pages = burst()
    with pytest.raises(AcceptanceError, match="causal reference cap exceeded"):
        replay_delta(prior, high_water, pages, row_cap=1024)
    result = replay_delta(prior, high_water, pages, row_cap=1024, causal_cap_at_end=True)
    assert result["processed"] == {"chunk": 1080, "archive": 1024, "operational": 0}
    assert len(result["pending_causal_references"]) == 56
    remaining = []
    for index in range(256, 270):
        remaining.extend(entries(lifecycle(index), "archive"))
    final = replay_delta(
        result, high_water, {"chunk": [], "archive": remaining, "operational": []},
        row_cap=1024, causal_cap_at_end=True,
    )
    assert final["processed"] == high_water
    assert final["pending_causal_references"] == []
    assert prior["processed"]["chunk"] == 200  # Replay never mutates its input.
    pages["archive"] = []
    with pytest.raises(AcceptanceError, match="causal reference cap exceeded"):
        replay_delta(prior, high_water, pages, row_cap=1024, causal_cap_at_end=True)


def test_final_cap_deferral_still_checks_each_cross_cursor_row_digest() -> None:
    prior, high_water, pages = burst()
    changed = copy.deepcopy(pages)
    # This old row was bound by a previous chunk observation, not the new table.
    changed["archive"][0]["row"]["evidence_json"] = '{"forged":true}'
    with pytest.raises(AcceptanceError, match="causal replay mismatch"):
        replay_delta(prior, high_water, changed, row_cap=1024, causal_cap_at_end=True)
