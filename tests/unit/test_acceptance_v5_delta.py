from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_delta import (
    ARCHIVE_STEPS,
    empty_continuation,
    replay_delta,
)


def archive() -> dict[str, Any]:
    events, transitions = [], []
    for index, (prefix, archive_state, chunk_state) in enumerate(ARCHIVE_STEPS[:4]):
        key = f"{prefix}:tx"
        events.append(
            {
                "event_id": 297 + index if index < 3 else 600,
                "transaction_id": "tx",
                "idempotency_key": key,
                "from_state": None if index == 0 else ARCHIVE_STEPS[index - 1][1],
                "to_state": archive_state,
                "occurred_at_utc_ns": index,
                "evidence_json": "{}",
            }
        )
        transitions.append(
            {
                "transition_id": 897 + index if index < 3 else 901,
                "chunk_id": "chunk",
                "idempotency_key": "archive-reserve:tx" if index == 0 else f"chunk:{key}",
                "from_state": "SEALED" if index == 0 else ARCHIVE_STEPS[index - 1][2],
                "to_state": chunk_state,
                "occurred_at_utc_ns": index,
                "evidence_json": "{}",
            }
        )
    return {
        "chunk": {"chunk_id": "chunk", "state": "LOCAL_DELETE_PENDING"},
        "transaction": {
            "transaction_id": "tx",
            "chunk_id": "chunk",
            "state": "LOCAL_DELETE_PENDING",
            "storage_id": "storage",
        },
        "events": events,
        "transitions": transitions,
        "target": {"storage_id": "storage"},
    }


def entry(family: str, bundle: dict[str, Any]) -> dict[str, Any]:
    row = bundle["transitions" if family == "chunk" else "events"][-1]
    return {
        "row": row,
        "companions": {"chunk": bundle["chunk"], "archive": bundle},
        "unit": None,
        "status": "acknowledged",
    }


def test_lookup_does_not_advance_companion_and_later_exact_replay() -> None:
    state = empty_continuation({"chunk": 900, "archive": 300, "operational": 0})
    state["archive_counts"]["VERIFIED"] = 1
    bundle = archive()
    high_water = {"chunk": 901, "archive": 600, "operational": 0}
    first = replay_delta(
        state,
        high_water,
        {
            "chunk": [entry("chunk", bundle)],
            "archive": [],
            "operational": [],
        },
    )
    assert first["processed"] == {"chunk": 901, "archive": 300, "operational": 0}
    assert [(ref["family"], ref["numeric_id"]) for ref in first["pending_causal_references"]] == [
        ("archive", 600),
    ]
    second = replay_delta(
        first,
        high_water,
        {
            "chunk": [],
            "archive": [entry("archive", bundle)],
            "operational": [],
        },
    )
    assert second["processed"]["archive"] == 600
    assert second["pending_causal_references"] == []


def test_later_changed_companion_is_blocker() -> None:
    state = empty_continuation({"chunk": 900, "archive": 300, "operational": 0})
    state["archive_counts"]["VERIFIED"] = 1
    bundle = archive()
    high_water = {"chunk": 901, "archive": 600, "operational": 0}
    first = replay_delta(
        state,
        high_water,
        {
            "chunk": [entry("chunk", bundle)],
            "archive": [],
            "operational": [],
        },
    )
    changed = copy.deepcopy(bundle)
    changed["events"][-1]["evidence_json"] = '{"tampered":true}'
    with pytest.raises(AcceptanceError, match="causal replay mismatch"):
        replay_delta(
            first,
            high_water,
            {
                "chunk": [],
                "archive": [entry("archive", changed)],
                "operational": [],
            },
        )


def test_missing_companion_cannot_skip_selected_row() -> None:
    state = empty_continuation({"chunk": 900, "archive": 300, "operational": 0})
    state["archive_counts"]["VERIFIED"] = 1
    bundle = archive()
    bundle["events"].pop()
    selected = entry("chunk", bundle)
    selected["status"] = "pending"
    pages = {"chunk": [selected], "archive": [], "operational": []}
    high_water = {"chunk": 901, "archive": 600, "operational": 0}
    assert replay_delta(state, high_water, pages)["processed"]["chunk"] == 900
    selected["status"] = "acknowledged"
    with pytest.raises(AcceptanceError, match="skipped/acknowledged"):
        replay_delta(state, high_water, pages)


def test_reference_bound_is_fail_closed() -> None:
    state = empty_continuation({"chunk": 900, "archive": 300, "operational": 0})
    state["pending_causal_references"] = [
        {
            "family": "operational",
            "numeric_id": ordinal,
        }
        for ordinal in range(1, 257)
    ]
    with pytest.raises(AcceptanceError, match="cap exceeded"):
        replay_delta(
            state,
            {"chunk": 901, "archive": 600, "operational": 300},
            {
                "chunk": [entry("chunk", archive())],
                "archive": [],
                "operational": [],
            },
        )


def test_budget_pending_preserves_cursor() -> None:
    state = empty_continuation({"chunk": 900, "archive": 300, "operational": 0})
    selected = entry("chunk", archive())
    selected["status"] = "budget_pending"
    result = replay_delta(
        state,
        {"chunk": 901, "archive": 600, "operational": 0},
        {
            "chunk": [selected],
            "archive": [],
            "operational": [],
        },
    )
    assert result["processed"] == state["processed"]
    assert result["pending_causal_references"] == []


def discontinuity_pair() -> list[dict[str, Any]]:
    pair = []
    for ordinal, kind in enumerate(("STARTED", "COMPLETED"), 1):
        identity = dict(market="spot", symbol="BTCUSDT", stream="diff_depth", gap_id="gap-a")
        body: dict[str, Any] = dict(identity)
        body.update(
            {"original_connection_id": "old", "original_generation": 0, "gap_started_at_utc_ns": 1}
            if kind == "STARTED"
            else {"new_connection_id": "new", "new_generation": 1, "gap_ended_at_utc_ns": 2}
        )
        pair.append(
            {
                **identity,
                "event_seq": ordinal,
                "event_id": kind,
                "event_type": f"STREAM_DISCONTINUITY_{kind}",
                "occurred_at_utc_ns": ordinal,
                "evidence_json": json.dumps(body),
            }
        )
    return pair


def test_operational_companion_lookup_replay_and_fixed_fanout() -> None:
    pair = discontinuity_pair()
    state = empty_continuation({"chunk": 0, "archive": 0, "operational": 0})
    high = {"chunk": 0, "archive": 0, "operational": 2}

    def selected(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "row": row,
            "companions": {"discontinuity": pair},
            "unit": None,
            "status": "acknowledged",
        }

    first = replay_delta(
        state, high, {"chunk": [], "archive": [], "operational": [selected(pair[0])]}
    )
    assert first["processed"]["operational"] == 1
    assert first["pending_causal_references"][0]["numeric_id"] == 2
    second = replay_delta(
        first, high, {"chunk": [], "archive": [], "operational": [selected(pair[1])]}
    )
    assert second["pending_causal_references"] == []
    assert second["open_discontinuities"] == {}
    pair.append(dict(pair[-1]))
    with pytest.raises(AcceptanceError):
        replay_delta(state, high, {"chunk": [], "archive": [], "operational": [selected(pair[0])]})


@pytest.mark.parametrize("fault", ["regression", "boolean", "duplicate", "malformed-pair"])
def test_delta_adversarial_input_is_rejected(fault: str) -> None:
    state = empty_continuation({"chunk": 0, "archive": 0, "operational": 0})
    high: dict[str, Any] = {"chunk": 0, "archive": 0, "operational": 2}
    pair = discontinuity_pair()
    pages: dict[str, Any] = {"chunk": [], "archive": [], "operational": []}
    if fault == "regression":
        state["observed_high_water"]["operational"] = 3
    elif fault == "boolean":
        high["operational"] = True
    else:
        if fault == "malformed-pair":
            pair[0]["evidence_json"] = "{}"
        entry = {
            "row": pair[0],
            "companions": {"discontinuity": pair},
            "unit": None,
            "status": "acknowledged",
        }
        pages["operational"] = [entry, entry] if fault == "duplicate" else [entry]
    with pytest.raises(AcceptanceError):
        replay_delta(state, high, pages)
