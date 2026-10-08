from __future__ import annotations

from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_admission import causal_prefixes
from binance_market_data_recorder.service.acceptance_v5_delta import empty_continuation
from tests.unit.test_acceptance_v5_delta import archive, discontinuity_pair


def pair_pages() -> dict[str, list[dict[str, Any]]]:
    pair = discontinuity_pair()
    return {"chunk": [], "archive": [], "operational": [
        {"row": row, "companions": {"discontinuity": pair}, "unit": None,
         "status": "acknowledged"} for row in pair
    ]}


def test_full_cap_allows_a_temporary_reference_until_pair_closes() -> None:
    prior = empty_continuation({"operational": 0, "chunk": 0, "archive": 0})
    prior["pending_causal_references"] = [
        {"family": "chunk", "numeric_id": n} for n in range(1, 1025)
    ]
    pages = pair_pages()
    # The first row needs one extra reference; the second discharges it. Only
    # testing single-row admission would deadlock forever at this valid prefix.
    assert causal_prefixes(prior, {"operational": 2, "chunk": 1024, "archive": 0},
                           pages, reference_cap=1024) == pages


def test_unclosed_operational_pair_cannot_starve_closed_archive_cycle_at_full_cap() -> None:
    prior = empty_continuation({"operational": 0, "chunk": 0, "archive": 0})
    prior["pending_causal_references"] = [
        {"family": "chunk", "numeric_id": n} for n in range(10000, 11024)
    ]
    pages = pair_pages()
    pages["operational"] = pages["operational"][:1]
    bundle = archive()
    for family, key in (("chunk", "transitions"), ("archive", "events")):
        pages[family] = [{
            "row": row, "companions": {"chunk": bundle["chunk"], "archive": bundle},
            "unit": None, "status": "acknowledged",
        } for row in bundle[key]]
    admitted = causal_prefixes(prior, {"operational": 2, "chunk": 12000, "archive": 600},
                               pages, reference_cap=1024)
    assert admitted["operational"] == []
    assert admitted["chunk"] == pages["chunk"]
    assert admitted["archive"] == pages["archive"]


def test_no_within_cap_prefix_keeps_cursors_unacknowledged() -> None:
    prior = empty_continuation({"operational": 0, "chunk": 0, "archive": 0})
    pages = pair_pages()
    pages["operational"] = pages["operational"][:1]
    assert causal_prefixes(prior, {"operational": 2, "chunk": 0, "archive": 0},
                           pages, reference_cap=0) == {
        "chunk": [], "archive": [], "operational": [],
    }


@pytest.mark.parametrize("status", ["pending", "budget_pending"])
def test_reached_pending_descriptor_is_preserved_without_acknowledgement(status: str) -> None:
    prior = empty_continuation({"operational": 0, "chunk": 0, "archive": 0})
    pages = pair_pages()
    pages["operational"][-1]["status"] = status
    assert causal_prefixes(prior, {"operational": 2, "chunk": 0, "archive": 0},
                           pages, reference_cap=1024) == pages
    assert prior["processed"]["operational"] == 0


def test_malformed_companion_still_fails_closed() -> None:
    prior = empty_continuation({"operational": 0, "chunk": 0, "archive": 0})
    pages = pair_pages()
    pages["operational"][0]["companions"]["discontinuity"].append({})
    with pytest.raises(AcceptanceError):
        causal_prefixes(prior, {"operational": 2, "chunk": 0, "archive": 0},
                        pages, reference_cap=1024)
