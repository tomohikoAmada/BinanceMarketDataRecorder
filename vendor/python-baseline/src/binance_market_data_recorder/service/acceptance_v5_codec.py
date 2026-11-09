"""Lossless, document-local sharing of frozen archive transaction companions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .acceptance import AcceptanceError, canonical_json
from .acceptance_v5_delta import COMPACT_BATCH_POLICY, FAMILIES, delta_limits


def compact_entry(
    entry: dict[str, Any], bundles: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Return a wire entry and uncommitted table additions for byte admission."""
    companions = entry["companions"]
    bundle = companions.get("archive")
    if bundle is None:
        return entry, {}
    transaction_id = bundle["transaction"]["transaction_id"]
    if not isinstance(transaction_id, str) or not transaction_id:
        raise AcceptanceError("invalid shared archive transaction identity")
    if transaction_id in bundles and bundles[transaction_id] != bundle:
        raise AcceptanceError("conflicting shared archive transaction")
    return (
        {**entry, "companions": {**companions, "archive": transaction_id}},
        {} if transaction_id in bundles else {transaction_id: bundle},
    )


def delta_bytes(document: Mapping[str, Any]) -> int:
    total = sum(
        len(canonical_json(entry))
        for page in document["delta_pages"].values()
        for entry in page
    )
    if document.get("delta_policy") == COMPACT_BATCH_POLICY:
        total += len(canonical_json(document["archive_companions"]))
    return total


def expanded_pages(document: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Resolve exact same-document companions before ordinary causal replay.

    Sharing introduces no cursor authority or proof substitution. Expanded
    entries refer to the same immutable bundle object rather than copying it.
    """
    pages = document["delta_pages"]
    if document.get("delta_policy") != COMPACT_BATCH_POLICY:
        return dict(pages)
    bundles = document.get("archive_companions")
    if not isinstance(bundles, dict) or set(pages) != set(FAMILIES):
        raise AcceptanceError("invalid shared archive companion table")
    _page_cap, row_cap = delta_limits(document)
    for key, bundle in bundles.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(bundle, dict)
            or not isinstance(bundle.get("transaction"), dict)
            or bundle["transaction"].get("transaction_id") != key
        ):
            raise AcceptanceError("shared archive transaction identity differs")
    used: set[str] = set()
    expanded: dict[str, list[dict[str, Any]]] = {}
    for family in FAMILIES:
        page = pages[family]
        if not isinstance(page, list) or len(page) > row_cap:
            raise AcceptanceError("SQL page cap exceeded")
        expanded[family] = []
        for entry in page:
            if not isinstance(entry, dict) or not isinstance(entry.get("companions"), dict):
                raise AcceptanceError("invalid cursor row/companions")
            companions = entry["companions"]
            transaction_id = companions.get("archive")
            if transaction_id is None:
                expanded[family].append(entry)
                continue
            if not isinstance(transaction_id, str) or transaction_id not in bundles:
                raise AcceptanceError("missing shared archive transaction")
            used.add(transaction_id)
            expanded[family].append(
                {**entry, "companions": {**companions, "archive": bundles[transaction_id]}}
            )
    if used != set(bundles):
        raise AcceptanceError("unreferenced shared archive transaction")
    return expanded
