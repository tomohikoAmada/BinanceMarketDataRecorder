"""Producer-only causal backpressure over already verified, bounded SQL pages."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .acceptance_v5_delta import FAMILIES, _companions, cursor_id


def causal_prefixes(
    prior: Mapping[str, Any],
    high_water: Mapping[str, int],
    pages: dict[str, list[dict[str, Any]]],
    *,
    reference_cap: int,
) -> dict[str, list[dict[str, Any]]]:
    """Advance the least expensive next row, never skipping a family prefix.

    Equal row counts do not imply equal causal progress: a chunk lifecycle has
    more transitions than its archive lifecycle. Use only reference identities
    for scheduling; ordinary independent replay still validates every exact row,
    companion, Raw proof and continuation digest. No cursor is published here.
    """
    references = {
        family: [
            [(other, cursor_id(other, row))
             for other, row in _companions(family, entry["row"], entry["companions"], high_water)]
            if entry["status"] == "acknowledged" else []
            for entry in pages[family]
        ]
        for family in FAMILIES
    }

    def walk(families: tuple[str, ...]) -> dict[str, int]:
        processed = dict(prior["processed"])
        pending = {(ref["family"], ref["numeric_id"])
                   for ref in prior["pending_causal_references"]}
        positions = dict.fromkeys(FAMILIES, 0)
        checkpoint = dict(positions)
        while True:
            choices: list[tuple[int, str, int, set[tuple[str, int]]]] = []
            for family in families:
                index = positions[family]
                if index == len(pages[family]):
                    continue
                entry = pages[family][index]
                if entry["status"] != "acknowledged":
                    continue
                numeric_id = cursor_id(family, entry["row"])
                remaining = pending - {(family, numeric_id)}
                remaining.update(
                    (other, other_id) for other, other_id in references[family][index]
                    if other_id > (numeric_id if other == family else processed[other])
                )
                choices.append((len(remaining), family, numeric_id, remaining))
            if not choices:
                return checkpoint
            # Stable family order breaks ties; lookups never acknowledge rows.
            _, family, numeric_id, pending = min(choices, key=lambda item: item[0])
            processed[family] = numeric_id
            positions[family] += 1
            # v2 bounds final observations. A mutually referencing lifecycle may
            # temporarily exceed the cap in this fixed-page/fixed-fanout walk.
            if len(pending) <= reference_cap:
                checkpoint = dict(positions)

    admitted_positions = walk(FAMILIES)
    if not any(admitted_positions.values()):
        # Operational pairs never reference chunk/archive rows. An unclosed
        # operational tail must not prevent a complete archive lifecycle from
        # advancing at a full cap (or vice versa). These are the two existing
        # causal groups, not a general graph/search framework.
        admitted_positions = max(
            (walk(("operational",)), walk(("chunk", "archive"))),
            key=lambda positions: sum(positions.values()),
        )
    admitted = {}
    for family in FAMILIES:
        count = admitted_positions[family]
        # Preserve the first real dependency/time-pending descriptor if reached.
        if count < len(pages[family]) and pages[family][count]["status"] != "acknowledged":
            count += 1
        admitted[family] = pages[family][:count]
    return admitted
