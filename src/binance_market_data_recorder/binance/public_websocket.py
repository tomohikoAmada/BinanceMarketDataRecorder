"""Combined-stream routing with exact payload retention and explicit logical identity."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any

from ..domain.event import EventEnvelope
from ..domain.product import Product
from .public_sources import Source


@dataclass(frozen=True, slots=True)
class StreamGroup:
    root: str
    route: str
    name: str
    sources: tuple[Source, ...]
    wire_names: tuple[str, ...] = field(init=False)
    by_wire: dict[str, Source] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        names = tuple(s.endpoint.rsplit("/", 1)[1] for s in self.sources)
        object.__setattr__(self, "wire_names", names)
        object.__setattr__(self, "by_wire", dict(zip(names, self.sources, strict=True)))


def stream_groups(sources: tuple[Source, ...], limit: int) -> tuple[StreamGroup, ...]:
    if not 1 <= limit <= 1024:
        raise ValueError("combined-stream limit must be between 1 and 1024")
    groups: dict[str, list[Source]] = {}
    for source in sources:
        if source.transport == "ws":
            groups.setdefault(source.endpoint.rsplit("/", 1)[0], []).append(source)
    result = []
    for endpoint, members in sorted(groups.items()):
        members.sort(key=lambda s: s.endpoint)
        route = ""
        root = endpoint
        if endpoint.endswith(("/market", "/public")):
            root, route = endpoint.rsplit("/", 1)
        for offset in range(0, len(members), limit):
            selected = tuple(members[offset : offset + limit])
            # One identity digest per immutable subscription group, consumed by
            # Raw/Catalog discontinuity ownership. Never a per-event digest.
            identity = sha256("\0".join(s.endpoint for s in selected).encode()).hexdigest()[:16]
            result.append(StreamGroup(root, route, f"ws_{route or 'market'}_{identity}", selected))
    return tuple(result)


def wire_document(envelope: EventEnvelope) -> Any:
    decoded = json.loads(envelope.raw_payload)
    if envelope.payload_encoding == "utf-8-json-combined.v1":
        return decoded["data"]
    return decoded


def logical_envelope(envelope: EventEnvelope) -> EventEnvelope:
    """Expose logical stream to existing sequence/read consumers; retain exact bytes."""
    stream = envelope.source_sequence.get("sourceStream")
    if not isinstance(stream, str):
        return envelope
    return envelope.model_copy(update={"stream": stream})


def envelope_for_group(
    group: StreamGroup,
    product: Product | None,
    *,
    raw_payload: bytes,
    connection_id: str,
    collector_instance_id: str,
    collector_version: str,
    receive_time_utc_ns: int,
    receive_monotonic_ns: int,
) -> EventEnvelope:
    source = group.sources[0]
    sequence: dict[str, int | str] = {}
    flags: tuple[str, ...] = ()
    event_time = transaction_time = trade_time = None
    try:
        combined = json.loads(raw_payload)
        wire = combined["stream"]
        if wire == "!serverShutdown":
            sequence["sourceStream"] = "server_shutdown"
        else:
            source = group.by_wire[wire]
            sequence.update(sourceStream=source.stream, sourceFamily=source.family, wireStream=wire)
        data = combined["data"]
        if isinstance(data, dict):
            if wire != "!serverShutdown" and any(k not in data for k in source.required_fields):
                raise ValueError("required WS field is missing")
            if source.stream == "index_price" and data.get("i") != source.symbol:
                raise ValueError("index pair does not match subscription")
            if product is not None:
                if "s" in data and data["s"] != product.key.symbol:
                    raise ValueError("payload symbol does not match directory")
                if "st" in data and data["st"] != product.symbol_type:
                    raise ValueError("symbol type does not match directory")
                if source.stream in {"diff_depth", "rpi_depth", "book_ticker", "agg_trade"}:
                    if data.get("s") != product.key.symbol:
                        raise ValueError("stream symbol does not match directory")
                    if product.venue_market != "spot" and data.get("st") != product.symbol_type:
                        raise ValueError("derivative stream lacks symbol type")
                    if "ps" in data and data["ps"] != product.pair:
                        raise ValueError("stream pair does not match directory")
                if (
                    source.stream == "liquidation"
                    and data.get("o", {}).get("s") != product.key.symbol
                ):
                    raise ValueError("liquidation symbol does not match directory")
            for key in ("E", "T"):
                value = data.get(key)
                if value is not None and (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or not 0 <= value <= 2**63 - 1
                ):
                    raise ValueError("invalid exchange timestamp")
            event_time = data.get("E")
            if source.stream in {"agg_trade", "trade", "block_trade"}:
                trade_time = data.get("T")
            elif source.stream in {"diff_depth", "rpi_depth", "book_ticker"}:
                transaction_time = data.get("T")
            for key in ("U", "u", "pu", "a", "f", "l", "t"):
                value = data.get(key)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    sequence[key] = value
            if source.stream in {"diff_depth", "rpi_depth"}:
                required = ("U", "u") if source.venue_market == "spot" else ("U", "u", "pu")
                if any(key not in sequence for key in required) or int(sequence["U"]) > int(
                    sequence["u"]
                ):
                    raise ValueError("invalid depth sequence")
                if not isinstance(data.get("b"), list) or not isinstance(data.get("a"), list):
                    raise ValueError("invalid depth levels")
        elif not isinstance(data, list):
            raise ValueError("invalid combined data")
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError):
        flags = ("malformed",)
        event_time = transaction_time = trade_time = None
    return EventEnvelope(
        market=source.market,
        symbol=source.symbol if product is None else product.key.symbol,
        stream=group.name,
        module="binance.public.websocket.v1",
        connection_id=connection_id,
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        exchange_event_time=event_time,
        exchange_transaction_time=transaction_time,
        exchange_trade_time=trade_time,
        source_sequence=sequence,
        raw_payload=raw_payload,
        payload_encoding="utf-8-json-combined.v1",
        capture_flags=flags,
    )


class DepthSequenceObserver:
    """Mark known native discontinuities before append; two IDs, no full book."""

    def __init__(self) -> None:
        self._last: dict[str, tuple[str, int]] = {}

    def observe(self, envelope: EventEnvelope) -> EventEnvelope:
        stream = envelope.source_sequence.get("sourceStream")
        if stream not in {"diff_depth", "rpi_depth"}:
            return envelope
        assert isinstance(stream, str)
        if "malformed" in envelope.capture_flags:
            self._last.pop(stream, None)
            return envelope
        first, final, previous = (envelope.source_sequence.get(k) for k in ("U", "u", "pu"))
        if not isinstance(first, int) or not isinstance(final, int):
            return envelope
        old = self._last.get(stream)
        if old is None or old[0] != envelope.connection_id:
            self._last[stream] = envelope.connection_id, final
            return envelope
        if final <= old[1]:  # Preserve duplicate/stale bytes; do not rewind the cursor.
            return envelope
        self._last[stream] = envelope.connection_id, final
        gap = first > old[1] + 1 if envelope.market == "spot" else previous != old[1]
        if gap:
            return envelope.model_copy(
                update={
                    "capture_flags": tuple(dict.fromkeys((*envelope.capture_flags, "sequence_gap")))
                }
            )
        return envelope
