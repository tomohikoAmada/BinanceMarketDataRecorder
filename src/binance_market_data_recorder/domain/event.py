"""Executable EventEnvelope v1 contract."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

EnvelopeSchema = Literal["event-envelope.v1"]
Market = Literal[
    "spot", "um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery", "um_shared", "cm_shared"
]
NonEmptyText = Annotated[str, StringConstraints(min_length=1, max_length=256)]
SequenceValue = int | str


class EventEnvelope(BaseModel):
    """Metadata and exact payload bytes captured at a receive boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: EnvelopeSchema = "event-envelope.v1"
    venue: Literal["binance"] = "binance"
    market: Market
    symbol: NonEmptyText
    stream: NonEmptyText
    module: NonEmptyText
    connection_id: NonEmptyText
    collector_instance_id: NonEmptyText
    collector_version: NonEmptyText
    receive_time_utc_ns: int = Field(ge=0)
    receive_monotonic_ns: int = Field(ge=0)
    exchange_event_time: int | None = Field(default=None, ge=0)
    exchange_transaction_time: int | None = Field(default=None, ge=0)
    exchange_trade_time: int | None = Field(default=None, ge=0)
    source_sequence: dict[NonEmptyText, SequenceValue] = Field(default_factory=dict)
    payload_encoding: NonEmptyText = "utf-8-json"
    raw_payload: bytes
    capture_flags: tuple[NonEmptyText, ...] = ()

    def canonical_mapping(self) -> dict[str, object]:
        """Return the language-neutral mapping encoded in Raw frames."""

        return {
            "schema_version": self.schema_version,
            "venue": self.venue,
            "market": self.market,
            "symbol": self.symbol,
            "stream": self.stream,
            "module": self.module,
            "connection_id": self.connection_id,
            "collector_instance_id": self.collector_instance_id,
            "collector_version": self.collector_version,
            "receive_time_utc_ns": self.receive_time_utc_ns,
            "receive_monotonic_ns": self.receive_monotonic_ns,
            "exchange_event_time": self.exchange_event_time,
            "exchange_transaction_time": self.exchange_transaction_time,
            "exchange_trade_time": self.exchange_trade_time,
            "source_sequence": self.source_sequence,
            "payload_encoding": self.payload_encoding,
            "raw_payload": self.raw_payload,
            "capture_flags": self.capture_flags,
        }
