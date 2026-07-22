from __future__ import annotations

import pytest
from pydantic import ValidationError

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.format import decode_envelope, encode_envelope
from tests.factories import event


def test_event_envelope_preserves_exact_payload_and_round_trips() -> None:
    payload = b'{  "e": "depthUpdate", "escaped":"\\u0061" }'
    original = event(payload=payload)
    encoded = encode_envelope(original)
    decoded = decode_envelope(encoded)

    assert decoded == original
    assert decoded.raw_payload == payload
    assert encode_envelope(decoded) == encoded


def test_event_envelope_is_strict_and_rejects_unknown_fields() -> None:
    values = event().model_dump()
    values["api_key"] = "forbidden"
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(values)

    values = event().model_dump()
    values["receive_time_utc_ns"] = "1700000000"
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(values)
