"""Combined/flat equivalence, valid fields and unchanged original Raw provenance."""

import hashlib
import json

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize.parser import parse_envelope
from normalize import parse_capture_envelope


def main():
    payload = {
        "e": "aggTrade",
        "E": 1000,
        "s": "ETHUSDT",
        "a": 12,
        "p": "2500.12345678",
        "q": "0.0123",
        "f": 20,
        "l": 21,
        "T": 999,
        "m": False,
        "M": True,
    }
    wire = json.dumps({"stream": "ethusdt@aggTrade", "data": payload}).encode()
    envelope = EventEnvelope(
        market="spot",
        symbol="ETHUSDT",
        stream="agg_trade",
        module="binance.public.websocket.v1",
        connection_id="fixture",
        collector_instance_id="fixture",
        collector_version="test",
        receive_time_utc_ns=1000000000,
        receive_monotonic_ns=2000000000,
        payload_encoding="utf-8-json-combined.v1",
        raw_payload=wire,
    )
    original_hash = hashlib.sha256(wire).hexdigest()
    parsed = parse_capture_envelope(envelope, payload_sha256=original_hash)
    flat = envelope.model_copy(
        update={
            "raw_payload": json.dumps(payload).encode(),
            "payload_encoding": "utf-8-json",
        }
    )
    assert parsed == parse_envelope(flat, payload_sha256=original_hash)
    assert parsed[0].valid and parsed[0].fields["price"] == "2500.12345678"
    assert envelope.raw_payload == wire
    assert hashlib.sha256(envelope.raw_payload).hexdigest() == original_hash
    malformed = envelope.model_copy(
        update={"raw_payload": b"{", "capture_flags": ("malformed",)}
    )
    assert not parse_capture_envelope(malformed)[0].valid
    print(
        "combined WS adapter validity, exact fields and original Raw provenance: PASS"
    )


if __name__ == "__main__":
    main()
