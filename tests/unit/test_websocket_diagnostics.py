from __future__ import annotations

import json

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from binance_market_data_recorder.binance.websocket_common import (
    websocket_disconnect_log_fields,
)


@pytest.mark.parametrize(
    "received,sent,order,received_code,sent_code",
    [
        (Close(1011, "server restart"), Close(1011, "ack"), True, 1011, 1011),
        (Close(1008, "policy"), Close(1008, "client close"), False, 1008, 1008),
        (Close(1001, "going away"), None, None, 1001, None),
        (None, Close(1011, "client timeout"), None, None, 1011),
        (None, None, None, None, None),
    ],
)
def test_close_diagnostics_preserve_direction_and_unknown_frames(
    received: Close | None,
    sent: Close | None,
    order: bool | None,
    received_code: int | None,
    sent_code: int | None,
) -> None:
    fields = websocket_disconnect_log_fields(
        ConnectionClosedError(received, sent, order),
        connected_at=100.0,
        observed_at=105.25,
    )
    assert fields["close_received_code"] == received_code
    assert fields["close_sent_code"] == sent_code
    assert fields["close_received_then_sent"] is order
    assert fields["connection_phase"] == "established"
    assert fields["connected_duration_seconds_at_log"] == 5.25
    if received is None:
        assert fields["close_received_reason"] is None
    if sent is None:
        assert fields["close_sent_reason"] is None
    assert fields["os_errno"] is None


def test_transport_failure_keeps_os_errno_without_exception_message() -> None:
    fields = websocket_disconnect_log_fields(
        ConnectionResetError(104, "private http://proxy.example:8080 payload"),
        connected_at=None,
        observed_at=100.0,
    )
    assert fields["connection_phase"] == "opening"
    assert fields["connected_duration_seconds_at_log"] is None
    assert fields["os_errno"] == 104
    assert fields["close_received_code"] is None
    assert "proxy.example" not in json.dumps(fields)
    assert "payload" not in json.dumps(fields)


def test_unframed_close_keeps_immediate_os_cause_without_fabricating_close() -> None:
    error = ConnectionClosedError(None, None)
    error.__cause__ = ConnectionResetError(104, "private transport message")
    fields = websocket_disconnect_log_fields(
        error, connected_at=100.0, observed_at=101.0,
    )
    assert fields["os_errno"] == 104
    assert fields["cause_type"] == "ConnectionResetError"
    assert fields["close_received_code"] is None
    assert fields["close_sent_code"] is None
    assert "private transport message" not in json.dumps(fields)


def test_close_reason_is_bounded_and_does_not_echo_a_url() -> None:
    fields = websocket_disconnect_log_fields(
        ConnectionClosedError(Close(1011, "x" * 1000), Close(1011, "http://proxy.example"), True),
        connected_at=100.0,
        observed_at=101.0,
    )
    assert fields["close_received_reason"] == "x" * 128
    assert fields["close_sent_reason"] == "[redacted URL]"
    assert "proxy.example" not in json.dumps(fields)


def test_timeout_does_not_invent_protocol_or_os_details() -> None:
    fields = websocket_disconnect_log_fields(
        TimeoutError("not a close frame"), connected_at=None, observed_at=1.0,
    )
    assert fields["close_received_code"] is None
    assert fields["close_sent_code"] is None
    assert fields["close_received_then_sent"] is None
    assert fields["cause_type"] is None
    assert fields["os_errno"] is None
