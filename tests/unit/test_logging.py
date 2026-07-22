from __future__ import annotations

import io
import json
import logging

from binance_market_data_recorder.logging import configure_logging, log_event


def test_structured_log_is_one_json_object() -> None:
    stream = io.StringIO()
    logger = configure_logging(stream=stream)
    log_event(logger, logging.INFO, "doctor.started", "running checks", check_count=4)

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "doctor.started"
    assert payload["message"] == "running checks"
    assert payload["fields"] == {"check_count": 4}
    assert payload["logger"] == "binance_market_data_recorder"
