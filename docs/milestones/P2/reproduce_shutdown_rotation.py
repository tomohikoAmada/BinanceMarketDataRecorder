"""Offline P2-R5 reproduction of the shutdown test's clock-dependent assertion.

Run: PYTHONPATH=. .venv/bin/python docs/milestones/P2/reproduce_shutdown_rotation.py
Exit 1 means the original test rejects a legal spool-rotation layout.
Only temporary test roots are written; no network or production data access.
"""

from __future__ import annotations

import json
import math
import tempfile
import traceback
from pathlib import Path
from unittest.mock import patch

from binance_market_data_recorder.spool import writer
from tests.integration.test_spot_stream_collector import (
    depth,
    manifests,
    sealed_envelopes,
    test_server_shutdown_frame_is_persisted_before_reconnect,
)


def case(*, rotate_shutdown_chunk: bool) -> dict[str, object]:
    creations = 0

    def deadline(
        *,
        opened_monotonic: float,
        period_seconds: float,
        market: str,
        symbol: str,
        stream: str,
    ) -> float:
        nonlocal creations
        creations += 1
        # First writer is the startup marker; second contains serverShutdown.
        # Model opening that second writer immediately before its phase deadline.
        # All other deadlines are held away from this short controlled exercise.
        if rotate_shutdown_chunk and creations == 2:
            return math.nextafter(opened_monotonic, math.inf)
        return opened_monotonic + period_seconds

    with tempfile.TemporaryDirectory(prefix="recorder-p2-r5-") as temporary:
        root = Path(temporary)
        failure = None
        with patch.object(writer, "_rotation_deadline", deadline):
            try:
                test_server_shutdown_frame_is_persisted_before_reconnect(root)
            except AssertionError as error:
                frame = traceback.extract_tb(error.__traceback__)[-1]
                failure = {"line": frame.lineno, "assertion": frame.line}
        documents = manifests(root)
        events = sealed_envelopes(root)
        expected_payloads = [b'{"e":"serverShutdown","E":1770123456789}', depth(2, 2)]
        assert [event.raw_payload for event in events] == expected_payloads
        return {
            "case": "rotate_shutdown_chunk" if rotate_shutdown_chunk else "no_rotation",
            "original_test_passed": failure is None,
            "assertion_failure": failure,
            "payloads_equal": True,
            "raw_records": len(events),
            "manifests": [
                {
                    "records": document["record_count"],
                    "flags": document["capture_flags"],
                    "complete": document["complete"],
                    "gap": document["gap"],
                }
                for document in documents
            ],
        }


def main() -> int:
    results = [case(rotate_shutdown_chunk=False), case(rotate_shutdown_chunk=True)]
    print(json.dumps({"cases": results}, indent=2))
    return int(any(not result["original_test_passed"] for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
