"""Offline P2-R5 regression check for both controlled shutdown/rotation layouts.

Run: PYTHONPATH=. .venv/bin/python docs/milestones/P2/reproduce_shutdown_rotation.py
Exit 1 means the current test rejects a legal spool-rotation layout.
The original failure remains in review-shutdown-rotation.json and commit 2b2b7b5.
Only temporary test roots are written; no network or production data access.
"""

from __future__ import annotations

import json
import tempfile
import traceback
from pathlib import Path

from tests.integration.test_spot_stream_collector import (
    depth,
    manifests,
    sealed_envelopes,
    test_server_shutdown_frame_is_persisted_before_reconnect,
)


def case(*, rotate_shutdown_chunk: bool) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="recorder-p2-r5-", dir="/var/tmp") as temporary:
        root = Path(temporary)
        failure = None
        try:
            test_server_shutdown_frame_is_persisted_before_reconnect(
                root, rotate_before_reconnect=rotate_shutdown_chunk
            )
        except AssertionError as error:
            frame = traceback.extract_tb(error.__traceback__)[-1]
            failure = {"line": frame.lineno, "assertion": frame.line}
        documents = manifests(root)
        events = sealed_envelopes(root)
        expected_payloads = [b'{"e":"serverShutdown","E":1770123456789}', depth(2, 2)]
        assert [event.raw_payload for event in events] == expected_payloads
        return {
            "case": "rotate_shutdown_chunk" if rotate_shutdown_chunk else "no_rotation",
            "test_passed": failure is None,
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
    return int(any(not result["test_passed"] for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
