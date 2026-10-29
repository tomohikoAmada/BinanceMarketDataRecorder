"""Read one primary REST representation, including historical snapshot records."""

from __future__ import annotations

import base64
import json
from typing import Any


def response_document(provenance: dict[str, Any]) -> Any:
    response = provenance["response"]
    # v1/v2 snapshots stored a model (v2 sometimes also stored the HTTP body).
    if "model" in response:
        return response["model"]
    if "raw_body_base64" in response:
        return json.loads(
            base64.b64decode(response["raw_body_base64"], validate=True), parse_float=str
        )
    raise ValueError("REST record has no declared primary response")
