"""Read one primary REST representation, including historical snapshot records."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import Any

_PROVENANCE_HEADERS = frozenset(
    {
        "content-type",
        "date",
        "retry-after",
        "x-mbx-used-weight",
        "x-mbx-used-weight-1m",
        "x-response-time",
    }
)


def safe_provenance_headers(headers: Mapping[str, object]) -> dict[str, str]:
    return {
        str(name).lower(): str(value)
        for name, value in headers.items()
        if str(name).lower() in _PROVENANCE_HEADERS
        or str(name).lower().startswith("x-mbx-used-weight-")
    }


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
