"""Durable, atomic runtime state consumed by the local CLI."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from ..storage.layout import fsync_directory

SERVICE_STATE_SCHEMA = "service-state.v1"
SERVICE_STATES = frozenset({"STARTING", "RUNNING", "STOPPING", "STOPPED", "FAILED"})


class ServiceStateError(ValueError):
    """A runtime state document violates the M14 contract."""


class ServiceStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def write(self, document: Mapping[str, object]) -> None:
        body = dict(document)
        body.setdefault("schema_version", SERVICE_STATE_SCHEMA)
        if body["schema_version"] != SERVICE_STATE_SCHEMA:
            raise ServiceStateError("unsupported service state schema")
        if body.get("status") not in SERVICE_STATES:
            raise ServiceStateError("invalid service status")
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        encoded = json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.partial")
        descriptor = -1
        temporary_owned = False
        published = False
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(temporary, flags, 0o600)
            temporary_owned = True
            try:
                os.fchmod(descriptor, 0o600)
                view = memoryview(encoded)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise OSError("service state write returned no progress")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                    descriptor = -1
            os.replace(temporary, self.path)
            published = True
            os.chmod(self.path, 0o600)
            fsync_directory(self.path.parent)
        finally:
            if temporary_owned and not published:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def read(self) -> dict[str, object] | None:
        if not self.path.is_file():
            return None
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ServiceStateError(f"cannot read service state: {type(exc).__name__}") from exc
        if not isinstance(document, dict):
            raise ServiceStateError("service state is not an object")
        if document.get("schema_version") != SERVICE_STATE_SCHEMA:
            raise ServiceStateError("unsupported service state schema")
        return document
