"""One operating-system service process per internal data root."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path


class ServiceAlreadyRunning(RuntimeError):
    """Another process owns the internal-root service lock."""


class ServiceProcessLock:
    """Hold an advisory lock for the complete supervised process lifetime."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    @property
    def acquired(self) -> bool:
        return self._descriptor is not None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise RuntimeError("service lock is already acquired")
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ServiceAlreadyRunning(
                    f"another Recorder service owns {self.path}"
                ) from exc
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()}\n".encode())
            os.fsync(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def __enter__(self) -> ServiceProcessLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()
