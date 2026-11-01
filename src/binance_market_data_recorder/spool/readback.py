"""One independent compressed read: stored identity plus expanded identity/frames."""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import BinaryIO, cast

import zstandard

from .format import ScanResult, scan_chunk_stream


class DigestReader:
    """Hash only bytes actually read from one owned descriptor; never cache paths."""

    def __init__(
        self,
        source: BinaryIO,
        *,
        consume: Callable[[int], None] | None = None,
        maximum_bytes: int | None = None,
    ) -> None:
        self.source = source
        self.consume = consume
        self.maximum_bytes = maximum_bytes
        self.digest = hashlib.sha256()
        self.byte_count = 0

    def read(self, size: int = -1) -> bytes:
        block = self.source.read(size)
        self.digest.update(block)
        self.byte_count += len(block)
        if self.maximum_bytes is not None and self.byte_count > self.maximum_bytes:
            raise ValueError("artifact exceeds its declared byte bound")
        if self.consume is not None:
            self.consume(len(block))
        return block


class ReadBudget:
    def __init__(self, bytes_per_second: int) -> None:
        self.rate = bytes_per_second
        self.started = time.monotonic()
        self.consumed = 0

    def consume(self, size: int) -> None:
        self.consumed += size
        delay = self.consumed / self.rate - (time.monotonic() - self.started)
        if delay > 0.01:
            time.sleep(delay)


class ExpandedReader:
    def __init__(
        self, source: BinaryIO, maximum: int | None, consume: Callable[[int], None] | None
    ) -> None:
        self.source, self.maximum, self.consume = source, maximum, consume
        self.count = 0

    def read(self, size: int = -1) -> bytes:
        block = self.source.read(size)
        self.count += len(block)
        if self.maximum is not None and self.count > self.maximum:
            raise ValueError("expanded artifact exceeds its declared byte bound")
        if self.consume is not None:
            self.consume(len(block))
        return block


@dataclass(frozen=True)
class ArtifactReadback:
    stored_bytes: int
    stored_sha256: str
    uncompressed_bytes: int
    uncompressed_sha256: str
    scan: ScanResult | None = None
    file_signature: tuple[int, ...] | None = None


def file_signature(stat: os.stat_result) -> tuple[int, ...]:
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def read_artifact(
    path: Path,
    *,
    scan_frames: bool = False,
    bytes_per_second: int | None = None,
    max_stored_bytes: int | None = None,
    max_uncompressed_bytes: int | None = None,
) -> ArtifactReadback:
    """Read to EOF after fsync at the caller's trust boundary, in bounded buffers."""

    budget = ReadBudget(bytes_per_second) if bytes_per_second else None
    consume = budget.consume if budget else None
    with path.open("rb", buffering=0) as source:
        before = file_signature(os.fstat(source.fileno()))
        stored = DigestReader(source, consume=consume, maximum_bytes=max_stored_bytes)
        with zstandard.ZstdDecompressor().stream_reader(
            cast(BinaryIO, stored), closefd=False
        ) as decoded:
            reader = ExpandedReader(cast(BinaryIO, decoded), max_uncompressed_bytes, consume)
            scan = None
            if scan_frames:
                scan = scan_chunk_stream(cast(BinaryIO, reader), path=path, file_size=0)
                if not scan.is_clean or scan.uncompressed_sha256 is None:
                    raise ValueError(f"Raw verification failed: {scan.issue}: {scan.detail}")
                expanded_size, expanded_hash = scan.valid_end, scan.uncompressed_sha256
                scan = replace(scan, file_size=expanded_size)
            else:
                digest = hashlib.sha256()
                expanded_size = 0
                while block := reader.read(1024 * 1024):
                    digest.update(block)
                    expanded_size += len(block)
                expanded_hash = digest.hexdigest()
        # The decompressor must consume the complete source, including frame trailers.
        if stored.read(1):
            raise ValueError("compressed artifact has unread trailing bytes")
        if before != file_signature(os.fstat(source.fileno())) or before != file_signature(
            path.stat()
        ):
            raise ValueError("artifact mutated or was replaced during readback")
        return ArtifactReadback(
            stored.byte_count, stored.digest.hexdigest(), expanded_size, expanded_hash, scan, before
        )
