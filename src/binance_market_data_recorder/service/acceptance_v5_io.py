"""Bounded canonical evidence I/O and exact manifest corpus snapshots (ADR-0034)."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import tempfile
import unicodedata
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import uuid4

from ..spool.seal import parse_strict_manifest_bytes
from ..storage.layout import fsync_directory
from .acceptance import AcceptanceError, canonical_json, sha256_bytes
from .acceptance import _publish as _publish_common

V5_SCHEMA_VERSION = "m22.9-acceptance-evidence.v5"
MAX_RECORDS_PER_SHARD = 512
MAX_CANONICAL_BYTES_PER_SHARD = 1024 * 1024
MANIFEST_FREEZE_MAX_RECORDS_PER_SHARD = MAX_RECORDS_PER_SHARD
MANIFEST_FREEZE_MAX_CANONICAL_BYTES_PER_SHARD = MAX_CANONICAL_BYTES_PER_SHARD
TERMINAL_AUDIT_MAX_RECORDS_PER_SHARD = MAX_RECORDS_PER_SHARD
TERMINAL_AUDIT_MAX_CANONICAL_BYTES_PER_SHARD = MAX_CANONICAL_BYTES_PER_SHARD
BUFFER_BYTES = 1024 * 1024
FREEZE_DOMAIN = b"BMDR-V5-MANIFEST-FREEZE\0"
AUDIT_DOMAIN = b"BMDR-V5-AUDIT-SHARDS\0"


def publish(root: Path, filename: str, document: Mapping[str, object]) -> tuple[Path, str]:
    """V5 private publication: refuse symlinked evidence directories."""
    if root.resolve(strict=False) != root.absolute():
        raise AcceptanceError("private evidence directory symlink rejected")
    return _publish_common(root, filename, document)


def canonical_path(value: object) -> str:
    if not isinstance(value, str) or not value or unicodedata.normalize("NFC", value) != value:
        raise AcceptanceError("noncanonical relative path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {".", ".."} for part in path.parts)
        or "\\" in value
        or "\0" in value
        or len(path.parts) > 64
    ):
        raise AcceptanceError("unsafe relative path")
    return value


@contextmanager
def open_exact(root: Path, relative: str) -> Iterator[BinaryIO]:
    """Open every path component with NOFOLLOW; leaf must be a regular file."""
    if root.resolve(strict=False) != root.absolute():
        raise AcceptanceError("authority directory symlink rejected")
    parts = PurePosixPath(canonical_path(relative)).parts
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = -1
    try:
        for part in parts[:-1]:
            following = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory
            )
            os.close(directory)
            directory = following
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise AcceptanceError("authority is not a regular file")
        with os.fdopen(descriptor, "rb", buffering=0) as source:
            descriptor = -1
            yield source
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory)


def hash_stream(source: BinaryIO, progress: Callable[[int], None] | None = None) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while block := source.read(BUFFER_BYTES):
        digest.update(block)
        size += len(block)
        if progress:
            progress(len(block))
    return size, digest.hexdigest()


def hash_file(path: Path, progress: Callable[[int], None] | None = None) -> tuple[int, str]:
    with open_exact(path.parent, path.name) as source:
        return hash_stream(source, progress)


def read_document(
    path: Path, *, limit: int = MAX_CANONICAL_BYTES_PER_SHARD
) -> tuple[dict[str, Any], str]:
    with open_exact(path.parent, path.name) as source:
        body = source.read(limit + 1)
    if len(body) > limit:
        raise AcceptanceError("evidence document exceeds its byte bound")
    try:
        value = json.loads(body)
    except (ValueError, UnicodeError) as exc:
        raise AcceptanceError("invalid evidence JSON") from exc
    if not isinstance(value, dict) or canonical_json(value) != body:
        raise AcceptanceError("noncanonical evidence JSON")
    if value.get("schema_version") != V5_SCHEMA_VERSION:
        raise AcceptanceError("V5 evidence schema mismatch")
    return value, sha256_bytes(body)


def chain_start(domain: bytes, anchor: str) -> str:
    if len(anchor) != 64:
        raise AcceptanceError("invalid shard anchor")
    return hashlib.sha256(domain + bytes.fromhex(anchor)).hexdigest()


def chain_next(prior: str, ordinal: int, digest: str) -> str:
    return hashlib.sha256(
        bytes.fromhex(prior) + ordinal.to_bytes(8, "big") + bytes.fromhex(digest)
    ).hexdigest()


class ShardWriter:
    def __init__(
        self,
        root: Path,
        *,
        domain: bytes,
        anchor: str,
        kind: str,
        on_publish: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
    ) -> None:
        self.root, self.anchor, self.kind = root, anchor, kind
        self.digest = chain_start(domain, anchor)
        self.ordinal, self.count = 0, 0
        self.previous: str | None = None
        self.records: list[dict[str, Any]] = []
        self.record_bytes = 0
        self.on_publish = on_publish

    def document(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": self.kind,
            "anchor": self.anchor,
            "ordinal": self.ordinal,
            "previous_shard_sha256": self.previous,
            "records": records,
        }

    def add(self, record: Mapping[str, Any]) -> bool:
        """Return whether a completed shard was published."""
        encoded_bytes = len(canonical_json(dict(record))) - 1
        overhead = len(canonical_json(self.document([])))
        if overhead + encoded_bytes > MAX_CANONICAL_BYTES_PER_SHARD:
            raise AcceptanceError("single audit record exceeds shard byte cap")
        flushed = False
        if (
            len(self.records) + 1 > MAX_RECORDS_PER_SHARD
            or overhead + self.record_bytes + encoded_bytes + len(self.records)
            > MAX_CANONICAL_BYTES_PER_SHARD
        ):
            self.flush()
            flushed = True
        self.records.append(dict(record))
        self.record_bytes += encoded_bytes
        return flushed

    def flush(self) -> None:
        if not self.records:
            return
        _path, digest = publish(
            self.root, f"shard-{self.ordinal:08d}.json", self.document(self.records)
        )
        self.digest = chain_next(self.digest, self.ordinal, digest)
        self.previous = digest
        self.ordinal += 1
        self.count += len(self.records)
        last_record = self.records[-1]
        self.records = []
        self.record_bytes = 0
        if self.on_publish:
            self.on_publish(self.summary(), last_record)

    def summary(self) -> dict[str, Any]:
        if self.records:
            raise AcceptanceError("shard writer has unpublished records")
        return self.published_summary()

    def published_summary(self) -> dict[str, Any]:
        return {"shard_count": self.ordinal, "record_count": self.count, "root_digest": self.digest}


def shard_records(
    root: Path, *, domain: bytes, anchor: str, summary: Mapping[str, Any], kind: str
) -> Iterator[dict[str, Any]]:
    digest = chain_start(domain, anchor)
    previous = None
    count = 0
    shard_count = summary.get("shard_count")
    if not isinstance(shard_count, int) or isinstance(shard_count, bool) or shard_count < 0:
        raise AcceptanceError("invalid shard count")
    seen = 0
    if root.exists():
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.name.startswith("shard-"):
                    if not entry.is_file(follow_symlinks=False) or not entry.name.endswith(".json"):
                        raise AcceptanceError("invalid shard directory member")
                    seen += 1
    if seen != shard_count:
        raise AcceptanceError("missing or extra audit shard")
    for ordinal in range(shard_count):
        doc, sha = read_document(root / f"shard-{ordinal:08d}.json")
        if set(doc) != {
            "schema_version",
            "evidence_kind",
            "anchor",
            "ordinal",
            "previous_shard_sha256",
            "records",
        } or (
            doc["evidence_kind"] != kind
            or doc["anchor"] != anchor
            or doc["ordinal"] != ordinal
            or doc["previous_shard_sha256"] != previous
        ):
            raise AcceptanceError("reordered or mismatched shard")
        records = doc["records"]
        if not isinstance(records, list) or not 1 <= len(records) <= MAX_RECORDS_PER_SHARD:
            raise AcceptanceError("invalid audit shard record cap")
        for record in records:
            if not isinstance(record, dict):
                raise AcceptanceError("invalid audit shard record")
            count += 1
            yield record
        digest = chain_next(digest, ordinal, sha)
        previous = sha
    if count != summary.get("record_count") or digest != summary.get("root_digest"):
        raise AcceptanceError("audit shard root digest/count mismatch")


@contextmanager
def path_inventory(data_root: Path, scratch: Path) -> Iterator[sqlite3.Connection]:
    """A disk sorted path set; neither scandir nor merge keeps N paths in RAM."""
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".membership-", dir=scratch) as directory:
        connection = sqlite3.connect(Path(directory) / "paths.sqlite")
        connection.execute("PRAGMA cache_size=-1024")
        connection.execute("CREATE TABLE paths(path TEXT PRIMARY KEY) WITHOUT ROWID")

        def visit(descriptor: int, prefix: str, depth: int) -> None:
            if depth > 64:
                raise AcceptanceError("manifest directory depth bound exceeded")
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    if entry.is_symlink():
                        raise AcceptanceError("manifest symlink rejected")
                    if entry.is_dir(follow_symlinks=False):
                        child = os.open(
                            entry.name,
                            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor,
                        )
                        try:
                            visit(child, f"{prefix}/{entry.name}", depth + 1)
                        finally:
                            os.close(child)
                    elif entry.is_file(follow_symlinks=False) and entry.name.endswith(
                        ".manifest.json"
                    ):
                        relative = canonical_path(f"{prefix}/{entry.name}")
                        connection.execute("INSERT INTO paths VALUES(?)", (relative,))
                    else:
                        raise AcceptanceError("unexpected manifest directory member")

        try:
            root_fd = os.open(data_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                data_fd = os.open(
                    "data",
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=root_fd,
                )
                try:
                    manifests_fd = os.open(
                        "manifests",
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=data_fd,
                    )
                    try:
                        visit(manifests_fd, "data/manifests", 0)
                    finally:
                        os.close(manifests_fd)
                finally:
                    os.close(data_fd)
            finally:
                os.close(root_fd)
            connection.commit()
            yield connection
        finally:
            connection.close()


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def snapshot_manifest(
    data_root: Path,
    relative: str,
    private_root: Path,
    *,
    between_reads: Callable[[], None] | None = None,
    progress: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    objects = private_root / "manifest-objects"
    if objects.resolve(strict=False) != objects.absolute():
        raise AcceptanceError("private manifest directory symlink rejected")
    objects.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = objects / f".{uuid4().hex}.partial"
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with open_exact(data_root, relative) as source, os.fdopen(descriptor, "wb") as target:
            before = _file_identity(os.fstat(source.fileno()))
            digest = hashlib.sha256()
            size = 0
            while block := source.read(BUFFER_BYTES):
                target.write(block)
                digest.update(block)
                size += len(block)
                if progress:
                    progress(len(block))
            target.flush()
            os.fsync(target.fileno())
            after = _file_identity(os.fstat(source.fileno()))
        if between_reads:
            between_reads()
        with open_exact(data_root, relative) as source:
            second_before = _file_identity(os.fstat(source.fileno()))
            second_size, second_digest = hash_stream(source, progress)
            second_after = _file_identity(os.fstat(source.fileno()))
        with open_exact(data_root, relative) as source:
            path_identity = _file_identity(os.fstat(source.fileno()))
        if not (before == after == second_before == second_after == path_identity) or (
            size,
            digest.hexdigest(),
        ) != (
            second_size,
            second_digest,
        ):
            raise AcceptanceError("manifest changed during corpus freeze")
        sha = digest.hexdigest()
        object_path = objects / f"{sha}.manifest"
        if object_path.exists():
            if hash_file(object_path) != (size, sha):
                raise AcceptanceError("existing private manifest snapshot changed")
        else:
            os.link(temporary, object_path)
            fsync_directory(objects)
        if hash_file(object_path) != (size, sha):
            raise AcceptanceError("private manifest readback failed")
        # Manifest controls are bounded by the same byte cap; Raw is streamed separately.
        with open_exact(objects, object_path.name) as source:
            body = source.read(MAX_CANONICAL_BYTES_PER_SHARD + 1)
        if len(body) > MAX_CANONICAL_BYTES_PER_SHARD:
            raise AcceptanceError("manifest control exceeds byte bound")
        manifest = parse_strict_manifest_bytes(body, path=object_path)
        return {
            "path": relative,
            "byte_length": size,
            "sha256": sha,
            "object": f"manifest-objects/{object_path.name}",
            "chunk_id": manifest["chunk_id"],
        }
    finally:
        temporary.unlink(missing_ok=True)


def freeze_manifests(
    data_root: Path,
    private_root: Path,
    catalog_sha: str,
    progress: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    writer = ShardWriter(
        private_root / "manifest-freeze",
        domain=FREEZE_DOMAIN,
        anchor=catalog_sha,
        kind="manifest-freeze-shard",
    )
    with path_inventory(data_root, private_root) as initial:
        initial.execute("CREATE TABLE identities(chunk_id TEXT PRIMARY KEY) WITHOUT ROWID")
        for (relative,) in initial.execute("SELECT path FROM paths ORDER BY path"):
            record = snapshot_manifest(data_root, relative, private_root, progress=progress)
            try:
                initial.execute("INSERT INTO identities VALUES(?)", (record["chunk_id"],))
            except sqlite3.IntegrityError as exc:
                raise AcceptanceError("duplicate manifest chunk identity") from exc
            writer.add(record)
        with path_inventory(data_root, private_root) as second:
            left = initial.execute("SELECT path FROM paths ORDER BY path")
            right = second.execute("SELECT path FROM paths ORDER BY path")
            while True:
                a, b = left.fetchone(), right.fetchone()
                if a != b:
                    raise AcceptanceError("manifest membership changed during corpus freeze")
                if a is None:
                    break
    writer.flush()
    return writer.summary()


def frozen_manifests(
    root: Path, corpus: Mapping[str, Any]
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    previous = ""
    for record in shard_records(
        root / "manifest-freeze",
        domain=FREEZE_DOMAIN,
        anchor=corpus["catalog_sha256"],
        summary=corpus["manifest_freeze"],
        kind="manifest-freeze-shard",
    ):
        if set(record) != {"path", "byte_length", "sha256", "object", "chunk_id"}:
            raise AcceptanceError("invalid manifest-freeze mapping")
        relative = canonical_path(record["path"])
        if not relative.startswith("data/manifests/") or not relative.endswith(".manifest.json"):
            raise AcceptanceError("manifest mapping is outside the manifest corpus")
        if relative <= previous:
            raise AcceptanceError("manifest freeze mapping order/identity violation")
        previous = relative
        object_relative = canonical_path(record["object"])
        if object_relative != f"manifest-objects/{record['sha256']}.manifest":
            raise AcceptanceError("private manifest object identity mismatch")
        with open_exact(root, object_relative) as source:
            body = source.read(MAX_CANONICAL_BYTES_PER_SHARD + 1)
        if len(body) > MAX_CANONICAL_BYTES_PER_SHARD or (len(body), sha256_bytes(body)) != (
            record["byte_length"],
            record["sha256"],
        ):
            raise AcceptanceError("private manifest snapshot corrupted")
        manifest = parse_strict_manifest_bytes(body, path=root / object_relative)
        if manifest["chunk_id"] != record["chunk_id"]:
            raise AcceptanceError("private manifest chunk identity mismatch")
        yield record, manifest
