"""官方 Binance 公开归档的不可变、版本感知导入器。

HistoricalImporter 是 Live 采集的离线兄弟(ADR-0024)。它从 data.binance.vision
(免凭证 HTTPS)下载 ZIP 文件,按相邻 .CHECKSUM 文件验证每个文件,以固定 50,000
行批次将 CSV 行流式写入 Parquet,并以源谱系信息发布 archive-clock Parquet。

关键不变量:
- 无接收时钟:historical 行携带 archive_event_time_utc_ns,clock_semantics=archive_source。
  它们不得进入 receive-time 重放。Live 和 Historical 从不会静默混合。
- 校验和修订:.CHECKSUM 变更会创建新修订版,带有 'supersedes' 谱系。
  不可变的 URL+SHA-256 身份被保留。
- 下载安全性:文件使用 .partial 后缀,通过 206 Content-Range 实现断点续传,
  Range 不支持时回退到全量下载(HTTP 200),显式处理 416 错误。无效/校验失败
  的 partial 在重试前被删除,防止损坏字节无限追加。
- 月/日排程:planner.py 将日期范围映射为整月文件和部分月的日文件。
  fundingRate 在官方归档布局中仅按月提供。
- 时间戳单位:2025-01-01 之后的 Spot 数据为微秒;更早的 Spot 和全部 USD-M
  为毫秒。规范化器输出 UTC 纳秒。
- 无历史 L2:归档不提供深度订单簿数据。
- 流式 CSV 规范化通过 ParquetWriter 写入固定批次 Arrow record group。
  不构建全量行列表。
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from ..storage.layout import fsync_directory
from ..version import package_version
from .planner import BackfillPlan, PlanEntry

CHECKSUM = re.compile(r"^(?P<digest>[0-9a-fA-F]{64})\s+\*?(?P<name>[^\s]+)\s*$")
CONTENT_RANGE = re.compile(
    r"^bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+|\*)$"
)
NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("archive_event_time_utc_ns", pa.int64(), nullable=False),
        pa.field("source_row_ordinal", pa.int64(), nullable=False),
        pa.field("source_values_json", pa.string(), nullable=False),
        pa.field("source_revision", pa.string(), nullable=False),
        pa.field("source_zip_sha256", pa.string(), nullable=False),
        pa.field("clock_semantics", pa.string(), nullable=False),
    ]
)


class HttpResponse(Protocol):
    status: int
    headers: object

    def read(self, amount: int = -1) -> bytes: ...

    def __enter__(self) -> HttpResponse: ...

    def __exit__(self, *args: object) -> None: ...


@dataclass(frozen=True, slots=True)
class ImportResult:
    status: str
    source_revision: str
    zip_path: str | None
    normalized_path: str | None
    gap: dict[str, object] | None

    def public_dict(self) -> dict[str, object]:
        return asdict(self)


class HistoricalImporter:
    def __init__(
        self,
        *,
        data_root: Path,
        opener: object = urllib.request.urlopen,
        chunk_bytes: int = 1024 * 1024,
        normalization_batch_rows: int = 50_000,
    ) -> None:
        if chunk_bytes <= 0 or normalization_batch_rows <= 0:
            raise ValueError("historical importer bounds must be positive")
        self.root = data_root.resolve() / "data" / "historical"
        self.sources = self.root / "sources"
        self.normalized = self.root / "normalized"
        self.gaps = self.root / "gaps"
        self.state = self.root / "state"
        for directory in (self.sources, self.normalized, self.gaps, self.state):
            directory.mkdir(parents=True, exist_ok=True)
        self.opener = opener
        self.chunk_bytes = chunk_bytes
        self.normalization_batch_rows = normalization_batch_rows

    @staticmethod
    def _validate_url(url: str) -> None:
        parsed = urlparse(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "data.binance.vision"
            or parsed.username
            or parsed.password
            or not parsed.path.startswith("/data/")
        ):
            raise ValueError("historical source must be official data.binance.vision")

    def _open(self, request: urllib.request.Request) -> HttpResponse:
        return self.opener(request, timeout=30)  # type: ignore[operator,no-any-return]

    def _download_bytes(self, url: str) -> bytes:
        self._validate_url(url)
        with self._open(
            urllib.request.Request(
                url, headers={"User-Agent": "BinanceMarketDataRecorder/0.1"}
            )
        ) as response:
            return response.read()

    def _download_zip(self, url: str, partial: Path) -> None:
        self._validate_url(url)
        offset = partial.stat().st_size if partial.exists() else 0
        headers = {"User-Agent": "BinanceMarketDataRecorder/0.1"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        request = urllib.request.Request(url, headers=headers)
        try:
            response_context = self._open(request)
        except urllib.error.HTTPError as exc:
            if exc.code == 416 and offset:
                return
            raise
        with response_context as response:
            if response.status == 206:
                content_range = self._response_header(response.headers, "Content-Range")
                match = CONTENT_RANGE.fullmatch(content_range or "")
                if (
                    match is None
                    or int(match["start"]) != offset
                    or int(match["end"]) < offset
                ):
                    partial.unlink(missing_ok=True)
                    raise ValueError(
                        "historical Range response has an invalid Content-Range"
                    )
                mode = "ab"
            elif response.status == 200:
                mode = "wb"
            else:
                raise RuntimeError(
                    f"historical archive returned unexpected HTTP {response.status}"
                )
            with partial.open(mode) as handle:
                while block := response.read(self.chunk_bytes):
                    handle.write(block)
                handle.flush()
                os.fsync(handle.fileno())

    @staticmethod
    def _response_header(headers: object, name: str) -> str | None:
        getter = getattr(headers, "get", None)
        if not callable(getter):
            return None
        value = getter(name)
        return str(value) if value is not None else None

    @staticmethod
    def _parse_checksum(body: bytes, expected_filename: str) -> tuple[str, str]:
        text = body.decode("ascii").strip()
        match = CHECKSUM.fullmatch(text)
        if match is None or Path(match["name"]).name != expected_filename:
            raise ValueError("official CHECKSUM is malformed or names another file")
        return match["digest"].lower(), text

    def run(self, plan: BackfillPlan) -> dict[str, object]:
        results = [self.import_entry(entry).public_dict() for entry in plan.entries]
        document = {
            "schema_version": "historical-import-run.v1",
            "clock_semantics": "archive_source_no_live_receive_clock",
            "plan": plan.public_dict(),
            "results": results,
            "completed_at_utc_ns": time.time_ns(),
        }
        self._atomic_json(self.state / "last_run.json", document)
        return document

    def import_entry(self, entry: PlanEntry) -> ImportResult:
        filename = Path(urlparse(entry.zip_url).path).name
        try:
            checksum_body = self._download_bytes(entry.checksum_url)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return self._record_gap(entry, "CHECKSUM_NOT_FOUND")
            raise
        expected_sha, checksum_text = self._parse_checksum(checksum_body, filename)
        identity = sha256(entry.zip_url.encode()).hexdigest()[:16]
        revision = f"{identity}-{expected_sha[:16]}"
        revision_dir = self.sources / revision
        zip_path = revision_dir / filename
        checksum_path = revision_dir / f"{filename}.CHECKSUM"
        manifest_path = revision_dir / "source_manifest.json"
        if manifest_path.is_file() and zip_path.is_file():
            self._verify_file(zip_path, expected_sha)
            normalized = self._normalize(entry, revision, zip_path, expected_sha)
            return ImportResult(
                "ALREADY_VERIFIED",
                revision,
                str(zip_path),
                str(normalized),
                None,
            )
        revision_dir.mkdir(parents=True, exist_ok=True)
        partial = revision_dir / f"{filename}.partial"
        try:
            self._download_zip(entry.zip_url, partial)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return self._record_gap(entry, "ZIP_NOT_FOUND")
            raise
        try:
            actual_sha = self._verify_file(partial, expected_sha)
        except ValueError:
            partial.unlink(missing_ok=True)
            raise
        os.replace(partial, zip_path)
        self._atomic_bytes(checksum_path, checksum_body)
        supersedes = self._previous_revisions(identity, revision)
        manifest = {
            "schema_version": "historical-source.v1",
            "source_revision": revision,
            "supersedes": supersedes,
            "official_url": entry.zip_url,
            "checksum_url": entry.checksum_url,
            "downloaded_at_utc_ns": time.time_ns(),
            "file_size": zip_path.stat().st_size,
            "official_checksum_text": checksum_text,
            "actual_sha256": actual_sha,
            "market": entry.market,
            "symbol": entry.symbol,
            "data_type": entry.data_type,
            "interval": entry.interval,
            "start_date": entry.start_date,
            "end_date": entry.end_date,
            "timestamp_unit": entry.timestamp_unit,
            "clock_semantics": "archive_source_no_live_receive_clock",
            "importer_version": package_version(),
        }
        self._atomic_json(manifest_path, manifest)
        normalized = self._normalize(entry, revision, zip_path, actual_sha)
        return ImportResult(
            "IMPORTED", revision, str(zip_path), str(normalized), None
        )

    def _previous_revisions(self, identity: str, current: str) -> list[str]:
        return sorted(
            path.name
            for path in self.sources.glob(f"{identity}-*")
            if path.is_dir() and path.name != current
        )

    def _record_gap(self, entry: PlanEntry, reason: str) -> ImportResult:
        gap = {
            "schema_version": "historical-gap.v1",
            "reason": reason,
            "entry": entry.public_dict(),
            "observed_at_utc_ns": time.time_ns(),
        }
        gap_id = sha256(
            f"{entry.zip_url}:{reason}".encode()
        ).hexdigest()
        self._atomic_json(self.gaps / f"{gap_id}.json", gap)
        return ImportResult("GAP", gap_id, None, None, gap)

    @staticmethod
    def _verify_file(path: Path, expected_sha: str) -> str:
        digest = sha256()
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                digest.update(block)
        actual = digest.hexdigest()
        if actual != expected_sha:
            raise ValueError(
                f"historical checksum mismatch: expected={expected_sha} actual={actual}"
            )
        return actual

    def _normalize(
        self, entry: PlanEntry, revision: str, zip_path: Path, source_sha: str
    ) -> Path:
        output = self.normalized / f"{revision}.parquet"
        if output.is_file():
            return output
        metadata = {
            b"source_revision": revision.encode(),
            b"source_zip_sha256": source_sha.encode(),
            b"clock_semantics": b"archive_source_no_live_receive_clock",
        }
        schema = NORMALIZED_SCHEMA.with_metadata(metadata)
        partial = output.with_suffix(".parquet.partial")
        row_count = 0
        try:
            with (
                pq.ParquetWriter(partial, schema, compression="zstd") as writer,
                zipfile.ZipFile(zip_path) as archive,
            ):
                members = [
                    name for name in archive.namelist() if name.endswith(".csv")
                ]
                if len(members) != 1:
                    raise ValueError(
                        "historical ZIP must contain exactly one CSV"
                    )
                with archive.open(members[0]) as raw:
                    reader = csv.reader(
                        io.TextIOWrapper(raw, encoding="utf-8", newline="")
                    )
                    batch: list[dict[str, object]] = []
                    for ordinal, values in enumerate(reader):
                        if not values or not values[0].lstrip("-").isdigit():
                            continue
                        batch.append(
                            self._normalized_row(
                                entry,
                                revision,
                                source_sha,
                                ordinal,
                                values,
                            )
                        )
                        if len(batch) == self.normalization_batch_rows:
                            writer.write_table(
                                pa.Table.from_pylist(batch, schema=schema)
                            )
                            row_count += len(batch)
                            batch.clear()
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        row_count += len(batch)
            with partial.open("rb") as handle:
                os.fsync(handle.fileno())
            self._verify_normalized(
                partial,
                revision=revision,
                source_sha=source_sha,
                expected_rows=row_count,
            )
            os.replace(partial, output)
            fsync_directory(output.parent)
            return output
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

    @staticmethod
    def _normalized_row(
        entry: PlanEntry,
        revision: str,
        source_sha: str,
        ordinal: int,
        values: list[str],
    ) -> dict[str, object]:
        timestamp = int(values[HistoricalImporter._timestamp_index(entry)])
        multiplier = 1_000 if entry.timestamp_unit == "microseconds" else 1_000_000
        return {
            "archive_event_time_utc_ns": timestamp * multiplier,
            "source_row_ordinal": ordinal,
            "source_values_json": json.dumps(values, separators=(",", ":")),
            "source_revision": revision,
            "source_zip_sha256": source_sha,
            "clock_semantics": "archive_source",
        }

    @staticmethod
    def _verify_normalized(
        path: Path,
        *,
        revision: str,
        source_sha: str,
        expected_rows: int | None = None,
    ) -> int:
        parquet = pq.ParquetFile(path)
        metadata = parquet.schema_arrow.metadata or {}
        if metadata.get(b"source_revision") != revision.encode():
            raise ValueError("normalized source_revision metadata mismatch")
        if metadata.get(b"source_zip_sha256") != source_sha.encode():
            raise ValueError("normalized source ZIP SHA-256 metadata mismatch")
        if metadata.get(b"clock_semantics") != (
            b"archive_source_no_live_receive_clock"
        ):
            raise ValueError("normalized clock semantics metadata mismatch")
        rows = int(parquet.metadata.num_rows)
        if expected_rows is not None and rows != expected_rows:
            raise ValueError(
                f"normalized row-count mismatch: expected={expected_rows} actual={rows}"
            )
        for batch in parquet.iter_batches(
            batch_size=50_000,
            columns=["source_row_ordinal", "archive_event_time_utc_ns"],
        ):
            if batch.num_rows > 50_000:
                raise ValueError("normalized readback batch exceeded its bound")
        return rows

    @staticmethod
    def _timestamp_index(entry: PlanEntry) -> int:
        if entry.data_type == "aggTrades":
            return 5
        if entry.data_type == "trades":
            return 4
        return 0

    def status(self) -> dict[str, object]:
        manifests = sorted(self.sources.glob("*/source_manifest.json"))
        gaps = sorted(self.gaps.glob("*.json"))
        return {
            "status": "OK",
            "source_revisions": len(manifests),
            "gaps": len(gaps),
            "last_run": (
                json.loads((self.state / "last_run.json").read_text())
                if (self.state / "last_run.json").is_file()
                else None
            ),
        }

    def verify(self) -> dict[str, object]:
        verified = 0
        normalized_rows = 0
        for manifest_path in self.sources.glob("*/source_manifest.json"):
            manifest = json.loads(manifest_path.read_text())
            revision = str(manifest.get("source_revision", ""))
            if (
                manifest.get("schema_version") != "historical-source.v1"
                or revision != manifest_path.parent.name
            ):
                raise ValueError("historical source manifest identity mismatch")
            filename = Path(urlparse(manifest["official_url"]).path).name
            source_sha = str(manifest.get("actual_sha256", ""))
            zip_path = manifest_path.parent / filename
            if not zip_path.is_file():
                raise ValueError("historical source ZIP is missing")
            if (
                manifest.get("checksum_url") != f"{manifest['official_url']}.CHECKSUM"
                or manifest.get("file_size") != zip_path.stat().st_size
            ):
                raise ValueError("historical source manifest does not match source ZIP")
            checksum_path = manifest_path.parent / f"{filename}.CHECKSUM"
            if not checksum_path.is_file():
                raise ValueError("historical source CHECKSUM is missing")
            checksum_sha, checksum_text = self._parse_checksum(
                checksum_path.read_bytes(), filename
            )
            if (
                checksum_sha != source_sha
                or checksum_text != manifest.get("official_checksum_text")
            ):
                raise ValueError("historical source CHECKSUM manifest mismatch")
            self._verify_file(zip_path, source_sha)
            normalized_path = self.normalized / f"{revision}.parquet"
            if not normalized_path.is_file():
                raise ValueError("historical normalized Parquet is missing")
            normalized_rows += self._verify_normalized(
                normalized_path,
                revision=revision,
                source_sha=source_sha,
            )
            verified += 1
        return {
            "status": "VERIFIED",
            "verified_source_revisions": verified,
            "verified_normalized_rows": normalized_rows,
        }

    @staticmethod
    def _atomic_bytes(path: Path, body: bytes) -> None:
        partial = path.with_suffix(path.suffix + ".partial")
        with partial.open("wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
        fsync_directory(path.parent)

    def _atomic_json(self, path: Path, document: dict[str, object]) -> None:
        self._atomic_bytes(
            path,
            json.dumps(
                document, sort_keys=True, separators=(",", ":"), default=str
            ).encode(),
        )
