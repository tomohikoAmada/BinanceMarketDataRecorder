from __future__ import annotations

import io
import json
import urllib.error
import zipfile
from dataclasses import replace
from datetime import date
from email.message import Message
from hashlib import sha256
from pathlib import Path
from typing import cast
from urllib.request import Request

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from binance_market_data_recorder.backfill import HistoricalImporter, build_plan
from binance_market_data_recorder.backfill.planner import PlanEntry
from binance_market_data_recorder.cli import build_parser


class Response:
    def __init__(
        self,
        body: bytes,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = io.BytesIO(body)
        self.status = status
        self.headers = headers or {}

    def read(self, amount: int = -1) -> bytes:
        return self.body.read(amount)

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FixtureOpener:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies = bodies
        self.requests: list[Request] = []

    def __call__(self, request: Request, timeout: int) -> Response:
        assert timeout == 30
        self.requests.append(request)
        url = request.full_url
        if url not in self.bodies:
            raise urllib.error.HTTPError(url, 404, "missing", Message(), None)
        body = self.bodies[url]
        range_header = request.get_header("Range")
        if range_header:
            offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
            return Response(
                body[offset:],
                status=206,
                headers={
                    "Content-Range": (
                        f"bytes {offset}-{len(body) - 1}/{len(body)}"
                    )
                },
            )
        return Response(body)


def _zip(rows: list[str]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("BTCUSDT-1m-2025-01-01.csv", "\n".join(rows) + "\n")
    return output.getvalue()


def test_planner_prefers_complete_months_and_uses_daily_edges() -> None:
    plan = build_plan(
        "baseline-bars", date(2025, 1, 15), date(2025, 3, 3)
    )
    klines = [
        entry
        for entry in plan.entries
        if entry.market == "spot" and entry.data_type == "klines"
    ]
    assert [entry.granularity for entry in klines].count("monthly") == 1
    assert any(entry.period == "2025-02" for entry in klines)
    assert all(entry.timestamp_unit == "microseconds" for entry in klines)
    assert cast(int, plan.public_dict()["estimated_bytes"]) > 0


def test_microstructure_profile_is_explicit_and_never_in_baseline() -> None:
    baseline = build_plan("baseline-bars", date(2024, 1, 1), date(2024, 1, 1))
    micro = build_plan(
        "microstructure-trades", date(2024, 1, 1), date(2024, 1, 1)
    )
    assert all(item.data_type not in {"trades", "aggTrades"} for item in baseline.entries)
    assert {item.data_type for item in micro.entries} == {"trades", "aggTrades"}


@pytest.mark.parametrize(
    ("profile", "market", "data_type", "interval", "filename_middle"),
    [
        ("baseline-bars", "spot", "klines", "1m", "1m"),
        ("baseline-bars", "futures/um", "klines", "1m", "1m"),
        ("baseline-bars", "futures/um", "markPriceKlines", "1m", "1m"),
        ("baseline-bars", "futures/um", "indexPriceKlines", "1m", "1m"),
        ("baseline-bars", "futures/um", "premiumIndexKlines", "1m", "1m"),
        ("baseline-bars", "futures/um", "fundingRate", None, "fundingRate"),
        ("microstructure-trades", "spot", "trades", None, "trades"),
        ("microstructure-trades", "futures/um", "trades", None, "trades"),
        ("microstructure-trades", "spot", "aggTrades", None, "aggTrades"),
        ("microstructure-trades", "futures/um", "aggTrades", None, "aggTrades"),
    ],
)
@pytest.mark.parametrize(
    ("start", "end", "granularity", "period"),
    [
        (date(2024, 1, 15), date(2024, 1, 15), "daily", "2024-01-15"),
        (date(2024, 2, 1), date(2024, 2, 29), "monthly", "2024-02"),
    ],
)
def test_planner_uses_official_product_specific_filenames(
    profile: str,
    market: str,
    data_type: str,
    interval: str | None,
    filename_middle: str,
    start: date,
    end: date,
    granularity: str,
    period: str,
) -> None:
    entry = next(
        item
        for item in build_plan(profile, start, end).entries
        if item.market == market
        and item.data_type == data_type
        and item.interval == interval
        and (
            item.granularity == granularity
            or data_type == "fundingRate"
        )
    )
    expected_period = (
        start.strftime("%Y-%m") if data_type == "fundingRate" else period
    )
    if data_type == "fundingRate":
        assert entry.granularity == "monthly"
    expected = f"BTCUSDT-{filename_middle}-{expected_period}.zip"
    assert entry.zip_url.endswith(f"/{expected}")
    assert entry.checksum_url.endswith(f"/{expected}.CHECKSUM")


def _entry() -> PlanEntry:
    return PlanEntry(
        market="spot",
        symbol="BTCUSDT",
        data_type="klines",
        interval="1m",
        granularity="daily",
        period="2025-01-01",
        start_date="2025-01-01",
        end_date="2025-01-01",
        timestamp_unit="microseconds",
        zip_url=(
            "https://data.binance.vision/data/spot/daily/klines/"
            "BTCUSDT/1m/BTCUSDT-1m-2025-01-01.zip"
        ),
        checksum_url=(
            "https://data.binance.vision/data/spot/daily/klines/"
            "BTCUSDT/1m/BTCUSDT-1m-2025-01-01.zip.CHECKSUM"
        ),
        estimated_bytes=1,
    )


def test_checksum_resume_normalization_lineage_and_idempotency(tmp_path: Path) -> None:
    entry = _entry()
    archive = _zip(
        [
            "open_time,open,high,low,close,volume",
            "1735689600000000,1,2,0,1,10",
        ]
    )
    digest = sha256(archive).hexdigest()
    checksum = f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()
    opener = FixtureOpener(
        {entry.zip_url: archive, entry.checksum_url: checksum}
    )
    importer = HistoricalImporter(data_root=tmp_path, opener=opener, chunk_bytes=13)
    identity = sha256(entry.zip_url.encode()).hexdigest()[:16]
    revision = f"{identity}-{digest[:16]}"
    partial_dir = importer.sources / revision
    partial_dir.mkdir(parents=True)
    partial = partial_dir / "BTCUSDT-1m-2025-01-01.zip.partial"
    partial.write_bytes(archive[:17])
    first = importer.import_entry(entry)
    second = importer.import_entry(entry)
    assert first.status == "IMPORTED"
    assert second.status == "ALREADY_VERIFIED"
    assert any(request.get_header("Range") == "bytes=17-" for request in opener.requests)
    table = pq.read_table(first.normalized_path)
    assert table.column("archive_event_time_utc_ns").to_pylist() == [
        1_735_689_600_000_000_000
    ]
    metadata = table.schema.metadata or {}
    assert metadata[b"source_zip_sha256"].decode() == digest
    assert metadata[b"clock_semantics"] == b"archive_source_no_live_receive_clock"
    manifest = json.loads(
        (partial_dir / "source_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["timestamp_unit"] == "microseconds"
    assert manifest["clock_semantics"] == "archive_source_no_live_receive_clock"


def test_checksum_failure_never_commits_zip(tmp_path: Path) -> None:
    entry = _entry()
    archive = _zip(["1735689600000000,1,2,0,1,10"])
    opener = FixtureOpener(
        {
            entry.zip_url: archive,
            entry.checksum_url: (
                f"{'0' * 64}  BTCUSDT-1m-2025-01-01.zip\n".encode()
            ),
        }
    )
    importer = HistoricalImporter(data_root=tmp_path, opener=opener)
    with pytest.raises(ValueError, match="checksum mismatch"):
        importer.import_entry(entry)
    assert not list(importer.sources.glob("*/*.zip"))
    assert not list(importer.sources.glob("*/*.partial"))


def test_range_resume_rejects_wrong_content_range_and_restarts_from_zero(
    tmp_path: Path,
) -> None:
    entry = _entry()
    archive = _zip(["1735689600000000,1,2,0,1,10"])
    digest = sha256(archive).hexdigest()
    checksum = f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()

    class WrongRangeOpener(FixtureOpener):
        def __call__(self, request: Request, timeout: int) -> Response:
            if request.full_url == entry.checksum_url:
                return Response(checksum)
            offset = int(
                cast(str, request.get_header("Range"))
                .removeprefix("bytes=")
                .removesuffix("-")
            )
            return Response(
                archive[offset:],
                status=206,
                headers={
                    "Content-Range": (
                        f"bytes {offset + 1}-{len(archive) - 1}/{len(archive)}"
                    )
                },
            )

    importer = HistoricalImporter(data_root=tmp_path, opener=WrongRangeOpener({}))
    revision = (
        f"{sha256(entry.zip_url.encode()).hexdigest()[:16]}-{digest[:16]}"
    )
    partial = importer.sources / revision / "BTCUSDT-1m-2025-01-01.zip.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(archive[:10])
    with pytest.raises(ValueError, match="Content-Range"):
        importer.import_entry(entry)
    assert not partial.exists()


def test_server_ignoring_range_rewrites_partial_from_zero(tmp_path: Path) -> None:
    entry = _entry()
    archive = _zip(["1735689600000000,1,2,0,1,10"])
    digest = sha256(archive).hexdigest()
    checksum = f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()

    class IgnoreRangeOpener(FixtureOpener):
        def __call__(self, request: Request, timeout: int) -> Response:
            if request.full_url == entry.checksum_url:
                return Response(checksum)
            return Response(archive, status=200)

    importer = HistoricalImporter(data_root=tmp_path, opener=IgnoreRangeOpener({}))
    revision = (
        f"{sha256(entry.zip_url.encode()).hexdigest()[:16]}-{digest[:16]}"
    )
    partial = importer.sources / revision / "BTCUSDT-1m-2025-01-01.zip.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"bad-prefix")
    result = importer.import_entry(entry)
    assert result.status == "IMPORTED"
    assert Path(cast(str, result.zip_path)).read_bytes() == archive


@pytest.mark.parametrize("partial_is_complete", [True, False])
def test_range_416_validates_partial_before_commit_or_restart(
    tmp_path: Path, partial_is_complete: bool
) -> None:
    entry = _entry()
    archive = _zip(["1735689600000000,1,2,0,1,10"])
    digest = sha256(archive).hexdigest()
    checksum = f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()

    class Range416Opener:
        def __init__(self) -> None:
            self.zip_attempts = 0

        def __call__(self, request: Request, timeout: int) -> Response:
            if request.full_url == entry.checksum_url:
                return Response(checksum)
            self.zip_attempts += 1
            if self.zip_attempts == 1:
                raise urllib.error.HTTPError(
                    request.full_url, 416, "range", Message(), None
                )
            return Response(archive)

    opener = Range416Opener()
    importer = HistoricalImporter(data_root=tmp_path, opener=opener)
    revision = (
        f"{sha256(entry.zip_url.encode()).hexdigest()[:16]}-{digest[:16]}"
    )
    partial = importer.sources / revision / "BTCUSDT-1m-2025-01-01.zip.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(archive if partial_is_complete else b"corrupt")
    if partial_is_complete:
        result = importer.import_entry(entry)
        assert result.status == "IMPORTED"
        assert opener.zip_attempts == 1
    else:
        with pytest.raises(ValueError, match="checksum mismatch"):
            importer.import_entry(entry)
        assert not partial.exists()
        result = importer.import_entry(entry)
        assert result.status == "IMPORTED"
        assert opener.zip_attempts == 2


def test_checksum_failure_can_retry_from_zero(tmp_path: Path) -> None:
    entry = _entry()
    good = _zip(["1735689600000000,1,2,0,1,10"])
    bad = _zip(["1735689600000000,9,9,9,9,9"])
    digest = sha256(good).hexdigest()
    checksum = f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()

    class ChangingOpener:
        def __init__(self) -> None:
            self.zip_attempts = 0

        def __call__(self, request: Request, timeout: int) -> Response:
            if request.full_url == entry.checksum_url:
                return Response(checksum)
            self.zip_attempts += 1
            return Response(bad if self.zip_attempts == 1 else good)

    opener = ChangingOpener()
    importer = HistoricalImporter(data_root=tmp_path, opener=opener)
    with pytest.raises(ValueError, match="checksum mismatch"):
        importer.import_entry(entry)
    assert importer.import_entry(entry).status == "IMPORTED"
    assert opener.zip_attempts == 2


def test_normalization_uses_fixed_batches_and_verifies_lineage(
    tmp_path: Path,
) -> None:
    entry = _entry()
    archive = _zip(
        [
            f"{1_735_689_600_000_000 + index},1,2,0,1,10"
            for index in range(7)
        ]
    )
    digest = sha256(archive).hexdigest()
    opener = FixtureOpener(
        {
            entry.zip_url: archive,
            entry.checksum_url: (
                f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()
            ),
        }
    )
    importer = HistoricalImporter(
        data_root=tmp_path,
        opener=opener,
        normalization_batch_rows=2,
    )
    result = importer.import_entry(entry)
    parquet = pq.ParquetFile(cast(str, result.normalized_path))
    assert parquet.metadata.num_rows == 7
    assert parquet.metadata.num_row_groups == 4
    assert all(
        parquet.metadata.row_group(index).num_rows <= 2
        for index in range(parquet.metadata.num_row_groups)
    )
    table = parquet.read()
    assert table.column("source_row_ordinal").to_pylist() == list(range(7))
    metadata = table.schema.metadata or {}
    assert metadata[b"source_revision"].decode() == result.source_revision
    assert metadata[b"source_zip_sha256"].decode() == digest
    assert importer.verify() == {
        "status": "VERIFIED",
        "verified_source_revisions": 1,
        "verified_normalized_rows": 7,
    }


def test_verify_rejects_missing_or_mismatched_normalized_lineage(
    tmp_path: Path,
) -> None:
    entry = _entry()
    archive = _zip(["1735689600000000,1,2,0,1,10"])
    digest = sha256(archive).hexdigest()
    importer = HistoricalImporter(
        data_root=tmp_path,
        opener=FixtureOpener(
            {
                entry.zip_url: archive,
                entry.checksum_url: (
                    f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()
                ),
            }
        ),
    )
    result = importer.import_entry(entry)
    normalized = Path(cast(str, result.normalized_path))
    normalized.unlink()
    with pytest.raises(ValueError, match="Parquet is missing"):
        importer.verify()


def test_404_is_explicit_gap_not_empty_data(tmp_path: Path) -> None:
    importer = HistoricalImporter(data_root=tmp_path, opener=FixtureOpener({}))
    result = importer.import_entry(_entry())
    assert result.status == "GAP"
    assert result.gap is not None
    assert result.gap["reason"] == "CHECKSUM_NOT_FOUND"
    assert len(list(importer.gaps.glob("*.json"))) == 1


def test_official_checksum_change_creates_revision_and_supersedes(
    tmp_path: Path,
) -> None:
    entry = _entry()
    first_zip = _zip(["1735689600000000,1,2,0,1,10"])
    second_zip = _zip(["1735689600000000,2,3,1,2,20"])
    opener = FixtureOpener({})
    importer = HistoricalImporter(data_root=tmp_path, opener=opener)
    revisions: list[str] = []
    for archive in (first_zip, second_zip):
        digest = sha256(archive).hexdigest()
        opener.bodies = {
            entry.zip_url: archive,
            entry.checksum_url: (
                f"{digest}  BTCUSDT-1m-2025-01-01.zip\n".encode()
            ),
        }
        revisions.append(importer.import_entry(entry).source_revision)
    assert revisions[0] != revisions[1]
    second_manifest = json.loads(
        (importer.sources / revisions[1] / "source_manifest.json").read_text()
    )
    assert second_manifest["supersedes"] == [revisions[0]]


def test_historical_source_rejects_nonofficial_hosts(tmp_path: Path) -> None:
    importer = HistoricalImporter(data_root=tmp_path)
    entry = _entry()
    unsafe = replace(
        entry,
        zip_url="https://example.com/x.zip",
        checksum_url="https://example.com/x.zip.CHECKSUM",
    )
    with pytest.raises(ValueError, match=r"official data\.binance\.vision"):
        importer.import_entry(unsafe)


@pytest.mark.parametrize("action", ["plan", "run", "status", "verify"])
def test_backfill_cli_contract(action: str) -> None:
    arguments = ["backfill", action]
    if action in {"plan", "run"}:
        arguments.extend(["--start", "2025-01-01", "--end", "2025-01-02"])
    parsed = build_parser().parse_args(arguments)
    assert parsed.command == "backfill"
    assert parsed.backfill_command == action
