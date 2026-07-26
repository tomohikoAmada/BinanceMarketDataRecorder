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
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = io.BytesIO(body)
        self.status = status
        self.headers: dict[str, str] = {}

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
            return Response(body[offset:], status=206)
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
