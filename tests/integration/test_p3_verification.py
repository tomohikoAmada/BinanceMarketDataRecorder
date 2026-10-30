from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import zstandard

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.service import verification
from binance_market_data_recorder.service.lock import ServiceProcessLock
from binance_market_data_recorder.spool.format import decode_chunk_header
from binance_market_data_recorder.spool.readback import read_artifact
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive


def test_seal_enrolls_atomically_and_verified_result_survives_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(fixture.layout.catalog) as catalog:
        assert catalog.raw_verification_status()["pending_files"] == 2
        result = verification.verify_new(fixture.layout, catalog, max_files=1)
        assert result["processed_files"] == 1 and result["pending_files"] == 1
        result = verification.verify_new(fixture.layout, catalog)
        assert result["verified_files"] == 2 and result["pending_files"] == 0
        proofs = catalog._connection.execute(
            "SELECT evidence_json FROM raw_verification_results"
        ).fetchall()
        for row in proofs:
            proof = json.loads(row[0])
            assert proof["verifier_version"] == verification.VERIFIER_VERSION
            assert proof["result"] == "VERIFIED" and proof["record_count"] == 1
            manifest = (fixture.layout.root / proof["manifest_path"]).read_bytes()
            assert proof["manifest_sha256"] == hashlib.sha256(manifest).hexdigest()

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("a caught-up queue must not read historical Raw")

    monkeypatch.setattr(verification, "read_artifact", forbidden)
    with Catalog(fixture.layout.catalog) as reopened:
        assert verification.verify_new(fixture.layout, reopened)["processed_files"] == 0


@pytest.mark.parametrize("mutation", ["compressed", "crc", "statistics", "identity"])
def test_corruption_is_retained_and_cannot_be_published_verified(
    tmp_path: Path,
    mutation: str,
) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        chunk_id = fixture.chunk_ids[0]
        row = catalog.chunk(chunk_id)
        assert row is not None
        artifact = fixture.layout.root / str(row["sealed_path"])
        manifest_path = fixture.layout.root / str(row["manifest_path"])
        original, original_manifest = artifact.read_bytes(), manifest_path.read_bytes()
        manifest = json.loads(original_manifest)
        if mutation == "compressed":
            artifact.write_bytes(original[:-1])
        elif mutation == "crc":
            import io

            expanded = bytearray(zstandard.ZstdDecompressor().decompress(original))
            source = io.BytesIO(expanded)
            _header, header_bytes = decode_chunk_header(source)
            expanded[len(header_bytes) + 12] ^= 1
            mutated = zstandard.ZstdCompressor(write_checksum=True).compress(expanded)
            artifact.write_bytes(mutated)
            manifest.update(
                stored_bytes=len(mutated),
                stored_sha256=hashlib.sha256(mutated).hexdigest(),
                uncompressed_sha256=hashlib.sha256(expanded).hexdigest(),
            )
            catalog._connection.execute(
                "UPDATE chunks SET stored_bytes=?, stored_sha256=?, uncompressed_sha256=? "
                "WHERE chunk_id=?",
                (
                    len(mutated),
                    manifest["stored_sha256"],
                    manifest["uncompressed_sha256"],
                    chunk_id,
                ),
            )
        elif mutation == "statistics":
            manifest["sequence_ranges"]["u"]["max"] += 1
        else:
            manifest["symbol"] = "ETHUSDT"
        manifest_path.write_text(json.dumps(manifest))
        result = verification.verify_new(fixture.layout, catalog)
        assert result["failed_this_batch"] == 1 and result["verified_files"] == 0
        assert artifact.exists() and manifest_path.exists()
        assert len(catalog.operational_events(event_type="RAW_VERIFICATION_FAILED")) == 1
        # The original saved corpus can be checked after repair, without recapture.
        artifact.write_bytes(original)
        manifest_path.write_bytes(original_manifest)
        old = json.loads(original_manifest)
        catalog._connection.execute(
            "UPDATE chunks SET stored_bytes=?, stored_sha256=?, uncompressed_sha256=? "
            "WHERE chunk_id=?",
            (old["stored_bytes"], old["stored_sha256"], old["uncompressed_sha256"], chunk_id),
        )
        catalog.enroll_raw_verification(chunk_id)
        result = verification.verify_new(fixture.layout, catalog)
        assert result["verified_files"] == 1 and result["failed_files"] == 0
        assert len(catalog.operational_events(event_type="RAW_VERIFICATION_FAILED")) == 1


def test_byte_budget_and_duplicate_worker_lock_do_not_skip_pending_files(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        result = verification.verify_new(fixture.layout, catalog, max_bytes=1)
        assert result["processed_files"] == 0
        assert result["budget_blocked_chunk_ids"] == list(fixture.chunk_ids)
        with ServiceProcessLock(fixture.layout.state / "raw-verification.lock"):
            assert verification.verify_new(fixture.layout, catalog)["status"] == "ALREADY_RUNNING"
        assert verification.verify_new(fixture.layout, catalog)["verified_files"] == 1


def test_retired_internal_file_is_verified_from_registered_archive(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        result = ArchiveManager(
            layout=fixture.layout, catalog=catalog, target=fixture.target
        ).run_once()
        assert result.deleted_local_bytes > 0
        checked = verification.verify_new(
            fixture.layout, catalog, external_roots={fixture.target.storage_id: fixture.target.root}
        )
        assert checked["verified_files"] == 1 and checked["failed_files"] == 0


def test_legacy_chunks_are_not_enrolled_by_schema_upgrade(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        catalog._connection.execute("DELETE FROM raw_verification_queue")
    with Catalog(fixture.layout.catalog) as catalog:
        assert verification.verify_new(fixture.layout, catalog)["processed_files"] == 0
        catalog.enroll_raw_verification(fixture.chunk_ids[0])
        assert verification.verify_new(fixture.layout, catalog)["verified_files"] == 1


def test_scanner_and_both_digests_share_one_compressed_read(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        row = catalog.chunk(fixture.chunk_ids[0])
        assert row is not None
        result = read_artifact(fixture.layout.root / str(row["sealed_path"]), scan_frames=True)
        assert result.stored_sha256 == row["stored_sha256"]
        assert result.uncompressed_sha256 == row["uncompressed_sha256"]
        assert result.scan is not None and result.scan.statistics.record_count == 1


def test_mutation_after_readback_invalidates_the_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = prepare_archive(tmp_path)
    original = read_artifact

    def mutated(path: Path, **kwargs: Any) -> Any:
        result = original(path, **kwargs)
        body = path.read_bytes()
        path.write_bytes(body[:-1] + bytes([body[-1] ^ 1]))
        return result

    monkeypatch.setattr(verification, "read_artifact", mutated)
    with Catalog(fixture.layout.catalog) as catalog:
        result = verification.verify_new(fixture.layout, catalog)
        assert result["failed_files"] == 1 and result["verified_files"] == 0


def test_declared_expansion_bound_stops_a_wrong_manifest_before_unbounded_read(
    tmp_path: Path,
) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        row = catalog.chunk(fixture.chunk_ids[0])
        assert row is not None
        with pytest.raises(ValueError, match="expanded artifact exceeds"):
            read_artifact(
                fixture.layout.root / str(row["sealed_path"]),
                scan_frames=True,
                max_uncompressed_bytes=1,
            )


def test_repeated_failure_retries_keep_the_first_exact_failure_evidence(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        row = catalog.chunk(fixture.chunk_ids[0])
        assert row is not None
        artifact = fixture.layout.root / str(row["sealed_path"])
        artifact.write_bytes(b"bad")
        assert verification.verify_new(fixture.layout, catalog)["failed_this_batch"] == 1
        first = catalog.operational_events(event_type="RAW_VERIFICATION_FAILED")
        catalog._connection.execute("UPDATE raw_verification_queue SET next_attempt_utc_ns=0")
        assert verification.verify_new(fixture.layout, catalog)["failed_this_batch"] == 1
        assert catalog.operational_events(event_type="RAW_VERIFICATION_FAILED") == first
        failures = catalog.raw_verification_status()["failures"]
        assert isinstance(failures, list) and failures[0]["attempts"] == 2
