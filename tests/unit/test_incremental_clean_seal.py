from __future__ import annotations

import io
import os
import random
import struct
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import cbor2
import google_crc32c
import pytest

import binance_market_data_recorder.spool.recovery as recovery_module
import binance_market_data_recorder.spool.seal as seal_module
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.format import (
    BYTE_ORDER_MARKER,
    FIXED_HEADER,
    FORMAT_MAJOR,
    FORMAT_MINOR,
    FRAME_PREFIX,
    MAGIC,
    ChunkFormatError,
    ChunkHeader,
    decode_chunk_header,
    decode_envelope,
    encode_chunk_header,
    scan_chunk,
)
from binance_market_data_recorder.spool.recovery import recover_partials, recover_storage
from binance_market_data_recorder.spool.seal import (
    RECONNECT_GAP_FLAG,
    SealError,
    _seal_clean_writer,
    seal_partial,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import StorageLayout, ensure_storage_layout


def _envelope(
    ordinal: int,
    *,
    connection_id: str = "connection-1",
    source_sequence: dict[str, int | str] | None = None,
    flags: tuple[str, ...] = (),
    payload: bytes | None = None,
    exchange_times: tuple[int | None, int | None, int | None] | None = None,
    collector_instance_id: str = "collector-1",
) -> EventEnvelope:
    event_time, transaction_time, trade_time = exchange_times or (
        1_700_000_000_000 + ordinal,
        None,
        None,
    )
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        module="binance.spot.v1",
        connection_id=connection_id,
        collector_instance_id=collector_instance_id,
        collector_version="0.1.0+test",
        receive_time_utc_ns=1_700_000_000_000_000_000 + ordinal,
        receive_monotonic_ns=5_000_000_000 + ordinal,
        exchange_event_time=event_time,
        exchange_transaction_time=transaction_time,
        exchange_trade_time=trade_time,
        source_sequence=(
            {"U": ordinal, "u": ordinal}
            if source_sequence is None
            else source_sequence
        ),
        raw_payload=(f'{{"u":{ordinal}}}'.encode() if payload is None else payload),
        capture_flags=flags,
    )


def _writer(
    layout: StorageLayout,
    catalog: Catalog,
    *,
    chunk_id: UUID | None = None,
    created_at_utc_ns: int | None = None,
    durability_interval_seconds: float = 1.0,
    operation_observer: Callable[[str, int], None] | None = None,
) -> RawChunkWriter:
    return RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        durability_interval_seconds=durability_interval_seconds,
        chunk_id=chunk_id,
        created_at_utc_ns=created_at_utc_ns,
        operation_observer=operation_observer,
    )


def _assert_incremental_matches_scan(writer: RawChunkWriter) -> None:
    evidence = writer._take_clean_seal_evidence()
    scanned = scan_chunk(writer.path)
    assert scanned.is_clean
    assert evidence.header == scanned.header
    assert evidence.statistics.mutable_copy() == scanned.statistics
    assert evidence.connection_transitions == scanned.connection_transitions
    assert evidence.file_size == scanned.file_size
    assert evidence.uncompressed_sha256 == scanned.uncompressed_sha256


def _read_envelopes(path: Path) -> list[EventEnvelope]:
    envelopes: list[EventEnvelope] = []
    with path.open("rb", buffering=0) as source:
        header, _header_bytes = decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            assert len(prefix) == FRAME_PREFIX.size
            body_length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            assert body_length <= header.max_frame_bytes
            body = source.read(body_length)
            assert len(body) == body_length
            envelopes.append(decode_envelope(body))
    return envelopes


def _unchecked_header_bytes(mapping: dict[str, object]) -> bytes:
    body = cbor2.dumps(mapping, canonical=True)
    fixed_without_crc = struct.pack(
        ">8sBBHII",
        MAGIC,
        FORMAT_MAJOR,
        FORMAT_MINOR,
        BYTE_ORDER_MARKER,
        0,
        len(body),
    )
    checksum = google_crc32c.value(fixed_without_crc + body)
    return fixed_without_crc + struct.pack(">I", checksum) + body


def test_incremental_evidence_matches_full_scan_exactly(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog, created_at_utc_ns=0)
        writer.append(
            _envelope(
                3,
                exchange_times=(30, 31, 32),
                source_sequence={"numeric": 3, "text": "b"},
                flags=("blue_green_overlap", "deployment_id=green"),
            )
        )
        writer.append(
            _envelope(
                1,
                connection_id="connection-2",
                exchange_times=(10, None, 12),
                source_sequence={"numeric": 1, "text": "a"},
                flags=("blue_green_overlap", "deployment_id=green"),
                collector_instance_id="collector-2",
            )
        )
        writer.append(
            _envelope(
                2,
                connection_id="connection-3",
                exchange_times=(None, 21, None),
                source_sequence={"numeric": "mixed"},
                flags=("sequence_gap", "orderbook_resync"),
                collector_instance_id="collector-2",
            )
        )
        _assert_incremental_matches_scan(writer)


def test_bounded_randomized_differential_evidence(tmp_path: Path) -> None:
    randomizer = random.Random(2304)
    flag_pool = (
        "sequence_gap",
        "orderbook_resync",
        "blue_green_overlap",
        "deployment_id=blue",
    )
    for case in range(32):
        layout = ensure_storage_layout(tmp_path / f"case-{case}")
        with Catalog(layout.catalog) as catalog:
            writer = _writer(layout, catalog, created_at_utc_ns=case)
            for ordinal in range(case % 11):
                sequence: dict[str, int | str]
                if (case + ordinal) % 5 == 0:
                    sequence = {"u": f"s-{ordinal}"}
                elif (case + ordinal) % 7 == 0:
                    sequence = {}
                else:
                    sequence = {"U": ordinal, "u": ordinal + case}
                flags = tuple(
                    flag
                    for flag in flag_pool
                    if randomizer.randrange(5) == 0
                )
                size = (0, 1, 17, 4096)[(case + ordinal) % 4]
                writer.append(
                    _envelope(
                        ordinal,
                        connection_id=f"connection-{(case + ordinal) % 3}",
                        source_sequence=sequence,
                        flags=flags,
                        payload=bytes(randomizer.randrange(256) for _ in range(size)),
                        exchange_times=(
                            None if ordinal % 2 else ordinal,
                            None if ordinal % 3 else ordinal + 1,
                            None if ordinal % 4 else ordinal + 2,
                        ),
                        collector_instance_id=f"collector-{ordinal % 2}",
                    )
                )
            _assert_incremental_matches_scan(writer)


def test_alias_mutation_after_snapshot_cannot_diverge_raw_and_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        envelope = _envelope(7, source_sequence={"U": 7, "u": 7})
        write_entered = threading.Event()
        release_write = threading.Event()
        original_write = os.write
        blocked = False

        def blocking_write(descriptor: int, data: Any) -> int:
            nonlocal blocked
            if descriptor == writer._descriptor and not blocked:
                blocked = True
                write_entered.set()
                assert release_write.wait(timeout=5)
            return original_write(descriptor, data)

        monkeypatch.setattr(os, "write", blocking_write)
        errors: list[BaseException] = []

        def append() -> None:
            try:
                writer.append(envelope)
            except BaseException as exc:  # pragma: no cover - assertion reports it
                errors.append(exc)

        thread = threading.Thread(target=append)
        thread.start()
        assert write_entered.wait(timeout=5)
        envelope.source_sequence["U"] = 700
        envelope.source_sequence["u"] = 700
        envelope.source_sequence["late"] = 1
        release_write.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert errors == []

        evidence = writer._take_clean_seal_evidence()
        scanned = scan_chunk(writer.path)
        persisted = _read_envelopes(writer.path)
        assert persisted[0].source_sequence == {"U": 7, "u": 7}
        assert scanned.statistics.sequence_ranges() == {
            "U": {"min": 7, "max": 7},
            "u": {"min": 7, "max": 7},
        }
        assert evidence.statistics.mutable_copy() == scanned.statistics
        assert evidence.uncompressed_sha256 == scanned.uncompressed_sha256


@pytest.mark.parametrize("created_at", [0, 1_700_000_000_000_000_000])
def test_writer_header_created_at_matches_decoder_contract(
    tmp_path: Path, created_at: int
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog, created_at_utc_ns=created_at)
        writer.close()
        scanned = scan_chunk(writer.path)
        assert scanned.is_clean
        assert scanned.header is not None
        assert scanned.header.created_at_utc_ns == created_at


def test_negative_created_at_is_rejected_before_partial_creation(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog, pytest.raises(
        ChunkFormatError, match="created_at"
    ):
        _writer(layout, catalog, created_at_utc_ns=-1)
    assert list(layout.active.iterdir()) == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("collector_instance_id", ""),
        ("collector_version", ""),
        ("market", ""),
        ("symbol", ""),
        ("stream", ""),
        ("max_frame_bytes", 1023),
        ("max_frame_bytes", 64 * 1024 * 1024 + 1),
    ],
)
def test_header_encoder_and_decoder_reject_same_invalid_semantics(
    field: str, value: object
) -> None:
    header = ChunkHeader(
        chunk_id=UUID("00112233-4455-6677-8899-aabbccddeeff"),
        created_at_utc_ns=0,
        collector_instance_id="collector",
        collector_version="version",
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        max_frame_bytes=1024,
    )
    invalid = ChunkHeader(**{**header.__dict__, field: value})
    with pytest.raises(ChunkFormatError):
        encode_chunk_header(invalid)
    mapping = header.canonical_mapping()
    mapping[field] = value
    with pytest.raises(ChunkFormatError):
        decode_chunk_header(io.BytesIO(_unchecked_header_bytes(mapping)))


@pytest.mark.parametrize("max_frame_bytes", [1024, 64 * 1024 * 1024])
def test_header_supported_frame_bounds_have_encode_decode_parity(
    max_frame_bytes: int,
) -> None:
    header = ChunkHeader(
        chunk_id=uuid4(),
        created_at_utc_ns=0,
        collector_instance_id="collector",
        collector_version="version",
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        max_frame_bytes=max_frame_bytes,
    )
    decoded, encoded = decode_chunk_header(io.BytesIO(encode_chunk_header(header)))
    assert decoded == header
    assert encoded == encode_chunk_header(header)


@pytest.mark.parametrize("failure", ["partial", "zero", "exception"])
def test_failed_frame_write_poison_prevents_clean_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        original_write = os.write
        calls = 0

        def failed_write(descriptor: int, data: Any) -> int:
            nonlocal calls
            if descriptor != writer._descriptor:
                return original_write(descriptor, data)
            calls += 1
            if failure == "partial" and calls == 1:
                return original_write(descriptor, data[:7])
            if failure == "zero":
                return 0
            raise OSError("injected write failure")

        monkeypatch.setattr(os, "write", failed_write)
        with pytest.raises(OSError):
            writer.append(_envelope(1))
        assert writer.poisoned
        with pytest.raises(RuntimeError, match="poisoned"):
            writer._take_clean_seal_evidence()
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.ACTIVE


def test_complete_physical_frame_before_evidence_commit_poisons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)

        def fail_commit(_prepared: object) -> None:
            raise RuntimeError("injected evidence commit failure")

        monkeypatch.setattr(writer, "_commit_prepared_frame", fail_commit)
        with pytest.raises(RuntimeError, match="evidence commit"):
            writer.append(_envelope(1))
        assert writer.poisoned
        assert scan_chunk(writer.path).statistics.record_count == 1
        with pytest.raises(RuntimeError, match="poisoned"):
            writer._take_clean_seal_evidence()


@pytest.mark.parametrize("phase", ["periodic", "final"])
def test_fsync_failure_poison_prevents_clean_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(
            layout,
            catalog,
            durability_interval_seconds=0 if phase == "periodic" else 1,
        )

        def fail_fsync(_descriptor: int) -> None:
            raise OSError("injected fsync failure")

        monkeypatch.setattr(os, "fsync", fail_fsync)
        if phase == "periodic":
            with pytest.raises(OSError, match="fsync"):
                writer.append(_envelope(1))
        else:
            writer.append(_envelope(1))
            with pytest.raises(OSError, match="fsync"):
                writer._take_clean_seal_evidence()
        assert writer.poisoned
        with pytest.raises(RuntimeError, match="poisoned"):
            writer._take_clean_seal_evidence()


def test_close_failure_poison_prevents_clean_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        descriptor = writer._descriptor
        original_close = os.close

        def fail_close(candidate: int) -> None:
            if candidate == descriptor:
                raise OSError("injected close failure")
            original_close(candidate)

        monkeypatch.setattr(os, "close", fail_close)
        with pytest.raises(OSError, match="close"):
            writer._take_clean_seal_evidence()
        assert writer.poisoned
        assert writer.closed
        monkeypatch.setattr(os, "close", original_close)
        original_close(descriptor)


def test_clean_evidence_is_one_shot(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        writer._take_clean_seal_evidence()
        with pytest.raises(RuntimeError, match="already consumed"):
            writer._take_clean_seal_evidence()


@pytest.mark.parametrize(
    "mutation",
    ["header", "frame_prefix", "frame_body", "truncate", "append"],
)
def test_clean_seal_fails_closed_after_source_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1, payload=b"source-mutation-proof"))
        real_compress = seal_module._compress

        def mutate_then_compress(
            source_path: Path, target_path: Path, source_size: int
        ) -> tuple[int, int]:
            raw = bytearray(source_path.read_bytes())
            with source_path.open("rb", buffering=0) as source:
                _header, header_bytes = decode_chunk_header(source)
            if mutation == "header":
                raw[FIXED_HEADER.size] ^= 0x01
            elif mutation == "frame_prefix":
                raw[len(header_bytes) + FRAME_PREFIX.size - 1] ^= 0x01
            elif mutation == "frame_body":
                raw[-1] ^= 0x01
            elif mutation == "truncate":
                raw = raw[:-1]
            else:
                raw.extend(b"unexpected-extra-bytes")
            source_path.write_bytes(raw)
            return real_compress(source_path, target_path, source_size)

        monkeypatch.setattr(seal_module, "_compress", mutate_then_compress)
        with pytest.raises(SealError, match=r"(byte count|readback)"):
            _seal_clean_writer(writer, layout=layout, catalog=catalog)
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.SEALING
        assert writer.path.exists()
        assert not list(layout.manifests.glob("*.manifest.json"))
        assert not list(layout.sealed.glob("*.bmdr.zst"))


def test_clean_and_scan_paths_produce_same_manifest_semantics(tmp_path: Path) -> None:
    chunk_id = UUID("00112233-4455-6677-8899-aabbccddeeff")
    created_at = 1_700_000_000_000_000_000
    manifests: list[dict[str, object]] = []
    for mode in ("clean", "scan"):
        layout = ensure_storage_layout(tmp_path / mode)
        with Catalog(layout.catalog) as catalog:
            writer = _writer(
                layout,
                catalog,
                chunk_id=chunk_id,
                created_at_utc_ns=created_at,
            )
            writer.append(_envelope(1, connection_id="old"))
            writer.append(
                _envelope(
                    2,
                    connection_id="new",
                    flags=("sequence_gap", "orderbook_resync"),
                )
            )
            if mode == "clean":
                manifest = _seal_clean_writer(writer, layout=layout, catalog=catalog)
            else:
                writer.close()
                manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
            manifests.append(manifest)
    nondeterministic = {"sealed_at_utc_ns", "fsync_completed_at_utc_ns"}
    assert {
        key: value for key, value in manifests[0].items() if key not in nondeterministic
    } == {
        key: value for key, value in manifests[1].items() if key not in nondeterministic
    }


def test_scan_routing_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = {"seal": 0, "recovery": 0}
    real_seal_scan = cast(Callable[[Path], Any], seal_module.__dict__["scan_chunk"])
    real_recovery_scan = cast(
        Callable[[Path], Any], recovery_module.__dict__["scan_chunk"]
    )

    def counted_seal_scan(path: Path) -> Any:
        counts["seal"] += 1
        return real_seal_scan(path)

    def counted_recovery_scan(path: Path) -> Any:
        counts["recovery"] += 1
        return real_recovery_scan(path)

    monkeypatch.setattr(seal_module, "scan_chunk", counted_seal_scan)
    monkeypatch.setattr(recovery_module, "scan_chunk", counted_recovery_scan)

    live_layout = ensure_storage_layout(tmp_path / "live")
    with Catalog(live_layout.catalog) as catalog:
        spool = StreamSpool(
            layout=live_layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            queue_capacity=4,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=1,
            max_frame_bytes=1024 * 1024,
        )
        spool.enqueue(_envelope(1))
        spool.close_and_seal()
    assert counts == {"seal": 0, "recovery": 0}

    general_layout = ensure_storage_layout(tmp_path / "general")
    with Catalog(general_layout.catalog) as catalog:
        writer = _writer(general_layout, catalog)
        writer.append(_envelope(1))
        writer.close()
        seal_partial(writer.path, layout=general_layout, catalog=catalog)
    assert counts["seal"] == 1

    recovery_layout = ensure_storage_layout(tmp_path / "recovery")
    with Catalog(recovery_layout.catalog) as catalog:
        writer = _writer(recovery_layout, catalog)
        writer.append(_envelope(1))
        writer.close()
        catalog.transition(
            str(writer.header.chunk_id),
            ChunkState.SEALING,
            idempotency_key=f"sealing:{writer.header.chunk_id}",
            evidence={"verified_frames": 1},
        )
        recover_storage(layout=recovery_layout, catalog=catalog)
    assert counts["recovery"] >= 1
    assert counts["seal"] >= 2


def test_zero_record_reconnect_marker_stays_on_scan_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    scans = 0
    real_scan = cast(Callable[[Path], Any], seal_module.__dict__["scan_chunk"])

    def counted_scan(path: Path) -> Any:
        nonlocal scans
        scans += 1
        return real_scan(path)

    monkeypatch.setattr(seal_module, "scan_chunk", counted_scan)
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            queue_capacity=4,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=1,
            max_frame_bytes=1024 * 1024,
        )
        manifest = spool.close_and_seal(
            forced_flags=frozenset({RECONNECT_GAP_FLAG}),
            seal_intent={
                "required_forced_flags": [RECONNECT_GAP_FLAG],
                "gap_id": "m23-4-zero-marker",
            },
        )
    assert scans == 1
    assert manifest is not None
    assert manifest["record_count"] == 0
    assert manifest["gap"] is True
    assert manifest["complete"] is False


def test_lost_clean_memory_after_close_is_recovered_only_by_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        writer.close()
        scans = 0
        real_scan = cast(
            Callable[[Path], Any], recovery_module.__dict__["scan_chunk"]
        )

        def counted_scan(path: Path) -> Any:
            nonlocal scans
            scans += 1
            return real_scan(path)

        monkeypatch.setattr(recovery_module, "scan_chunk", counted_scan)
        actions = recover_partials(layout=layout, catalog=catalog)
        assert scans == 1
        assert actions[0].action == "unchanged"
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.ACTIVE


@pytest.mark.parametrize(
    "failure_phase",
    ["sealing", "compression", "readback", "rename", "manifest", "catalog_sealed"],
)
def test_clean_seal_failure_boundaries_retain_source_and_never_false_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        original_transition = catalog.transition
        original_replace = os.replace

        if failure_phase in {"sealing", "catalog_sealed"}:
            target = (
                ChunkState.SEALING
                if failure_phase == "sealing"
                else ChunkState.SEALED
            )

            def fail_transition(
                chunk_id: str, state: ChunkState, **kwargs: Any
            ) -> None:
                if state is target:
                    raise OSError(f"injected {failure_phase} failure")
                original_transition(chunk_id, state, **kwargs)

            monkeypatch.setattr(catalog, "transition", fail_transition)
        elif failure_phase == "compression":
            monkeypatch.setattr(
                seal_module,
                "_compress",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("injected compression failure")
                ),
            )
        elif failure_phase == "readback":
            monkeypatch.setattr(
                seal_module,
                "_decompressed_identity",
                lambda _path: (0, "0" * 64),
            )
        elif failure_phase == "rename":

            def fail_replace(source: object, target: object) -> None:
                if str(target).endswith(".bmdr.zst"):
                    raise OSError("injected rename failure")
                original_replace(cast(Any, source), cast(Any, target))

            monkeypatch.setattr(os, "replace", fail_replace)
        else:
            monkeypatch.setattr(
                seal_module,
                "_atomic_json",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("injected manifest failure")
                ),
            )

        with pytest.raises((OSError, SealError), match=r"(injected|readback)"):
            _seal_clean_writer(writer, layout=layout, catalog=catalog)
        assert writer.path.exists()
        assert catalog.state(str(writer.header.chunk_id)) is not ChunkState.SEALED


def test_catalog_sealed_before_source_delete_recovers_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        original_unlink = Path.unlink
        injected = False

        def fail_source_unlink(path: Path, *args: Any, **kwargs: Any) -> None:
            nonlocal injected
            if path == writer.path and not injected:
                injected = True
                raise OSError("injected source unlink failure")
            original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", fail_source_unlink)
        with pytest.raises(OSError, match="source unlink"):
            _seal_clean_writer(writer, layout=layout, catalog=catalog)
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.SEALED
        assert writer.path.exists()
        monkeypatch.setattr(Path, "unlink", original_unlink)
        actions = recover_storage(layout=layout, catalog=catalog)
        assert not writer.path.exists()
        assert any(action.action == "seal_completed_after_crash" for action in actions)


def test_compressor_reported_source_count_is_an_explicit_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        writer.append(_envelope(1))
        real_compress = seal_module._compress

        def wrong_count(
            source: Path, target: Path, source_size: int
        ) -> tuple[int, int]:
            consumed, stored = real_compress(source, target, source_size)
            return consumed - 1, stored

        monkeypatch.setattr(seal_module, "_compress", wrong_count)
        with pytest.raises(SealError, match="source byte count"):
            _seal_clean_writer(writer, layout=layout, catalog=catalog)
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.SEALING
        assert writer.path.exists()
