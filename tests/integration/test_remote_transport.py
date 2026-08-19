from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

import pytest

from binance_market_data_recorder.archive import (
    InProcessRemoteTransport,
    OpenSSHRemoteTransport,
    RemoteArchiveSession,
    RemoteArchiveSessionError,
    RemoteReceiveError,
    RemoteReceiver,
    RemoteReceiveTarget,
    RemoteSourceExporter,
    generate_archive_set_id,
)
from binance_market_data_recorder.archive.remote_transport import (
    RemoteTransportProcessError,
)
from binance_market_data_recorder.storage.catalog import Catalog, RemoteArchiveState
from tests.archive_support import PreparedArchive, prepare_archive


@dataclass(frozen=True)
class TransportFixture:
    prepared: PreparedArchive
    target: RemoteReceiveTarget
    config: Path
    shim: Path


def _fixture(root: Path, *, chunk_count: int = 1) -> TransportFixture:
    prepared = prepare_archive(root / "vps", chunk_count=chunk_count, payload_bytes=4096)
    target = RemoteReceiveTarget(
        archive_set_id=generate_archive_set_id(),
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )
    config = root / "remote-recorder.toml"
    config.write_text(f'[recorder]\ndata_root = "{prepared.layout.root}"\n')
    shim = Path(__file__).parents[1] / "remote_transport_ssh_shim.py"
    return TransportFixture(prepared, target, config, shim)


def _ssh(fixture: TransportFixture) -> OpenSSHRemoteTransport:
    return OpenSSHRemoteTransport(
        host_alias="test-vps",
        ssh_executable=str(fixture.shim),
        remote_config_path=fixture.config.as_posix(),
        cleanup_timeout_seconds=2,
    )


def _receive(
    fixture: TransportFixture, transport: Any
) -> tuple[object, object]:
    source = transport.select_oldest_source()
    assert source is not None
    receipt = RemoteReceiver(provider=transport, target=fixture.target).receive(
        source,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    return source, receipt


def test_ssh_selection_manifest_and_raw_are_byte_exact(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        local = RemoteSourceExporter(
            layout=fixture.prepared.layout, catalog=catalog
        ).select_oldest()
    assert local is not None
    transport = _ssh(fixture)
    source = transport.select_oldest_source()
    assert source is not None
    assert source.descriptor_bytes == local.descriptor_bytes
    assert source.manifest_bytes == local.manifest_bytes
    stream = transport.open_stored_bytes(source)
    process = stream.process  # type: ignore[attr-defined]
    with stream:
        body = stream.read()
        assert stream.read(1) == b""
    assert body == local.sealed_path.read_bytes()
    assert process.poll() == 0


@pytest.mark.parametrize(
    "fault",
    [
        "raw_partial",
        "raw_extra",
        "raw_modified",
        "raw_full_nonzero",
        "raw_nonzero_before",
    ],
)
def test_ssh_raw_faults_fail_before_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", fault)
    source = _ssh(fixture).select_oldest_source()
    assert source is not None
    with pytest.raises(RemoteReceiveError):
        RemoteReceiver(provider=_ssh(fixture), target=fixture.target).receive(
            source,
            session_id="12345678-1234-4234-9234-123456789abc",
        )
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        assert catalog.remote_archive_transactions() == []
    assert fixture.prepared.layout.sealed.joinpath(
        Path(source.descriptor.source_relative_path).name
    ).is_file()


def test_full_correct_raw_then_nonzero_is_process_failure_and_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "raw_full_nonzero")
    transport = _ssh(fixture)
    source = transport.select_oldest_source()
    assert source is not None
    stream = transport.open_stored_bytes(source)
    process = stream.process  # type: ignore[attr-defined]
    with pytest.raises(RemoteTransportProcessError), stream:
        assert stream.read() == fixture.prepared.layout.root.joinpath(
            source.descriptor.source_relative_path
        ).read_bytes()
        stream.read(1)
    assert process.poll() is not None


def test_stderr_pressure_does_not_deadlock_raw_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "raw_stderr_pressure")
    source, receipt = _receive(fixture, _ssh(fixture))
    assert receipt.chunk_id == source.descriptor.chunk_id  # type: ignore[attr-defined]


def test_premature_stream_close_terminates_and_reaps_child(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    transport = _ssh(fixture)
    source = transport.select_oldest_source()
    assert source is not None
    stream: BinaryIO = transport.open_stored_bytes(source)
    process = stream.process  # type: ignore[attr-defined]
    stream.close()
    assert process.poll() is not None


def test_inprocess_one_source_session_uses_real_authority_and_deleter(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        result = RemoteArchiveSession(
            transport=InProcessRemoteTransport(
                layout=fixture.prepared.layout, catalog=catalog
            ),
            target=fixture.target,
        ).run_one(
            delete=True,
            session_id="12345678-1234-4234-9234-123456789abc",
        )
        assert result.worked
        assert result.authority is not None
        assert result.authority.state is RemoteArchiveState.REMOTE_DELETED
        assert catalog.remote_archive_transaction(result.authority.receipt_id) is not None
    assert result.source is not None
    assert not fixture.prepared.layout.root.joinpath(
        result.source.descriptor.source_relative_path
    ).exists()


def test_ssh_one_source_session_matches_domain_semantics(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    result = RemoteArchiveSession(
        transport=_ssh(fixture), target=fixture.target
    ).run_one(
        delete=True,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    assert result.worked and result.authority is not None
    assert result.authority.state is RemoteArchiveState.REMOTE_DELETED
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        row = catalog.remote_archive_transaction(result.authority.receipt_id)
        assert row is not None and row["state"] == "REMOTE_DELETED"


def test_authorization_response_loss_resolves_same_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "authorize_response_loss")
    result = RemoteArchiveSession(
        transport=_ssh(fixture), target=fixture.target
    ).run_one(
        delete=False,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    assert result.receipt is not None and result.authority is not None
    assert result.authority.receipt_id == result.receipt.receipt_id
    assert result.authority.state is RemoteArchiveState.REMOTE_DELETE_PENDING


def test_receipt_stdin_is_byte_exact_and_authority_absence_is_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    capture = tmp_path / "receipt.stdin"
    monkeypatch.setenv("BMDR_SSH_SHIM_STDIN_CAPTURE", str(capture))
    transport = _ssh(fixture)
    assert transport.inspect_authority("a" * 64) is None
    result = RemoteArchiveSession(
        transport=transport, target=fixture.target
    ).run_one(
        delete=False,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    assert result.receipt is not None
    assert capture.read_bytes() == result.receipt.canonical_bytes()


def test_delete_response_loss_while_pending_retries_same_receipt_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "delete_response_loss_pending")
    monkeypatch.setenv("BMDR_SSH_SHIM_ONCE_FILE", str(tmp_path / "delete.once"))
    result = RemoteArchiveSession(
        transport=_ssh(fixture), target=fixture.target
    ).run_one(
        delete=True,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    assert result.receipt is not None and result.authority is not None
    assert result.authority.receipt_id == result.receipt.receipt_id
    assert result.authority.state is RemoteArchiveState.REMOTE_DELETED


def test_delete_response_loss_after_terminal_resolves_same_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "delete_response_loss")
    result = RemoteArchiveSession(
        transport=_ssh(fixture), target=fixture.target
    ).run_one(
        delete=True,
        session_id="12345678-1234-4234-9234-123456789abc",
    )
    assert result.receipt is not None and result.authority is not None
    assert result.authority.receipt_id == result.receipt.receipt_id
    assert result.authority.state is RemoteArchiveState.REMOTE_DELETED


def test_stale_selected_source_cannot_redirect_to_another_chunk(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, chunk_count=2)
    transport = _ssh(fixture)
    source = transport.select_oldest_source()
    assert source is not None
    selected_path = fixture.prepared.layout.root / source.descriptor.source_relative_path
    selected_path.rename(selected_path.with_suffix(".changed"))
    stream = transport.open_stored_bytes(source)
    with pytest.raises(RemoteTransportProcessError), stream:
        stream.read(1)
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        assert catalog.remote_archive_transactions() == []


def test_one_source_failure_does_not_select_or_authorize_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, chunk_count=2)
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "raw_partial")
    with pytest.raises(RemoteArchiveSessionError):
        RemoteArchiveSession(
            transport=_ssh(fixture), target=fixture.target
        ).run_one(
            delete=True,
            session_id="12345678-1234-4234-9234-123456789abc",
        )
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        assert catalog.remote_archive_transactions() == []
    assert len(list(fixture.prepared.layout.sealed.glob("*.zst"))) == 2


def test_real_openssh_client_version_probe() -> None:
    assert subprocess.run(
        ["ssh", "-V"], capture_output=True, check=False
    ).returncode == 0
