from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from pathlib import Path

import pytest

from binance_market_data_recorder.archive import (
    OpenSSHRemoteTransport,
    RemoteSourceIdentity,
    RemoteSourceSelection,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceError,
    canonical_descriptor_bytes,
    descriptor_sha256,
    remote_source_descriptor_from_bytes,
    validate_remote_source_identity,
)
from binance_market_data_recorder.archive.remote_transport import (
    RemoteTransportError,
    remote_authority_status_from_bytes,
)
from tests.unit.test_remote_receive import _selection


def test_portable_identity_projects_selection_without_vps_paths() -> None:
    selection = _selection()
    identity = RemoteSourceIdentity(
        descriptor=selection.descriptor,
        descriptor_bytes=selection.descriptor_bytes,
        descriptor_sha256=selection.descriptor_sha256,
        manifest_bytes=selection.manifest_bytes,
    )
    validate_remote_source_identity(identity)
    assert set(field.name for field in fields(RemoteSourceIdentity)) == {
        "descriptor",
        "descriptor_bytes",
        "descriptor_sha256",
        "manifest_bytes",
    }
    assert isinstance(selection, RemoteSourceIdentity)
    assert {field.name for field in fields(RemoteSourceSelection)}.issuperset(
        {"manifest_path", "sealed_path"}
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.rstrip(b"\n"),
        lambda body: b" " + body,
        lambda body: body.replace(b'"market":"spot"', b'"market": "spot"'),
        lambda body: body.replace(b"}\n", b',"extra":1}\n'),
    ],
)
def test_descriptor_parser_rejects_noncanonical_or_nonexact_bytes(mutate: object) -> None:
    selection = _selection()
    with pytest.raises(RemoteSourceError):
        remote_source_descriptor_from_bytes(mutate(selection.descriptor_bytes))  # type: ignore[operator]


def test_descriptor_parser_round_trip_is_exact() -> None:
    selection = _selection()
    parsed = remote_source_descriptor_from_bytes(selection.descriptor_bytes)
    assert parsed == selection.descriptor
    assert canonical_descriptor_bytes(parsed) == selection.descriptor_bytes
    assert descriptor_sha256(selection.descriptor_bytes) == selection.descriptor_sha256


def test_authority_control_parser_is_exact_and_typed() -> None:
    document = {
        "receipt_id": "a" * 64,
        "chunk_id": "00112233-4455-6677-8899-aabbccddeeff",
        "state": "REMOTE_DELETE_PENDING",
        "source_descriptor_sha256": "b" * 64,
        "source_manifest_sha256": "c" * 64,
        "stored_bytes": 3,
        "stored_sha256": "d" * 64,
    }
    body = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    status = remote_authority_status_from_bytes(body)
    assert status is not None and status.canonical_bytes() == body
    assert remote_authority_status_from_bytes(b"null\n") is None
    document["extra"] = True
    with pytest.raises(RemoteTransportError, match="fields"):
        remote_authority_status_from_bytes(
            (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host_alias", "-bad"),
        ("host_alias", "bad host"),
        ("host_alias", "bad;rm"),
        ("remote_recorder_executable", "recorder;rm"),
        ("remote_recorder_executable", "$(recorder)"),
        ("remote_recorder_executable", "../recorder"),
        ("remote_config_path", "relative/config.toml"),
        ("remote_config_path", "/etc/../tmp/config"),
        ("remote_config_path", "/etc/config;rm"),
    ],
)
def test_ssh_constructor_rejects_command_injection(field: str, value: str) -> None:
    arguments: dict[str, object] = {"host_alias": "archive-vps"}
    arguments[field] = value
    with pytest.raises(RemoteTransportError):
        OpenSSHRemoteTransport(**arguments)  # type: ignore[arg-type]


def test_ssh_argv_has_batch_mode_timeout_and_no_host_key_bypass() -> None:
    transport = OpenSSHRemoteTransport(
        host_alias="archive-vps",
        remote_config_path="/etc/binance-market-data-recorder/recorder.toml",
        connect_timeout_seconds=41,
    )
    argv = transport._argv("binance-market-recorder _remote select-oldest")
    joined = " ".join(argv)
    assert argv[:5] == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=41"]
    assert "StrictHostKeyChecking" not in joined
    assert "UserKnownHostsFile" not in joined
    assert "PasswordAuthentication" not in joined


@pytest.mark.parametrize(
    "value",
    ["not-uuid", "-00112233-4455-6677-8899-aabbccddeeff", "x\n"],
)
def test_dynamic_chunk_identity_rejected_before_spawn(value: str) -> None:
    selection = _selection()
    changed = RemoteSourceIdentity(
        descriptor=selection.descriptor.__class__(
            **{**selection.descriptor.document(), "chunk_id": value}  # type: ignore[arg-type]
        ),
        descriptor_bytes=b"invalid",
        descriptor_sha256=hashlib.sha256(b"invalid").hexdigest(),
        manifest_bytes=selection.manifest_bytes,
    )
    transport = OpenSSHRemoteTransport(host_alias="archive-vps")
    with pytest.raises((RemoteSourceError, RemoteTransportError)):
        transport.open_stored_bytes(changed)


def test_missing_ssh_executable_fails_without_semantic_mutation(tmp_path: Path) -> None:
    transport = OpenSSHRemoteTransport(
        host_alias="archive-vps", ssh_executable=str(tmp_path / "missing-ssh")
    )
    with pytest.raises(RemoteTransportError, match="cannot start"):
        transport.select_oldest_source()
