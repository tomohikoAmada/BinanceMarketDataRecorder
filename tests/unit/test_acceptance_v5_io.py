from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_corpus import freeze_corpus, verify_corpus
from binance_market_data_recorder.service.acceptance_v5_io import (
    AUDIT_DOMAIN,
    ShardWriter,
    freeze_manifests,
    frozen_manifests,
    shard_records,
    snapshot_manifest,
)
from binance_market_data_recorder.service.archive_timer import (
    ARCHIVE_SERVICE_NAME,
    ARCHIVE_TIMER_NAME,
)
from binance_market_data_recorder.service.systemd import SYSTEMD_SERVICE_NAME
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_deployment_identity import _identity
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope


def stopped(_root: Path) -> dict[str, Any]:
    return {
        "units": {
            unit: {
                "LoadState": "loaded",
                "ActiveState": "inactive",
                "MainPID": "0",
                "UnitFileState": "disabled",
            }
            for unit in (SYSTEMD_SERVICE_NAME, ARCHIVE_SERVICE_NAME, ARCHIVE_TIMER_NAME)
        },
        "active_partial_count": 0,
    }


def fixture(root: Path) -> tuple[Path, str]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
        manifest = seal_chunk(layout, catalog, [usdm_envelope("one", 1)])
        chunk = catalog.chunk(manifest["chunk_id"])
        assert chunk is not None
        relative = str(chunk["manifest_path"])
    return root / relative, relative


def test_exact_private_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "data"
    source, relative = fixture(root)
    evidence = tmp_path / "evidence"
    record = snapshot_manifest(root, relative, evidence)
    assert (evidence / record["object"]).read_bytes() == source.read_bytes()
    assert os.stat(evidence / record["object"]).st_mode & 0o777 == 0o600
    summary = freeze_manifests(root, evidence, "a" * 64)
    corpus = {"catalog_sha256": "a" * 64, "manifest_freeze": summary}
    records = list(frozen_manifests(evidence, corpus))
    assert records[0][0] == record
    source.write_bytes(b"post-freeze arbitrary live mutation")
    assert next(iter(frozen_manifests(evidence, corpus)))[0] == record
    (evidence / record["object"]).write_bytes(b"private corruption")
    with pytest.raises(AcceptanceError, match="corrupted"):
        list(frozen_manifests(evidence, corpus))


@pytest.mark.parametrize("action", ["mutate", "replace", "delete"])
def test_change_between_two_source_reads_fails(tmp_path: Path, action: str) -> None:
    root = tmp_path / "data"
    source, relative = fixture(root)
    original = source.read_bytes()

    def change() -> None:
        if action == "mutate":
            source.write_bytes(original + b" ")
        elif action == "replace":
            replacement = source.with_name("replacement")
            replacement.write_bytes(original)
            replacement.replace(source)
        else:
            source.unlink()

    with pytest.raises((AcceptanceError, FileNotFoundError)):
        snapshot_manifest(root, relative, tmp_path / "evidence", between_reads=change)
    assert not (tmp_path / "evidence" / "quiescence-corpus.json").exists()


def test_symlink_rejected(tmp_path: Path) -> None:
    root = tmp_path / "data"
    source, relative = fixture(root)
    outside = tmp_path / "outside"
    source.rename(outside)
    source.symlink_to(outside)
    with pytest.raises((AcceptanceError, OSError)):
        snapshot_manifest(root, relative, tmp_path / "evidence")
    with pytest.raises(AcceptanceError, match="symlink"):
        freeze_manifests(root, tmp_path / "evidence", "a" * 64)


def test_catalog_backup_and_corpus_binding(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    root = tmp_path / "data"
    fixture(root)
    private = tmp_path / "evidence"
    _path, digest, corpus = freeze_corpus(
        data_root=root,
        root=private,
        identity=identity,
        anchor_sha256="b" * 64,
        boot_id="boot-a",
        probe=stopped,
    )
    assert verify_corpus(private, identity=identity, anchor_sha256="b" * 64) == (corpus, digest)
    with (private / "catalog.sqlite").open("r+b") as output:
        output.seek(512)
        output.write(b"corruption")
    with pytest.raises(AcceptanceError, match="backup corrupted"):
        verify_corpus(private, identity=identity, anchor_sha256="b" * 64)


def test_shard_counts_and_chain(tmp_path: Path) -> None:
    writer = ShardWriter(tmp_path, domain=AUDIT_DOMAIN, anchor="a" * 64, kind="audit-shard")
    for index in range(1025):
        writer.add({"family": "manifest", "key": index})
    writer.flush()
    summary = writer.summary()
    assert summary["shard_count"] == 3
    records = list(
        shard_records(
            tmp_path,
            domain=AUDIT_DOMAIN,
            anchor="a" * 64,
            summary=summary,
            kind="audit-shard",
        )
    )
    assert [record["key"] for record in records] == list(range(1025))
    with pytest.raises(AcceptanceError, match="byte cap"):
        writer.add({"oversize": "x" * (1024 * 1024)})
    (tmp_path / "shard-00000001.json").unlink()
    with pytest.raises(AcceptanceError, match="missing or extra"):
        list(
            shard_records(
                tmp_path,
                domain=AUDIT_DOMAIN,
                anchor="a" * 64,
                summary=summary,
                kind="audit-shard",
            )
        )


def test_duplicate_chunk_identity_and_private_symlink_rejected(tmp_path: Path) -> None:
    root = tmp_path / "data"
    source, relative = fixture(root)
    source.with_name("duplicate.manifest.json").write_bytes(source.read_bytes())
    with pytest.raises(AcceptanceError, match="duplicate"):
        freeze_manifests(root, tmp_path / "evidence", "a" * 64)
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "private-link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(AcceptanceError, match="symlink"):
        snapshot_manifest(root, relative, tmp_path / "private-link")


def test_second_membership_pass_rejects_added_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.service import acceptance_v5_io as module

    root = tmp_path / "data"
    source, _relative = fixture(root)
    original = module.snapshot_manifest

    def changed(*args: Any, **kwargs: Any) -> dict[str, Any]:
        result = original(*args, **kwargs)
        source.with_name("post-inventory.manifest.json").write_bytes(source.read_bytes())
        return result

    monkeypatch.setattr(module, "snapshot_manifest", changed)
    with pytest.raises(AcceptanceError, match="membership changed"):
        freeze_manifests(root, tmp_path / "evidence", "a" * 64)


def test_reordered_shards_rejected(tmp_path: Path) -> None:
    writer = ShardWriter(tmp_path, domain=AUDIT_DOMAIN, anchor="a" * 64, kind="audit-shard")
    for number in range(513):
        writer.add({"key": number})
    writer.flush()
    first, second = tmp_path / "shard-00000000.json", tmp_path / "shard-00000001.json"
    a, b = first.read_bytes(), second.read_bytes()
    first.write_bytes(b)
    second.write_bytes(a)
    with pytest.raises(AcceptanceError):
        list(
            shard_records(
                tmp_path,
                domain=AUDIT_DOMAIN,
                anchor="a" * 64,
                summary=writer.summary(),
                kind="audit-shard",
            )
        )
