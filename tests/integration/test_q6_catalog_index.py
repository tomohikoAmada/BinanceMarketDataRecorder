from __future__ import annotations

from pathlib import Path

from binance_market_data_recorder.storage.catalog import Catalog


def test_index_upgrade_preserves_ties_ownership_and_read_only_old_catalog(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        # Selection-only metadata fixture: no physical Raw or retirement operation.
        for chunk, state, created in [("c", "SEALED", 10), ("b", "SEALED", 10),
                                      ("a", "SEALED", 10), ("old", "LOCAL_DELETED", 0)]:
            catalog._connection.execute(
                "INSERT INTO chunks(chunk_id,state,created_at_utc_ns,updated_at_utc_ns) "
                "VALUES(?,?,?,?)", (chunk, state, created, created)
            )
        catalog.reserve_archive_transaction(
            transaction_id="owned-a", chunk_id="a", storage_id="fixture-target", market="spot",
            stream="agg_trade", source_relative_path="sealed/a.bmdr.zst",
            source_manifest_relative_path="manifests/a.json", source_manifest_sha256="a" * 64,
            target_relative_path="raw/a.bmdr.zst", target_temp_relative_path="raw/a.copying",
            external_manifest_relative_path="manifests/a.json", stored_bytes=1,
            stored_sha256="b" * 64,
        )
        catalog._connection.execute("DROP INDEX chunks_by_state_created_id")
        before = [
            tuple(r) for r in catalog._connection.execute("SELECT * FROM chunks ORDER BY chunk_id")
        ]
        expected = catalog.oldest_unowned_sealed_chunk()
        assert expected is not None and expected["chunk_id"] == "b"
    frozen = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with Catalog(path, read_only=True) as catalog:
        assert catalog.oldest_unowned_sealed_chunk() == expected
        assert not catalog._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='chunks_by_state_created_id'"
        ).fetchone()
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == frozen
    with Catalog(path) as catalog:
        assert catalog.oldest_unowned_sealed_chunk() == expected
        assert catalog._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='chunks_by_state_created_id'"
        ).fetchone()
        assert [
            tuple(r) for r in catalog._connection.execute("SELECT * FROM chunks ORDER BY chunk_id")
        ] == before
        catalog._connection.execute("UPDATE chunks SET state='LOCAL_DELETED' WHERE chunk_id='b'")
        next_row = catalog.oldest_unowned_sealed_chunk()
        assert next_row is not None and next_row["chunk_id"] == "c"
