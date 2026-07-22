from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.update_binance_docs import (
    DocumentationUpdateError,
    DocumentSpec,
    FetchedDocument,
    Selection,
    load_selection,
    update_documents,
    validate_source_url,
)


@pytest.mark.parametrize(
    "url",
    [
        "http://developers.binance.com/en/docs/llms.txt",
        "https://example.com/doc.md",
        "https://github.com/other/project/blob/main/README.md",
        "https://developers.binance.com/en/docs/llms-full.txt",
    ],
)
def test_source_allowlist_and_full_index_default(url: str) -> None:
    with pytest.raises(DocumentationUpdateError):
        validate_source_url(url)


def test_official_sources_are_allowed() -> None:
    assert validate_source_url("https://developers.binance.com/en/docs/llms.txt")
    assert validate_source_url("https://github.com/binance/repository/blob/main/README.md")


def test_selection_rejects_unknown_keys(tmp_path: Path) -> None:
    selection = tmp_path / "selection.toml"
    selection.write_text(
        'index_url = "https://developers.binance.com/en/docs/llms.txt"\nunknown = true\n',
        encoding="utf-8",
    )
    with pytest.raises(DocumentationUpdateError, match="only index_url and documents"):
        load_selection(selection)


def test_update_writes_exact_bytes_and_hash_manifest(tmp_path: Path) -> None:
    page_url = "https://developers.binance.com/en/docs/example.md"
    selection = Selection(
        index_url="https://developers.binance.com/en/docs/llms.txt",
        documents=(
            DocumentSpec(
                id="example",
                url=page_url,
                expected_terms=("required semantics",),
                listed_in_index=True,
            ),
        ),
    )
    bodies = {
        selection.index_url: b"# Binance Developer Docs\n- [example](/en/docs/example.md)\n",
        page_url: b"# Example\nrequired semantics\n",
    }

    def fake_fetch(url: str, **_kwargs: object) -> FetchedDocument:
        return FetchedDocument(url, url, "text/plain", bodies[url])

    manifest = update_documents(selection, tmp_path, fetcher=fake_fetch)
    assert (tmp_path / "example.body").read_bytes() == bodies[page_url]
    saved = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert saved == manifest
    assert saved["remote_content_executed"] is False
    assert saved["llms_full_loaded"] is False
    assert len(saved["sources"][1]["sha256"]) == 64


def test_markdown_portal_html_is_rejected(tmp_path: Path) -> None:
    page_url = "https://developers.binance.com/en/docs/example.md"
    selection = Selection(
        index_url="https://developers.binance.com/en/docs/llms.txt",
        documents=(DocumentSpec("example", page_url, ("portal",), True),),
    )

    def fake_fetch(url: str, **_kwargs: object) -> FetchedDocument:
        body = (
            b"# Binance Developer Docs\n/en/docs/example.md"
            if url.endswith("llms.txt")
            else b"<html>portal</html>"
        )
        return FetchedDocument(url, url, "text/html", body)

    with pytest.raises(DocumentationUpdateError, match="portal HTML"):
        update_documents(selection, tmp_path, fetcher=fake_fetch)
