#!/usr/bin/env python3
"""Fetch a small allowlisted Binance documentation selection without executing it."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urldefrag, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

USER_AGENT = "BinanceMarketDataRecorder-doc-updater/0.2"
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
ALLOWED_HOSTS = frozenset({"developers.binance.com", "github.com"})
DEFAULT_SELECTION = Path(__file__).with_name("binance_docs.toml")
DEFAULT_OUTPUT = (
    Path.home() / "Library" / "Caches" / "BinanceMarketDataRecorder" / "binance-docs"
)
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")


class DocumentationUpdateError(RuntimeError):
    """Raised when source policy or retrieval validation fails."""


@dataclass(frozen=True)
class DocumentSpec:
    id: str
    url: str
    expected_terms: tuple[str, ...]
    listed_in_index: bool


@dataclass(frozen=True)
class Selection:
    index_url: str
    documents: tuple[DocumentSpec, ...]


@dataclass(frozen=True)
class FetchedDocument:
    requested_url: str
    final_url: str
    content_type: str
    body: bytes


@dataclass(frozen=True)
class ManifestEntry:
    id: str
    requested_url: str
    final_url: str
    retrieved_at_utc: str
    content_type: str
    bytes: int
    sha256: str
    file: str


def validate_source_url(url: str, *, allow_full_index: bool = False) -> str:
    """Validate scheme, host, organization path, and full-index policy."""

    clean_url, _fragment = urldefrag(url)
    parsed = urlparse(clean_url)
    if parsed.scheme != "https" or parsed.username or parsed.password:
        raise DocumentationUpdateError(f"source must use credential-free HTTPS: {url}")
    if parsed.hostname not in ALLOWED_HOSTS or parsed.port not in {None, 443}:
        raise DocumentationUpdateError(f"source host is not allowlisted: {url}")
    if parsed.hostname == "github.com":
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) < 2 or segments[0] != "binance":
            raise DocumentationUpdateError(f"GitHub source is outside github.com/binance: {url}")
    if not allow_full_index and parsed.path.endswith("/llms-full.txt"):
        raise DocumentationUpdateError("llms-full.txt requires explicit --include-full-index")
    return clean_url


class _SafeRedirectHandler(HTTPRedirectHandler):
    def __init__(self, *, allow_full_index: bool) -> None:
        super().__init__()
        self.allow_full_index = allow_full_index

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        validate_source_url(newurl, allow_full_index=self.allow_full_index)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def load_selection(path: Path, *, allow_full_index: bool = False) -> Selection:
    """Load and strictly validate the project-owned TOML selection."""

    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise DocumentationUpdateError(f"cannot read selection {path}: {exc}") from exc

    if set(raw) != {"index_url", "documents"}:
        raise DocumentationUpdateError("selection must contain only index_url and documents")
    index_url = validate_source_url(
        str(raw["index_url"]), allow_full_index=allow_full_index
    )
    if not index_url.endswith("/llms.txt"):
        raise DocumentationUpdateError("index_url must select llms.txt, not a portal page")

    documents: list[DocumentSpec] = []
    seen_ids: set[str] = set()
    for item in raw["documents"]:
        if set(item) != {"id", "url", "expected_terms", "listed_in_index"}:
            raise DocumentationUpdateError(f"invalid document keys: {sorted(item)}")
        source_id = str(item["id"])
        if not SAFE_ID.fullmatch(source_id) or source_id in seen_ids:
            raise DocumentationUpdateError(f"invalid or duplicate document id: {source_id}")
        seen_ids.add(source_id)
        terms = tuple(str(term) for term in item["expected_terms"])
        if not terms or any(not term for term in terms):
            raise DocumentationUpdateError(f"expected_terms required for {source_id}")
        documents.append(
            DocumentSpec(
                id=source_id,
                url=validate_source_url(
                    str(item["url"]), allow_full_index=allow_full_index
                ),
                expected_terms=terms,
                listed_in_index=bool(item["listed_in_index"]),
            )
        )
    return Selection(index_url=index_url, documents=tuple(documents))


def fetch_document(
    url: str,
    *,
    allow_full_index: bool = False,
    attempts: int = 3,
    timeout_seconds: float = 30,
) -> FetchedDocument:
    """Download bytes after validating the request and every redirect target."""

    requested_url = validate_source_url(url, allow_full_index=allow_full_index)
    opener = build_opener(_SafeRedirectHandler(allow_full_index=allow_full_index))
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = Request(requested_url, headers={"User-Agent": USER_AGENT})
            with opener.open(request, timeout=timeout_seconds) as response:
                status = response.getcode()
                if status != 200:
                    waf_action = response.headers.get("x-amzn-waf-action")
                    detail = f", WAF action={waf_action}" if waf_action else ""
                    raise DocumentationUpdateError(
                        f"source returned HTTP {status}{detail}: {url}"
                    )
                final_url = validate_source_url(
                    response.geturl(), allow_full_index=allow_full_index
                )
                content_type = response.headers.get_content_type()
                body = response.read(MAX_DOCUMENT_BYTES + 1)
                if len(body) > MAX_DOCUMENT_BYTES:
                    raise DocumentationUpdateError(f"source exceeds byte limit: {url}")
                if not body:
                    raise DocumentationUpdateError(f"source returned an empty body: {url}")
                return FetchedDocument(
                    requested_url=requested_url,
                    final_url=final_url,
                    content_type=content_type,
                    body=body,
                )
        except DocumentationUpdateError:
            raise
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(0.5 * (2**attempt))
    raise DocumentationUpdateError(f"failed to fetch {url}: {last_error}") from last_error


def _validate_body(spec: DocumentSpec, fetched: FetchedDocument, index_text: str) -> None:
    text = fetched.body.decode("utf-8", errors="replace")
    if spec.listed_in_index and urlparse(spec.url).path not in index_text:
        raise DocumentationUpdateError(f"selected page is absent from llms.txt: {spec.url}")
    missing = [term for term in spec.expected_terms if term not in text]
    if missing:
        raise DocumentationUpdateError(f"{spec.id} missing expected terms: {missing}")
    parsed = urlparse(spec.url)
    if (
        parsed.hostname == "developers.binance.com"
        and parsed.path.endswith(".md")
        and "<html" in text[:500].casefold()
    ):
        raise DocumentationUpdateError(f"Markdown source resolved to portal HTML: {spec.url}")


def _atomic_write(path: Path, body: bytes) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_bytes(body)
    os.replace(partial, path)


def update_documents(
    selection: Selection,
    output_dir: Path,
    *,
    allow_full_index: bool = False,
    fetcher: Callable[..., FetchedDocument] = fetch_document,
) -> dict[str, object]:
    """Fetch the index and selected pages, then write exact bytes plus a manifest."""

    output_dir.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(UTC).isoformat()
    index = fetcher(selection.index_url, allow_full_index=allow_full_index)
    index_text = index.body.decode("utf-8", errors="strict")
    if not index_text.startswith("# Binance Developer Docs") or "<html" in index_text.casefold():
        raise DocumentationUpdateError("llms.txt response is not the expected text index")

    entries: list[ManifestEntry] = []
    all_sources = (DocumentSpec("llms-index", selection.index_url, (), False), *selection.documents)
    fetched_sources = {"llms-index": index}
    for spec in selection.documents:
        fetched_sources[spec.id] = fetcher(spec.url, allow_full_index=allow_full_index)

    for spec in all_sources:
        fetched = fetched_sources[spec.id]
        if spec.id != "llms-index":
            _validate_body(spec, fetched, index_text)
        filename = f"{spec.id}.body"
        _atomic_write(output_dir / filename, fetched.body)
        entries.append(
            ManifestEntry(
                id=spec.id,
                requested_url=fetched.requested_url,
                final_url=fetched.final_url,
                retrieved_at_utc=retrieved_at,
                content_type=fetched.content_type,
                bytes=len(fetched.body),
                sha256=sha256(fetched.body).hexdigest(),
                file=filename,
            )
        )

    manifest: dict[str, object] = {
        "schema_version": 1,
        "retrieved_at_utc": retrieved_at,
        "remote_content_executed": False,
        "llms_full_loaded": any(
            urlparse(entry.requested_url).path.endswith("/llms-full.txt") for entry in entries
        ),
        "sources": [asdict(entry) for entry in entries],
    }
    _atomic_write(
        output_dir / "manifest.json",
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode(),
    )
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--include-full-index",
        action="store_true",
        help="permit a configured llms-full.txt source; never enabled by default",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        selection = load_selection(
            args.selection, allow_full_index=args.include_full_index
        )
        manifest = update_documents(
            selection,
            args.output_dir.expanduser().resolve(),
            allow_full_index=args.include_full_index,
        )
    except DocumentationUpdateError as exc:
        print(json.dumps({"error": "documentation_update_failed", "message": str(exc)}))
        return 1
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
