# ADR-0010: Raw chunk v1 byte format and crash lifecycle

- Status: Accepted
- Date: 2026-07-22
- Refines: ADR-0002

## Context

ADR-0002 selected canonical CBOR records in a length-prefixed, CRC32C-protected
uncompressed active chunk, followed by separately compressed immutable sealed
content. M3 must freeze every byte and crash boundary before any network
Collector can create Raw data.

## Decision

All unsigned integers in framing are big-endian. Raw v1 uses only definite-size
CBOR values, shortest integer/length encodings, and deterministic map-key order.
The supported profile contains maps, arrays, UTF-8 text, byte strings, unsigned
or signed integers, booleans, and null; it contains no floats, indefinite
lengths, application tags, or duplicate map keys. `cbor2==6.1.3` with
`canonical=True` is the certified Python encoder. Readers re-encode the decoded
value and reject a non-canonical byte representation.

### Fixed chunk prefix

| Offset | Size | Value |
| ---: | ---: | --- |
| 0 | 8 | ASCII/binary magic `BMRCHNK\x1a` |
| 8 | 1 | format major `1` |
| 9 | 1 | format minor `0` |
| 10 | 2 | byte-order marker `0xfeff` |
| 12 | 4 | chunk flags, zero in v1 |
| 16 | 4 | CBOR header body length |
| 20 | 4 | CRC32C |
| 24 | variable | canonical CBOR header body |

The header CRC32C is Castagnoli CRC-32C over bytes 0–19 followed immediately by
the CBOR body; the four CRC bytes themselves are excluded. Header bodies are at
most 64 KiB.

The header map contains exactly:

- `format = "bmdr-raw-chunk"`;
- `chunk_schema_version = "raw-chunk.v1"`;
- `envelope_schema_version = "event-envelope.v1"`;
- `chunk_id` as the 16 RFC 4122 UUID bytes;
- `created_at_utc_ns`;
- `collector_instance_id` and `collector_version`;
- homogeneous `market`, `symbol`, and `stream` identity;
- `max_frame_bytes`, default 16 MiB and reader-supported range 1 KiB–64 MiB.

### Record frame

| Relative offset | Size | Value |
| ---: | ---: | --- |
| 0 | 4 | canonical CBOR envelope body length |
| 4 | 2 | frame flags, zero in v1 |
| 6 | 2 | reserved, zero in v1 |
| 8 | 4 | CRC32C |
| 12 | variable | canonical CBOR `EventEnvelope v1` body |

The frame CRC32C covers the first eight prefix bytes followed by the complete
body; it excludes the CRC field. Length is checked before allocation. Unknown
flags/reserved values, oversized lengths, CRC failures, non-canonical CBOR,
schema errors, or an event whose market/symbol/stream differs from its header
are hard corruption—not a recoverable truncation.

`raw_payload` is a CBOR byte string and therefore round-trips without JSON
decode/re-encode. `source_sequence` is a text-keyed map of exact integers or
text. All nullable exchange times remain explicit null when absent. CBOR arrays
for `capture_flags` map to the immutable tuple in the Python model.

### Files, seal, and compression

- Active: `data/active/<uuidhex>.bmdr.partial`.
- Sealed: `data/sealed/<uuidhex>.bmdr.zst`.
- Manifest: `data/manifests/<uuidhex>.manifest.json`.
- Temporary sealed and manifest outputs add `.partial` to the final suffix.

Active files are mode 0600 and uncompressed. Creation writes header, fsyncs the
file, and fsyncs its directory before Catalog registration. A frame append uses
a complete-write loop. The configured writer loop calls `sync_if_due` even
while a stream is idle; the default and maximum durability interval is one
second. Rotation is the first of 60 seconds or 128 MiB, both configurable.

Seal order is:

1. fsync and close the active descriptor;
2. forward-scan every frame and compute SHA-256 of the complete uncompressed
   header-plus-frame stream;
3. Catalog transition to `SEALING`;
4. stream-compress to `.zst.partial` using Zstandard level 3, checksum and
   content-size enabled, dictionary ID disabled, and one thread;
5. fsync, reopen/decompress, and compare the complete uncompressed SHA-256;
6. atomically rename to `.bmdr.zst` and fsync the sealed directory;
7. write/fsync/atomically rename the canonical JSON manifest and fsync its
   directory;
8. transactionally record `SEALED` plus hashes/paths in Catalog;
9. unlink the internal uncompressed `.partial` and fsync its directory.

Any failure before step 8 retains the source. Existing final names are reused
only after verified content identity and are never overwritten on mismatch.
All steps have stable idempotency keys. Startup can reconcile a verified sealed
artifact/manifest pair after a crash before Catalog commit.

### Recovery and completeness

A partial frame prefix or body at EOF is truncated to the last verified frame;
the file and directory are fsynced, a `RECOVERED` transition records removed
bytes, and any later manifest has `recovered=true`, recovery evidence, and
`complete=false`. A damaged header, impossible length, unknown flags, CRC
failure, invalid CBOR, invalid envelope, or identity mismatch moves the entire
artifact to `data/quarantine/` with its SHA-256 and reason. Recovery does not
search for a plausible later frame because that could hide corruption.

Raw accepts duplicates. `sequence_gap`, `orderbook_resync`, `recovered_tail`,
`checksum_failure`, or mixed sequence types force `complete=false`; no M3 code
interprets Binance order-book sequence semantics.

### Golden vectors

`tests/golden/raw_chunk_v1.json` freezes a complete one-record chunk.
Python reconstructs and scans it byte-for-byte. The standard-library-only Go
program `tools/verify_raw_chunk_golden.go` independently checks magic, version,
big-endian lengths, and both Castagnoli CRC32C values. Any byte-profile change
requires a new major/minor format decision and new vectors; existing readers
must remain available for existing non-test chunks.

## Dependencies

- `cbor2==6.1.3`: deterministic language-neutral CBOR encoding/decoding.
- `google-crc32c==1.8.0`: hardware-accelerated Castagnoli CRC32C.
- `zstandard==0.25.0`: streaming immutable sealed representation.

All are exact-pinned because their encoded/checksum/compression behavior is
part of the certified Raw implementation. Their full macOS arm64/Python 3.12
resolution is hash-locked.

## Consequences

Recovery uses memory bounded by the configured maximum single frame plus fixed
buffers and statistics cardinality, not record count. Human inspection needs a
reader. Sealing temporarily requires both uncompressed and compressed content;
M11 capacity policy must include that overhead. Zstandard bytes may change only
under a new declared compression profile; logical Raw identity remains the
uncompressed SHA-256.

## Rollback

Stop writers, retain all test artifacts needed for diagnosis, and revert M3.
After any non-test Raw v1 data exists, rollback must retain this reader and use
a verified manifest-preserving transcoder; it may not rewrite sealed chunks.
