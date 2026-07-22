# ADR-0002: Framed raw chunk format

- Status: Accepted; byte-level profile refined by ADR-0010
- Date: 2026-07-22

M0.2 scope note: this is the Binance Recorder's Raw format. It preserves
Binance Spot/USD-M provenance without encoding consumer-specific matrix
concepts. It is not a speculative multi-exchange interchange format.

## Decision drivers

The format must preserve exact payload bytes, scan/recover after process death,
detect truncated/corrupt records, stream sequentially, remain simple in Python,
be readable from Go/Rust, compress well, and support immutable seal/manifests.

## Options considered

| Candidate | Raw bytes | Tail recovery | Truncation detection | Portability | Storage/complexity |
| --- | --- | --- | --- | --- | --- |
| NDJSON | awkward base64/escaping | line scan | newline only; corruption weak | excellent | simplest, largest |
| NDJSON + Zstandard | base64/escaping | poor inside active compressed stream | frame-dependent | excellent | compact but crash recovery/append complex |
| Length-prefixed MessagePack | native bytes | forward frame scan | good with checksum | broad, schema discipline needed | compact/simple |
| Length-prefixed CBOR | native bytes and standard tags | forward frame scan | good with checksum | IETF-defined, broad | compact/simple |
| Protobuf/Avro | native bytes | possible | framing still custom | broad | schema toolchain and evolution overhead |

## Decision

Use a language-neutral binary chunk with:

1. a fixed magic, major/minor chunk-format version, byte-order declaration, and
   versioned CBOR header;
2. repeated unsigned big-endian length-prefixed CBOR record frames;
3. exact payload as a CBOR byte string, never decode/re-encode as the Raw source;
4. per-frame CRC32C over the canonical declared length plus frame body;
5. explicit maximum frame length and reserved flags so corruption cannot cause
   unbounded allocation;
6. `.partial` active files written uncompressed for reliable forward recovery;
7. seal by closing/fsyncing, validating every frame, producing manifest and
   whole-artifact SHA-256, then creating a separate Zstandard-compressed sealed
   artifact through a temporary file and atomic rename;
8. SHA-256 for both canonical uncompressed frame stream and stored compressed
   artifact; compression parameters/version recorded in the manifest.

M3 must specify exact header keys, CRC byte order/coverage, maximum sizes,
canonical CBOR rules, Zstandard frame settings, filenames, fsync/rename order,
and golden cross-language byte vectors before declaring the writer accepted.
Compression must not obscure a partial active tail and never modifies a sealed
artifact in place.

Rotation defaults are 60 seconds or 128 MiB, first reached. The maximum
durability window is one second. These values are configurable operational
defaults and not embedded in the file-format version.

## Consequences

- Precise tail recovery and byte fidelity are straightforward.
- Readers need CBOR, CRC32C, and Zstandard support, all language-neutral.
- Two hashes distinguish logical raw content from stored representation.
- A seal needs temporary space; capacity forecasts and emergency reserve must
  account for worst-case seal overhead.
- Human inspection requires a tool, unlike NDJSON.

## Alternatives rejected

NDJSON loses compact native byte representation and has weak per-record
integrity. Compressing an active NDJSON stream complicates bounded crash
recovery. MessagePack is viable but CBOR has a stable public standard and clear
language-independent data model. Schema-heavy formats add tooling without
eliminating the need for record framing and raw JSON bytes.

## Rollback

Before M3 data exists, supersede this ADR. Afterward, retain the versioned reader
and provide a manifest-preserving transcoder that verifies source/destination
hashes; never rewrite existing sealed chunks.
