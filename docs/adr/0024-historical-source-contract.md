# ADR-0024: Official historical source contract

- Status: Accepted for M19
- Date: 2026-07-26

## Context

Low-volume bars can be recovered from Binance's official public archive, while
historical L2 and a live receive clock cannot. Archive revisions and 404s must
not be hidden.

## Decision

Historical Importer accepts only credential-free HTTPS URLs below
`data.binance.vision/data/` and the adjacent `.CHECKSUM`. Original ZIP and
checksum text are immutable. A source revision is keyed by official URL plus
official SHA-256; a changed checksum creates a new revision with `supersedes`
lineage. Downloads use `.partial`, bounded one-file concurrency, validated
`206 Content-Range` offsets, safe restart-from-zero on an ignored Range,
explicit `416` handling, fsync, checksum verification, then atomic commit.
Invalid or checksum-failed partials are removed before retry so corrupt bytes
cannot be appended forever. A 404 creates an explicit `historical-gap.v1`.

`baseline-bars` plans Spot 1m klines plus USD-M 1m klines, mark/index/premium
klines and funding rate. `microstructure-trades` is explicit and plans Spot and
USD-M trades/aggTrades. Complete months prefer monthly files; partial months
use daily files, except `fundingRate`, whose verified official public archive
layout is monthly-only. Filenames are product-explicit:

- kline families: `BTCUSDT-<interval>-<period>.zip`;
- trades: `BTCUSDT-trades-<period>.zip`;
- aggTrades: `BTCUSDT-aggTrades-<period>.zip`;
- fundingRate: `BTCUSDT-fundingRate-<period>.zip`.

The planner does not route these products through one ambiguous filename
template.

Timestamp units come from the source contract: Spot archives dated 2025-01-01
or later are microseconds; earlier Spot and USD-M are milliseconds. The
normalizer emits UTC nanoseconds without numeric-size guessing. Historical
CSV normalization reads incrementally and writes fixed 50,000-row Arrow record
batches through `ParquetWriter` to a temporary file. It fsyncs, performs a
bounded logical readback, and atomically commits. No full-file row list is
constructed. Historical Parquet embeds source revision and ZIP SHA-256. Its clock is
`archive_source_no_live_receive_clock`; it must not silently join receive-clock
Live replay.

`backfill verify` re-hashes the source ZIP, reconciles the source manifest and
checksum text, opens the normalized Parquet, and verifies its
`source_revision` and `source_zip_sha256` metadata.

## Deferred data

No historical L2 is invented. Live raw trades and live klines remain deferred:
1m archive bars and Live aggTrade cover current bar construction needs, while
raw trades are imported explicitly if required. Public L2 does not provide L3
queue position.

## Rollback

Remove only derived historical Parquet. Preserve every verified source
revision, checksum, manifest, and gap. Disable an affected dataset rather than
substituting a third-party source.
