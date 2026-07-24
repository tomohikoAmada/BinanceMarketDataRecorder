# ADR-0020: Content-addressed normalized Parquet datasets

- Status: Accepted
- Date: 2026-07-24
- Milestone: M15

## Context

Raw chunk v1 is the immutable system of record. It already seals active
uncompressed framing into a separately verified Zstandard artifact under
ADR-0002/ADR-0010. Research consumers need typed, UTC-partitioned Parquet, but
normalization must remain rebuildable, retain malformed and incomplete
evidence, and deterministically remove ordinary transport and ADR-0018
blue/green duplicates. Re-running a build must not overwrite Raw or make
filesystem enumeration order semantic.

The official Apache Arrow Python documentation defines `ParquetWriter` and
`write_table` over explicit Arrow schemas and supports Zstandard compression,
row-group sizing, statistics and Parquet logical type version selection.
PyArrow 25.0.0 is the exact certified writer. DuckDB 1.5.5 is an exact
development dependency used only to prove that the published files and Hive
partition paths are independently queryable.

Implementation references retrieved 2026-07-24:

- Apache Arrow Python `pyarrow.parquet.ParquetWriter` API:
  <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html>
- DuckDB Parquet overview (`read_parquet`, Hive partitioning):
  <https://duckdb.org/docs/stable/data/parquet/overview>

## Decision

### Dataset and artifact identity

The dataset contract is `normalized-dataset.v1`. Output lives only below:

```text
data/normalized/normalized-dataset.v1/
├── artifacts/market=<market>/stream=<stream>/date=<UTC-date>/hour=<HH>/
│   ├── part-<logical-sha256>.parquet
│   └── part-<logical-sha256>.manifest.json
└── builds/<build-id>.manifest.json
```

`build-id` is SHA-256 over the dataset version, the sorted immutable Raw
manifest identities and the sorted verified checkpoint identities. A partition
name is SHA-256 over its canonical logical rows. A repeated build over identical
Raw/checkpoints therefore selects the same build and partition identities.
Existing artifacts are reused only after Parquet, stored SHA-256 and logical
row readback agree; mismatches are errors and are never overwritten.

Every write uses an in-directory unique `.partial`, file fsync, reopened
readback, atomic rename and directory fsync. Parquet is written with an explicit
schema, format version 2.6 for nanosecond-capable types, Zstandard level 3,
statistics, deterministic row order, fixed row-group size and no writer-created
timestamps. Logical determinism is the compatibility promise; byte identity is
also recorded but may change only with a new writer profile/schema version.
At the next locked build, hidden M15-owned partials and the disposable `.work`
tree are removed before new output is generated; final content-addressed files
are never selected by that cleanup.

### Source verification and location

Before decoding, each source is verified against `raw-chunk-manifest.v1` by
stored size/SHA-256 and decompressed logical SHA-256. The normalizer prefers the
internal artifact. If M10 already deleted it, the same verified archive
transaction may resolve it through a currently READY registered target; the
build records content identity, never the internal or external absolute path.
Missing or unverified Raw is a hard build failure, not a silently partial
dataset.

Raw is streamed through the frozen header/frame/CRC/canonical-CBOR reader.
Partition assignment uses `receive_time_utc_ns` converted to UTC date/hour.
Source manifest ordering and physical path do not affect row order.

### Schemas, malformed input and gaps

Each market/stream has a versioned explicit Arrow schema with common provenance
columns and typed stream fields. Decimal exchange values remain exact text;
millisecond exchange times and nanosecond receive times retain their units in
column names. Repeated level arrays and complex public REST models use canonical
JSON text rather than a consumer-specific object model.

Every row retains source chunk ID/hash, record and subrecord ordinal, connection
and Collector provenance, capture flags, source sequence mapping, raw-payload
SHA-256 and the semantic/logical hashes used by deduplication. Malformed or
schema-invalid Raw is emitted with `valid=false` and an error code instead of
being dropped. Manifest incompleteness, gap, resync and recovery fields are
propagated to every affected row and partition manifest. No missing value is
forward-filled.

Funding history expands to one typed row per returned event; an empty response
emits an explicit observation row. Funding-info emits the BTCUSDT adjustment
when present or an explicit `symbol_present=false` observation. This preserves
the sparse/no-fixed-cadence semantics from ADR-0012.

Verified M6 order-book checkpoints are not rewritten. The build manifest indexes
their file hash, logical book hash, algorithm/schema version, source Raw hashes
and immutable incomplete intervals. Consumers can therefore select a checkpoint
without treating it as Raw or hiding prior gaps.

### Deterministic deduplication

`normalized-dedup.v1` defines a semantic identity per stream:

- depth uses market plus `U/u` and USD-M `pu`;
- aggregate trade uses market plus aggregate trade ID;
- book ticker uses market plus update ID;
- mark price and liquidation use their documented event/trade identity;
- premium/open-interest snapshots use their documented observation time;
- funding history uses symbol plus funding time;
- depth snapshots use update ID plus canonical model hash;
- funding-info and exchange-info preserve distinct poll observations, except
  attributable ADR-0018 overlap uses deployment plus canonical model identity.

Malformed events use exact payload hash. Within one semantic identity, rows with
the same logical content collapse to the lexicographically smallest stable Raw
provenance tuple. All contributing source references and duplicate count remain
in the winner. If the same semantic identity has different logical content,
none is discarded: every variant remains with `identity_conflict=true`.

### Metrics and operation

`binance-market-recorder normalize run` is an explicit offline/non-core
operation. `normalize status` is read-only. M14 Collector callbacks never run
Parquet work. Each newly committed partition adds an idempotent metric batch
for `normalized_rows` and `normalized_bytes`; repeated builds do not double
count an existing artifact.

Candidates are materialized under the internal normalized `.work` directory,
then externally merge-sorted in fixed 10,000-row runs and dispatched to
partition spools with at most 32 open files. Only one 10,000-row sort/write
batch and one semantic-identity duplicate group are held in memory, rather than
the full history or a full UTC hour. An abnormally large collision group can
still grow memory and remains a measured operational risk. Space-emergency
orchestration may suspend normalization without affecting Raw.

## Consequences

- Raw bytes, hashes, manifests, archive state and Catalog chunk lifecycle do not
  change.
- Builds are append-only/content-addressed. A newer source set creates a new
  build manifest and may share unchanged partition artifacts.
- Complex arrays remain lossless canonical JSON columns, while high-value
  scalar fields are typed and directly queryable.
- A build fails closed if any selected Raw artifact or checkpoint cannot be
  verified.
- PyArrow is a production dependency. DuckDB remains a development-only
  interoperability verifier and is not a Recorder database.

## Alternatives rejected

- Modify or recompress Raw in place: violates the system-of-record contract.
- Glob every Parquet file as one dataset: mixes superseded builds and duplicates
  rows after incremental rebuilds.
- Deduplicate by receive time or payload bytes only: fails across blue/green
  connections and ignores Binance event identity.
- Drop malformed/conflicting rows: hides quality evidence.
- Use pandas/dataframes or a database as the normalization engine: adds
  unnecessary dependencies and risks another event store.
- Let Collector callbacks write Parquet: violates bounded capture isolation.

## Rollback

Stop normalization and remove only content-addressed build/partition artifacts
whose manifests prove `normalized-dataset.v1`. Retain Raw, Raw manifests,
Catalog, archive evidence and M6 checkpoints. Revert M15 code/dependencies and
rebuild later from the same verified Raw. Never edit or delete Raw as rollback.
