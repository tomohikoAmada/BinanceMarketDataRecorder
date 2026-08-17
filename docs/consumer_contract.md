# Generic Consumer Contract

Status: `consumer-contract.v1`, implemented by M16 and ADR-0021.

## Boundary

Binance Market Data Recorder publishes immutable Binance datasets to arbitrary
read-only research, backtest, monitoring and simulation consumers. It does not
publish strategies, factors, positions, orders, account state or execution
behavior. No named consumer has privileged fields or lifecycle control.

The supported Python surface is exactly the public names exported by:

```python
from binance_market_data_recorder import replay
```

Consumers must not import `storage`, `archive`, `normalize`, `orderbook`,
Collector or supervisor internals. They do not query Recorder's SQLite tables
or resolve external volumes. The distribution includes `py.typed` so the public
annotations are available to static type checkers.

## Versions

| Contract | Current value | Meaning |
| --- | --- | --- |
| Consumer API | `consumer-contract.v1` | Public types and behavior in this document |
| Dataset | `normalized-dataset.v1` | Selected immutable normalized build |
| Replay order | `replay-order.v1` | Clock and total-order semantics |
| Deduplication | `normalized-dedup.v1` | M15 duplicate/conflict semantics |
| Parquet profile | `bmdr-parquet.v1` | Physical writer/readback profile |
| Stream schema | `normalized-binance-<market>-<stream>.v1` | Typed row fields |

Callers record at least the exact distribution version, build ID, dataset
version, replay-order version, query and policies with every derived result.
Unsupported major versions fail rather than being guessed.

## Dataset discovery

One configured Recorder application-data root is the only storage input:

```python
from pathlib import Path
from binance_market_data_recorder.replay import ManifestCatalog

catalog = ManifestCatalog(
    Path("~/Library/Application Support/BinanceMarketDataRecorder").expanduser()
)
for build in catalog.list_builds():
    print(build.build_id, build.dataset_version, build.normalized_rows)

dataset = catalog.open_build(EXPLICIT_BUILD_ID)
```

Build IDs are content identities, not timestamps. `list_builds()` is
deterministically sorted by manifest filename but does not declare the last
item newest. There is no implicit “latest” build.

Opening verifies:

- build identity and source/checkpoint set;
- build/partition manifest agreement;
- every manifest-relative path remains inside the configured data root;
- partition size and stored SHA-256;
- build and partition row totals;
- checkpoint file/document/book hashes, algorithm version and Raw lineage.

Failure raises `ReplayCatalogError` before Replay. Public `BuildSummary`,
`PartitionDescriptor`, and `CheckpointDescriptor` expose identities, versions,
hashes, counts and time bounds. They do not expose absolute artifact paths,
external mountpoints, archive transaction state or `storage_id`.

Normalized artifacts remain under the internal application-data root even when
their source Raw has been safely archived or moved through the future VPS/local
archive workflow. A consumer therefore never decides whether Raw is internal,
VPS-resident, or external, and the deployment topology does not change this
public data contract.

## Query

`ReplayQuery` supports:

- `markets`: zero or more exact market names; empty means all in the build;
- `streams`: zero or more exact stream names; empty means all;
- `symbol`: exact symbol, currently `BTCUSDT`;
- `start_time_ns`, `end_time_ns`: optional half-open
  `[start_time_ns, end_time_ns)` bounds in the selected clock;
- `clock`: receive or exchange;
- explicit gap and missing-exchange-time policies;
- optional verified checkpoint ID.

All query bounds and `ReplayEvent.event_time_ns` are Unix nanoseconds.

```python
from binance_market_data_recorder.replay import (
    GapPolicy,
    ReplayClock,
    ReplayQuery,
)

query = ReplayQuery(
    clock=ReplayClock.RECEIVE_TIME,
    markets=("spot",),
    streams=("agg_trade",),
    start_time_ns=START,
    end_time_ns=END,
    gap_policy=GapPolicy.ERROR,
)

for event in dataset.replay(query):
    consume_read_only(event.event_time_ns, event.row)
```

`ReplayEvent.row` is an immutable mapping of the selected normalized row.
It retains dataset/schema/dedup versions, market/stream/symbol, exact decimal
text, receive and exchange clocks, source sequences, Raw chunk/record
provenance, Collector/connection identity, payload and logical hashes,
duplicate/conflict evidence, and source quality fields. Stream-specific fields
are defined by `docs/data_contract.md` and the Parquet schema version.

## Event clocks and total order

`ReplayClock.RECEIVE_TIME` selects `receive_time_utc_ns`.
`ReplayClock.EXCHANGE_TIME` selects the stream-specific documented field from
ADR-0021 and converts its milliseconds exactly to nanoseconds. `EventClock`
exposes the same resolution independently when a consumer needs to inspect a
row.

Exchange time is missing for documented cases including Spot bookTicker,
depth snapshots and funding-info observations. The caller must choose:

- `MissingExchangeTimePolicy.ERROR`: fail before yielding;
- `EXCLUDE`: omit rows without exchange time;
- `FALLBACK_RECEIVE`: use receive time and set
  `ReplayEvent.used_receive_time_fallback=true`.

Fallback is never silent.

`replay-order.v1` orders by selected event time and stable logical provenance,
as frozen in ADR-0021. Exchange replay also uses receive UTC immediately after
exchange time. Equal exchange times, duplicate connection sessions and
filesystem enumeration cannot make output nondeterministic. Monotonic values
are compared only after Collector identity.

## Gap policy

A row is unreliable when `source_gap=true` or `source_complete=false`.
Original `source_gap`, `source_complete`, `source_resync`,
`source_recovered`, and source capture flags always remain in the row.

- `GapPolicy.ERROR`: fail before yielding any selected result;
- `INCLUDE`: yield with `ReplayEvent.is_unreliable=true`;
- `EXCLUDE`: omit unreliable rows.

Exclusion does not make adjacent data complete and does not repair a book.
There is no forward fill or synthetic market event.

## Checkpoint seek

Checkpoints accelerate only a single-market, single-symbol `diff_depth` query:

```python
checkpoint = dataset.checkpoint(CHECKPOINT_ID)
query = ReplayQuery(
    markets=(checkpoint.market,),
    streams=("diff_depth",),
    symbol=checkpoint.symbol,
    checkpoint_id=checkpoint.checkpoint_id,
)

restore_book(checkpoint.book)
for event in dataset.replay(query):
    apply_depth(event.row)
```

The checkpoint exposes immutable logical book state, update ID, book/file hash,
source hashes and prior unreliable intervals. Replay skips rows with
`final_update_id <= checkpoint.update_id`. Time and gap policies still apply;
checkpoint state never certifies later continuity. Invalid scope, missing
sequence fields or a checkpoint outside the selected build raises
`CheckpointSeekError`.

## Error model

- `ReplayCatalogError`: build, manifest, artifact or checkpoint verification
  failed;
- `ReplayError`: normalized row or replay work cannot satisfy the contract;
- `MissingExchangeTimeError`: selected exchange clock is unavailable under
  `ERROR`;
- `ReplayGapError`: selected data is unreliable under `ERROR`;
- `CheckpointSeekError`: checkpoint scope or sequence continuation is unsafe.

Replay performs selection and external sorting before its first yield, so
clock/gap/checkpoint errors do not appear after a partially yielded sequence.
An I/O failure can still interrupt iteration; consumers commit their own
derived work transactionally.

## Resource and concurrency behavior

Replay is read-only. It does not mutate Raw, normalized Parquet, manifests,
Catalog, archive state or checkpoints. It scans Parquet in 10,000-row batches
and uses bounded 32-way external merge passes. Work files use an ephemeral
temporary directory and are not persistent Recorder data.

An opened dataset represents one immutable build. New Recorder builds do not
change it. Consumers may open a new explicit build separately and compare
identities before switching.

## Independent example

`examples/replay_consumer.py` imports only the public replay package. It accepts
an explicit root/build/query, streams events, and emits a deterministic count
and SHA-256 summary:

```bash
python3.12 examples/replay_consumer.py \
  --data-root "$HOME/Library/Application Support/BinanceMarketDataRecorder" \
  --build-id <64-hex-build-id> \
  --market spot \
  --stream agg_trade
```

The example contains no strategy, factor, backtest, account or trading logic.
It is the required generic integration proof; adapting any named external
consumer is optional and cannot change this contract.

## Compatibility and rollback

Additive optional fields may extend a v1 stream schema. Clock meaning, total
order, time units, gap policy, checkpoint seek, identity fields, deduplication,
path resolution or existing required fields cannot change without a new
contract/order/dataset version and migration notes.

Rollback removes the M16 reader/example while retaining the M15 dataset, Raw,
manifests and checkpoints. A consumer that needs `replay-order.v1` pins the
matching distribution or exports its selected replay result with full version
and build provenance.
