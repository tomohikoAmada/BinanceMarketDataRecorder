# Data and Storage

## Internal-first invariant

Every live event is written to the internal data root first. An active Raw file
ends in `.partial`; it is forward-scannable and crash-recoverable. Sealing
produces an immutable Zstandard artifact, SHA-256 evidence, a manifest, and an
idempotent Catalog transition. SQLite stores lifecycle metadata and aggregates,
not the market-event corpus.

```text
~/Library/Application Support/BinanceMarketDataRecorder/
├── data/
│   ├── active/
│   ├── sealed/
│   ├── manifests/
│   ├── checkpoints/
│   ├── normalized/
│   ├── reports/
│   └── quarantine/
├── state/
│   ├── catalog.sqlite
│   └── service_state.json
└── logs/
```

Raw envelopes retain exact payload bytes, market/symbol/stream identity,
exchange times when present, receive wall and monotonic times, connection and
Collector identity, sequence IDs, and schema/version provenance. Duplicates
may exist in Raw. Normalization applies documented deterministic deduplication
and propagates gaps; it never mutates Raw.

## Rotation and durability

Defaults are 60 seconds or 128 MiB, whichever occurs first, with at most a
one-second configured durability window. The settings are
`rotation_seconds`, `rotation_bytes`, and `durability_interval_seconds`.
Ingress and frame memory are bounded by `ingress_queue_capacity` and
`max_frame_bytes`.

## Optional external archive

Recorder registers one existing directory below an external volume root. The
identity combines volume UUID, relative path, marker, and `storage_id`.
Mountpoint and device number are observations, not identity.

An archive transaction:

1. reserves an internal sealed chunk;
2. writes a transaction-owned `.copying` target;
3. flushes and fsyncs;
4. reopens and rereads the entire target;
5. verifies size and SHA-256;
6. atomically renames the target and commits its external manifest;
7. commits the Catalog transaction;
8. separately authorizes deletion of the internal source.

Any failure before verified Catalog commit retains the internal source.
Recorder never follows a registered target outside its resolved directory,
never writes elsewhere on the volume, and never formats or repairs a
filesystem.

After local deletion, the external artifact may be the only Raw copy. Maintain
an independent backup and periodically run archive verification.

## Space policy

Internal free-space severities are:

- warning at 40% remaining;
- critical at 15% remaining;
- emergency at `max(10 GiB, 5%)`.

Forecasts use 1-hour, 6-hour, 24-hour, and 7-day windows. The emergency path
prioritizes verified archive, suspends non-core derived work, and finally seals
and stops capture at the hard reserve. It never silently deletes unarchived
Raw.
