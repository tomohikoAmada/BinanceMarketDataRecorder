# Data and Storage

The current implementation writes and archives through local application-data
roots. The approved future profile places the live root on an Ubuntu 24.04
x86_64 VPS and moves verified sealed Raw to a local Offline Workspace through a
pulling archive client. This topology changes custody and execution roles, not
Raw/EventEnvelope semantics.

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

Any failure before verified Catalog commit retains the internal source. In the
future VPS profile, local durable receipt verification, Archive Set/storage
identity, VPS receipt validation, and source revalidation are also required
before VPS deletion. The V1 transport is SSH behind a replaceable
`RemoteTransport` seam; SSH success alone is never deletion authority. See
`archive_transfer_contract.md`.
Recorder never follows a registered target outside its resolved directory,
never writes elsewhere on the volume, and never formats or repairs a
filesystem.

After local deletion, the external artifact may be the only Raw copy. Maintain
an independent backup and periodically run archive verification.

## Space policy

For the future shared 40 GB-class VPS, the initial policy is based on real free
bytes and observed net-growth:

- NORMAL: free > 18 GiB;
- WARNING: free <= 18 GiB or ETA to hard reserve <= 7 days;
- CRITICAL: free <= 14 GiB or ETA <= 72 hours;
- EMERGENCY: free <= 12 GiB or ETA <= 24 hours;
- HARD RESERVE: free <= 10 GiB.

The emergency path prioritizes live capture, seal, Catalog, recovery, and
verified archive-space recovery. At hard reserve it drains/seals what can be
proven, records `DISK_EMERGENCY_STOP` and an explicit gap, and stops accepting
new capture. It never silently deletes unarchived Raw. Forecasts continue to
use 1-hour, 6-hour, 24-hour, and 7-day observed-growth windows.

The existing local implementation's M11 percentage/reserve calculation remains
historical implementation behavior for current local profiles; it is not the
future VPS policy.
