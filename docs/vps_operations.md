# VPS Operations

Status: approved future production profile; not deployed or production
validated.

This document describes the intended Ubuntu 24.04 LTS x86_64 profile for a
shared 2 vCPU, 4 GiB RAM, 40 GB-class VPS. Ubuntu 22.04 x86_64 is a
compatibility target. macOS is a development/local profile and RK3588 remains
a separate LAN Linux validation and historical evidence profile.

## Responsibility boundary

The VPS runs only the integrity-critical live path:

- Binance public Spot/USD-M acquisition;
- Raw active/spool, sealing, and compression;
- one live Catalog and startup/crash recovery;
- gap, completeness, and provenance state;
- metrics and structured status; and
- support for local-client archive export.

Full Normalize jobs, heavy Replay/analytical scans, and Historical Backfill run
locally through offline execution profiles. They remain Recorder-owned
capabilities in the same distribution. A co-resident unrelated service is not
managed or inspected by Recorder.

## Network profile

Direct Binance connectivity is the intended production default for the Germany
VPS. `environment` and `explicit` proxy modes remain available for local
development, fault injection, and LAN testing. Proxy URLs and credentials do
not enter Raw, manifests, Catalog event bodies, status, or logs.

## Capacity states

Forecasting uses observed filesystem free bytes and measured ingest/net-growth
rates. It does not assume that Recorder owns the filesystem or the host.

| State | Entry condition | Action |
| --- | --- | --- |
| NORMAL | free > 18 GiB and ETA to hard reserve > 7 days | Continue live capture |
| WARNING | free <= 18 GiB or ETA <= 7 days | Continue capture; plan archive soon |
| CRITICAL | free <= 14 GiB or ETA <= 72 hours | Continue integrity-critical work; strongly request archive |
| EMERGENCY | free <= 12 GiB or ETA <= 24 hours | Prioritize capture/seal/Catalog/recovery and archive space recovery |
| HARD RESERVE | free <= 10 GiB | Do not intentionally consume protected reserve; drain/seal, record stop/gap, stop accepting new capture |

The 10 GiB reserve protects the OS and co-resident services. At or near the
reserve, the fail-closed rule remains: never delete unarchived Raw, safely
drain and seal what can be proven, persist `DISK_EMERGENCY_STOP` and the gap
start, and stop before writing the filesystem to zero. Forecast ETAs continue
to use observed 1h, 6h, 24h, and 7d evidence where available.

These are initial VPS-profile thresholds. Changes require another explicit
architecture decision and measured evidence.

## Archive workflow

The local archive client pulls from the VPS over SSH in V1. It verifies local
durability, readback, size, Raw hash, manifest identity, Archive Set identity,
and receipt durability before asking the VPS to delete the source. The VPS
revalidates the source and receipt before authorization. SSH success alone is
never sufficient. After successful authorization, VPS Raw may be deleted
immediately; no grace period is required.

After each successful session, the VPS creates a consistent post-session
Catalog snapshot through a SQLite-supported backup mechanism. The local client
verifies it and retains `latest` and `previous`. A snapshot failure is surfaced
and retried independently; it does not undo Raw integrity.

See [`archive_transfer_contract.md`](archive_transfer_contract.md) and
[`offline_workspace.md`](offline_workspace.md).

## Deployment and acceptance

Future implementation and deployment must use an immutable artifact identity,
then execute this exact sequence on the production VPS:

```text
exact artifact identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
```

Each stage has an independent T0, target, and evidence root. A stage result is
not inferred from the previous stage. LAN Linux 72h evidence does not
substitute for the VPS 72h gate.

This architecture freeze does not provision a VPS, install systemd units,
change a production configuration, or claim any stage has passed.

## Operations and recovery

The future service must remain non-root, bounded, and recovery-first:

1. start with Raw/Catalog recovery before network capture;
2. preserve exact bytes and explicit gap evidence across reconnects;
3. give live capture/seal/Catalog priority over offline work;
4. surface archive absence or transfer failure without stopping capture until
   the hard reserve requires fail-closed stop;
5. retry archive and Catalog snapshot failures idempotently;
6. never claim `COMPLETE` for an unknown interval.

Future notification hooks may report archive pressure, degradation, stop, and
transfer outcomes through Firebase or another future messaging transport, but
notification failure must never control data integrity. Future Web UI work
remains separately authorized and broader than a status screen: it may include
professional market-data visualization, TradingView-like charting concepts,
order flow, depth, trades/volume, host state, Recorder state, completeness and
integrity, archive state, and future controlled operational actions. View,
Health, and Control concerns must remain separable from Raw/Recorder core.
