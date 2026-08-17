# Test Environment Matrix

Status: approved future acceptance architecture; this document does not claim
that the VPS profile or its staged windows have been executed.

| Environment | Primary role | Required evidence | Not a substitute for |
| --- | --- | --- | --- |
| MacBook / macOS Apple Silicon | Development, unit/integration/fault tests, short online smoke, local archive-client development | Offline deterministic tests, local filesystem/Catalog faults, short public-data smoke | LAN Linux or production VPS acceptance |
| LAN Linux host | Real Linux pre-production validation | systemd/non-root behavior, Linux storage discovery, archive transfer faults, 24h/72h operational tests, resource and recovery evidence | Exact production VPS long-run acceptance |
| Exact production VPS | Production-specific acceptance | Immutable artifact identity, readiness, 2h, 12h, 24h, 72h, and 168h stages; live-path resource/capacity/archive evidence | No other environment |

## MacBook

The MacBook is the normal development workspace. It exercises macOS LaunchAgent
and Disk Arbitration behavior where available, local Catalog and Raw recovery,
fault injection, short online public endpoints, and future cross-platform
archive-client portability tests. It must not be treated as production.

## LAN Linux

The LAN Linux host exercises the non-root systemd and already-mounted storage
adapter on real Linux. RK3588 evidence remains historical and platform-specific
within this role; it is never relabeled as VPS evidence. LAN runs may include
24h and 72h operational/fault windows, but a passing LAN 72h window does not
advance the VPS acceptance chain.

## Exact production VPS

The production VPS uses Ubuntu 24.04 LTS x86_64, Python 3.12, systemd, and the
shared 2 vCPU/4 GiB/40 GB-class profile from ADR-0028. Acceptance starts only
after exact artifact identity and readiness are proven. Every stage has its own
time anchors and evidence directory; no stage is automatically inferred or
started.

## Cross-environment invariants

- Raw/EventEnvelope, manifest, gap, Catalog market-data, normalized, and replay
  semantics are platform-independent.
- Direct mode is the intended VPS network profile; proxy modes remain testable
  locally and on LAN Linux.
- Heavy Normalize, Replay, and Historical Backfill tests run offline/local and
  do not compete with a live capture acceptance run.
- Tests never write to production Raw, Catalog, or registered archive media
  unless a separately authorized test explicitly provides that environment.
- Historical evidence remains labelled by its actual host and time.
