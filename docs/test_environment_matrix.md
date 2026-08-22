# Test Environment Matrix

Status: approved future acceptance architecture; this document does not claim
that the VPS profile or its staged windows have been executed.

| Environment | Primary role | Required evidence | Not a substitute for |
| --- | --- | --- | --- |
| MacBook / macOS Apple Silicon | Development, unit/integration/fault tests, short online smoke, local archive-client development and M22.8 operator side | Offline deterministic tests, local filesystem/Catalog faults, short public-data smoke | Remote Linux integrated acceptance or production VPS staged acceptance |
| Remote Linux test host (preferred: Germany Ubuntu VPS) | M22.8 cross-machine remote-source/archive failure acceptance in an isolated disposable workspace | SSH/network, transfer, receiver storage, receipt, authorization/deletion, response-loss, crash, retry/idempotency, Catalog snapshot, and storage-failure evidence | M22.9 exact VPS staged production acceptance |
| LAN Linux host | Separate real-Linux pre-production validation profile | systemd/non-root behavior, Linux storage discovery, and optionally isolated remote archive faults | M22.9 exact VPS staged production acceptance |
| Exact production VPS | Production-specific acceptance | Immutable artifact identity, readiness, 2h, 12h, 24h, 72h, and 168h stages; live-path resource/capacity/archive evidence | No other environment |

## MacBook

The MacBook is the normal development workspace. It exercises macOS LaunchAgent
and Disk Arbitration behavior where available, local Catalog and Raw recovery,
fault injection, short online public endpoints, and future cross-platform
archive-client portability tests. It must not be treated as production.

## Remote Linux and LAN Linux

M22.8 exercises the integrated cross-machine archive lifecycle against a real
remote Linux environment. The preferred current topology is Mac ->
Internet/SSH -> the Germany Ubuntu VPS -> an isolated disposable M22.8 test
workspace. The same physical VPS may host the production-target environment,
but M22.8 failure injection must use separate filesystem/workspace/config and
test identities/state; it must never touch production Raw, Catalog, receipts,
deletion authorization, remote ownership, or M22.7B evidence. M22.8 is not a
LAN bandwidth, latency, router, or RK3588 hardware acceptance.

The LAN Linux host remains a separate real-Linux validation profile. RK3588
evidence remains historical and platform-specific within that role; it is never
relabeled as VPS evidence. Any LAN run used for M22.8 must still satisfy the
same isolated remote-source/archive semantics, but physical LAN topology is
not required. No M22.8 result advances the M22.9 VPS acceptance chain.

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
