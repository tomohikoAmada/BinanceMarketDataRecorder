# ADR-0028: VPS production deployment profile

- Status: Accepted
- Date: 2026-08-17
- Supersedes prospectively: deployment assumptions in ADR-0019 and ADR-0026
- Relates to: ADR-0016, ADR-0023, ADR-0025, ADR-0027

## Context

The accepted M20 deployment work proved a Linux ARM64/RK3588 profile, while the
next production direction is a small Germany VPS. A deployment profile must
separate historical RK3588 evidence from the future production authority and
must protect the latency- and integrity-critical capture path from offline
workloads. The VPS is shared infrastructure, not a Recorder-owned appliance.

The current repository contains the Recorder service and Linux adapter, but this
ADR is an architecture freeze. It does not claim that the VPS has been
provisioned, that the service has been deployed there, or that any VPS
acceptance window has passed.

## Decision

The primary production profile is:

- Ubuntu 24.04 LTS, x86_64, Python 3.12;
- a non-root Recorder service managed by systemd;
- 2 vCPU, 4 GiB RAM, and a 40 GB-class filesystem;
- direct Binance connectivity as the intended production network mode;
- a host that may run unrelated services alongside Recorder.

Ubuntu 22.04 LTS x86_64 is a compatibility target. macOS Apple Silicon is a
development/local profile. Ubuntu ARM64/RK3588 remains a distinct Linux
validation and historical evidence profile; its evidence is not VPS evidence.

The VPS owns the live path:

1. Binance public live acquisition;
2. Raw active/spool and sealing/compression;
3. Catalog and recovery;
4. gap, completeness, and provenance state;
5. metrics/status; and
6. support for exporting sealed Raw to the local archive client.

Normalize, heavy Replay/analytical scans, and Historical Backfill are not
default VPS duties. They remain Recorder-owned capabilities and run through
the same distribution/codebase using distinct offline execution profiles on a
local workspace. This is execution-role separation, not a repository or
microservice split.

Capacity policy is based on actual filesystem free space and measured growth;
Recorder does not assume exclusive ownership of the VPS disk or host. The
initial production-profile states are WARNING at 18 GiB, CRITICAL at 14 GiB,
EMERGENCY at 12 GiB, and a protected HARD RESERVE at 10 GiB, with ETA triggers
of 7 days, 72 hours, and 24 hours respectively. The detailed action policy is
in `docs/vps_operations.md` and `docs/storage_contract.md`.

VPS acceptance is staged and artifact-bound:

```text
exact artifact identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
```

LAN Linux validation cannot substitute for the final VPS sequence.

## Invariants

- The live path receives resource priority over offline derivation.
- Hard-reserve behavior never deletes unarchived Raw and always records an
  explicit stop/gap condition.
- A co-resident service cannot change Recorder data, Catalog, proxy, or archive
  policy through an implicit dependency.
- Direct mode is the intended production default; environment and explicit
  proxy modes remain available for local development and testing.
- No deployment-topology change alters EventEnvelope, Raw, manifest, Catalog
  market-data, normalized, or replay semantics.
- Production acceptance evidence is not implied by merged code or historical
  RK3588/LAN evidence.

## Responsibilities

### VPS

The VPS runs the non-root live Recorder service, protects the internal active
and sealed areas, performs recovery and Catalog updates, exposes operational
status, and serves immutable sealed source artifacts to an authenticated local
archive client over the selected transport.

### Local machine/workspace

The local machine runs offline Normalize, Replay, and Historical Backfill work,
holds the Offline Workspace, and runs the future cross-platform archive client.

### Operators

Operators provision the VPS, control unrelated services, select archive media,
and authorize deployment and staged acceptance. Recorder does not manage the
host, mount unrelated media, or change system routing.

## Explicitly excluded scope

- VPS provisioning, deployment, or production service changes;
- Docker, Kubernetes, clustering, Catalog replication, or a second repository;
- automatic mounting, formatting, repair, RAID, replication, or erasure coding;
- mandatory proxy use in production;
- web UI, notification transport, custom API server, or trading capability;
- claiming any VPS acceptance or production-readiness result in this ADR.

## Consequences and tradeoffs

The small shared VPS has a clear low-latency responsibility and avoids
competition from full-data scans. Local processing requires an explicit data
movement workflow and adequate workspace storage. The 40 GB profile leaves
little room for unarchived backlog, so forecasting and archive scheduling are
operationally important. The profile remains intentionally simple: one
distribution with live and offline roles rather than premature service
decomposition.

## Relationship to existing ADRs

ADR-0019 remains the historical authority for the implemented macOS LaunchAgent
profile. ADR-0026 remains the historical and validation authority for Ubuntu
ARM64/RK3588 systemd and mounted-directory behavior. This ADR prospectively
establishes the x86_64 VPS production profile without rewriting either record.
ADR-0016's fail-closed reserve principle remains authoritative; this ADR
selects the initial VPS thresholds. ADR-0027's completeness and reconnect
invariants remain unchanged.

## Migration and implementation implications

Future implementation must add a clearly named production profile and retain
the macOS and RK3588/LAN profiles. It must make live/offline execution roles
explicit, apply the VPS thresholds to real filesystem observations, and expose
the acceptance artifact identity independently of repository HEAD. It must not
move Normalize, Replay, or Backfill into a second product or run them by
default in the live service.

## Validation requirements

- Prove exact Wheel/artifact identity before every VPS stage.
- Prove non-root systemd readiness, live-path recovery, bounded resource use,
  Raw sealing, Catalog durability, and explicit gap behavior.
- Exercise the 2h, 12h, 24h, 72h, and 168h stages on the exact VPS artifact.
- Record skipped LAN, macOS, and physical-media tests separately; never promote
  their results into VPS evidence.
- Validate the threshold and ETA policy with observed ingest/net-growth data
  before revising it through another ADR.
