# ADR-0016: Capacity forecast and emergency reserve

- Status: Accepted
- Date: 2026-07-23
- Milestone: M11

## Context

Daily last/max gauges cannot reconstruct a trustworthy consumption curve or
distinguish capture growth from verified archive deletion. Capacity decisions
must survive restart, cover internal and registered external storage
independently, avoid unstable single-interval extrapolation, and stop capture
before sealing itself can exhaust the filesystem.

## Decision

Persist aggregate-only `storage_space_samples` in Catalog. A sample contains
scope, UTC observation time, total/free bytes, current archive backlog and
oldest-unarchived time; it contains no market event. Internal runtime metrics
sample this history at their existing periodic boundary. UTC times are bucketed
to one minute so independent Spot/USD-M metric recorders idempotently share one
internal sample instead of creating near-simultaneous slope noise. `storage
forecast` also samples internal capacity and every currently accessible
registered target.

For 1 h, 6 h, 24 h and 7 d windows, calculate consecutive interval slopes as
positive bytes consumed per second and select their median. A window is
`INSUFFICIENT_DATA` until it has at least two observations covering 80% of the
window, with both its cutoff anchor and newest sample within 20% of the intended
boundary. Sparse old samples therefore cannot masquerade as a short-window
rate. Capacity changes invalidate only the affected interval. The operational
rate is the maximum available window median: this is conservative under a
recent surge while each input remains outlier-resistant. If no window is
available, report `INSUFFICIENT_DATA`; if the selected rate is non-positive,
report `NOT_APPROACHING`.

Thresholds are exact integer byte comparisons:

- warning: free <= 40% of total;
- critical: free <= 15% of total;
- emergency: free <= `max(10 GiB, 5% of total)`;
- exhausted: zero free bytes.

The most severe reached threshold wins. Positive-rate ETAs include UTC
nanoseconds and ISO UTC text. Already crossed thresholds are `REACHED`.
Unrepresentably distant times are `BEYOND_SUPPORTED_RANGE`; JSON never contains
NaN or infinity.

Severity transitions are append-only Catalog alerts. Internal and each
`external:<storage_id>` scope have independent histories. As clarified for
M21.0, an accessible external target at WARNING remains `READY` and reports its
exact warning so archival can continue. CRITICAL and EMERGENCY report
`LOW_SPACE` and block new archive transactions. Threshold values are unchanged.

Emergency and hard stop are distinct. EMERGENCY first suspends non-core work
and prioritizes only already-safe verified archive/delete work. The hard reserve
is:

```text
max(5 GiB, 2% of total capacity, 2 * configured Raw rotation bytes)
```

At or below it, the coordinator drains/seals active Raw, stops Collectors,
persists `DISK_EMERGENCY_STOP`, and opens a gap at the observation time. It has
no operation that deletes unarchived Raw.

## Consequences

- Archive insertion can make net growth non-positive without rewriting history.
- Forecast quality becomes available progressively after 48 minutes, 4.8
  hours, 19.2 hours and 5.6 days of each configured window.
- The hard reserve preserves multiple maximum-size rotation buffers plus a
  fixed minimum on ordinary disks.
- M11 provides the monitor/coordinator library; persistent process scheduling
  and launchd ownership remain M14.

## Alternatives rejected

- Extrapolating a single free-space delta: too sensitive to compaction and other
  processes.
- Linear regression without robustification: outliers can dominate ETA.
- Using daily gauges only: insufficient timestamp resolution.
- Stopping immediately at the emergency alert: leaves no archive-first margin.
- Deleting oldest unarchived Raw under pressure: violates the core data contract.

## Rollback

Keep the conservative hard-stop behavior enabled. Forecast samples, alerts and
ETAs are derived Catalog data and may be ignored or rebuilt. Rollback never
deletes Raw, archive files, manifests, or storage registrations.
