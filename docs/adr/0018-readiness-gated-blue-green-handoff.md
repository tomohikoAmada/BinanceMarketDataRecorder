# ADR-0018: Readiness-gated blue/green Collector handoff

- Status: Accepted
- Date: 2026-07-23
- Milestone: M13

## Context

Binance WebSocket connections have a documented 24-hour lifetime, and planned
code upgrades must not create an unmarked capture gap. Replacing a connection
before its successor is usable is unsafe: an open socket alone does not prove
that all core streams are being durably recorded or that diff-depth events can
bridge an official REST snapshot.

Raw already preserves collector instance and version identity and permits
duplicates. M6 provides separate Spot and USD-M order-book reconstruction.
These are the primitives needed for a make-before-break handoff; a new
deployment protocol must not invent weaker health or sequence semantics.

## Decision

Blue/green is one independently running Collector instance per market and
symbol, not an in-place reconnect and not a per-stream partial promotion.
Spot and USD-M are deployed independently so failure in one market does not
stop or promote the other.

Every candidate has a distinct `collector_instance_id` and an explicit
`collector_version`. Its readiness gate requires all of the following:

1. current connections for `diff_depth`, `agg_trade`, and `book_ticker`;
2. at least one event from each current connection drained to the Raw writer;
3. an official public REST depth snapshot drained to Raw;
4. market-specific M6 reconstruction synchronized across the buffered depth
   update and snapshot boundary; and
5. no recorded readiness failure.

A disconnect removes that stream's connection and persisted-event readiness.
Reconnection therefore requires new durable evidence. Candidate snapshot
capture retries with bounded backoff until the depth bridge succeeds or the
handoff stops; ordinary standalone startup retains its existing one-snapshot
behavior.

After candidate readiness, the supervisor establishes fresh event-count
baselines and requires both old and new instances to durably record additional
events. Only then is overlap confirmed and the old stop event set. This proves
make-before-break ordering without pretending that Binance assigns a global
cross-connection sequence to trades or book ticker.

Every Raw event received while a handoff context is active carries additive
capture flags:

- `blue_green_overlap`;
- `deployment_id=<uuid>`;
- `instance_role=active|candidate`; and
- `handoff_reason=upgrade|rollback|connection_rotation`.

The existing instance/version, connection ID, receive clocks, source sequence
and exact payload remain authoritative. M15 owns deterministic normalized
deduplication; M13 only makes duplicates identifiable and never deletes them.

Catalog stores deployment sessions and append-only transition evidence:
`CANDIDATE_STARTING`, `CANDIDATE_READY`, `OVERLAP_CONFIRMED`,
`CUTOVER_COMPLETE`, or `ROLLED_BACK`. Candidate failure, timeout, or loss of
readiness before old shutdown stops the candidate, clears handoff context, and
keeps the old task running. A rollback to an earlier version uses the same
candidate gate with reason `ROLLBACK`; it is not an unsafe direct restart.

Once overlap is confirmed and old shutdown is requested, the ready candidate
is retained even if the old task exits with an error or exceeds its graceful
shutdown timeout. Stopping both instances at that boundary would create the
gap the protocol exists to prevent. The warning remains in Catalog and the old
stop request remains set. If the candidate loses readiness during that final
boundary, an explicit operational gap event is recorded.

Proactive connection rotation calls the identical deployment state machine at
23 hours 40 minutes, before the documented 24-hour limit. The M4/M5
stream-local 23-hour-50-minute planned reconnect remains a safety fallback when
the instance supervisor is absent or delayed; it continues to produce explicit
planned-reconnect evidence.

USD-M side data is failure-isolated and does not gate core L2 promotion. It may
overlap with the instance, but the readiness contract covers only the three
core streams and depth snapshot.

## Consequences

- Planned upgrades do not stop the old instance before durable, synchronized
  candidate readiness and post-readiness overlap evidence.
- Raw overlap is intentional, attributable, and retained.
- Deployment audit survives process restart in SQLite without storing market
  payloads there.
- M13 provides a library lifecycle primitive. LaunchAgent installation,
  process locks, command orchestration, sleep handling, and reboot recovery
  remain M14.
- M13 does not claim zero gaps for network, host, power, or simultaneous
  instance failure. Those failures must be marked and remain M17 evidence.

## Alternatives rejected

- Stop old, then start new: creates a planned capture gap.
- Promote on socket-open health: does not prove durable stream capture or L2
  synchronization.
- Require only a snapshot: says nothing about all core WebSocket streams.
- Treat book ticker as an order-book checksum: Binance does not document that
  semantic, and ADR-0011 rejects it.
- Deduplicate or mutate Raw during handoff: violates Raw immutability and moves
  M15 normalized semantics into the capture path.
- Make USD-M side data block promotion: a sparse or failed auxiliary source
  must not stop core L2 recording.

## Rollback

Before cutover, stop the candidate and retain the active instance. After a
completed cutover, deploy the previous version as a new candidate with reason
`ROLLBACK` and apply the same readiness and overlap gate. Preserve Raw overlap,
Catalog deployment events, operational gap evidence, and collector identities.
Never rewrite history or delete evidence as part of rollback.
