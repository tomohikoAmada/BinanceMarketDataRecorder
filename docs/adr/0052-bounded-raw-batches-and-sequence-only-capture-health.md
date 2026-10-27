# ADR-0052: Bounded Raw batches and sequence-only capture health

Status: implemented for the owner's P1 request, 2026-10-27. This supersedes
ADR-0011's use of a full live book as a capture/readiness prerequisite; its
offline reconstruction algorithm, checkpoints and official bridge interpretation
are unchanged. ADR-0051 remains the production qualification authority.

## Capture boundary

Receive timestamps and exact application payload bytes are captured before
parsing. Spot/USD-M metadata extractors tolerate extra fields; invalid JSON,
invalid schema and excessive JSON nesting remain exact Raw payloads marked
`malformed`. Metadata extraction does not require Decimal prices or a full book.
The writer and Catalog remain fail-closed on storage/integrity failures.

Capture readiness retains only bounded depth sequence tuples (U/u, and USD-M pu,
plus receive time), the snapshot bridge, connected/written stream identities and
bounded quality history. It uses the existing Spot snapshot ID + 1 and USD-M
snapshot ID rules, duplicate/stale handling, gap callbacks and overflow recovery.
Malformed depth invalidates sequence health after Raw admission. Complete price
levels, best-price/crossed-book calculations, checkpoint construction and reports
are consumer work, not prerequisites for further capture. A valid sequence is
not evidence of a complete/correct reconstructed book or every exchange event.

Readiness `public_dict()` declares `capture_health_contract=capture-depth-sequence.v1`,
`depth_sequence_synchronized` and `full_orderbook_evaluated=false`. The existing
`orderbook_synchronized` field/property is retained as a compatibility alias for
the snapshot-backed sequence bridge, no longer a full-book computation claim.
Historical evidence retains its original interpretation. Product identity and
other readiness conditions remain; P3 implements per-source availability.

## Bounded append and progress

Raw v1, EventEnvelope v1, manifests, SQLite and readers are unchanged. The existing
batch thread handoff now reaches actual `writev` on POSIX. Platforms without
`writev` join the bounded group and use the existing complete-write loop. Each
writer group holds at most 256 frames and 1 MiB plus one maximum encoded frame;
spool/receipt batches have the same count/payload stopping rule. Rotation checks
retain the first frame that crosses the size limit, then rotate. A group may
finish across a time deadline, as can a single existing frame write.

An immutable semantic snapshot is encoded once per frame. Only a completely
written group commits frame ordinals, connection transitions, stats and the
existing uncompressed SHA-256. That digest still binds the exact stored snapshot
to clean sealing; no new per-event digest is added. Short writes advance through
vectors, interrupted writes retry, and zero/error writes poison clean evidence.
Partial groups may leave complete physical frames; recovery, not a clean writer
claim, determines that tail. Evidence/observer/fsync/close errors retain their
existing poison rules. Independent reads and archive destination readback remain
required; incremental writer evidence does not replace them.

Receipt ownership retains its 64 MiB payload budget through writer processing.
The downstream spool now also has a 64 MiB byte budget (payload plus conservative
metadata charge), in addition to its configured item count. Overflow is explicit;
no discard, growable backlog or hidden retry queue is introduced. A consumer-owned
batch is bounded separately. Multiply these per-stream budgets by the configured
product/stream count; ten assets are not a claim that every queue can saturate on
the production VPS simultaneously.

`record_count` / `written_frames` means completed append, while
`synced_record_count` / `synchronized_frames` advances only after successful fsync.
Spool snapshots conservatively publish completed operations and can lag an
in-flight worker. Receipt queue ownership is not persistence. Periodic and idle
sync, explicit sync-each-frame mode, final drain/sync/seal and owned worker
cancellation remain. No new power-loss promise or owner loss window is implied.

Each connection owns one receive loop and its stop/rotation controls. A buffered
recv yields cooperatively without allocating a task per frame. An idle recv can
be cancelled; an in-hand receipt finishes its existing bounded handoff. Writer
failure wins over a coincident stop; transport errors are arbitrated after control
cleanup so a global stop does not invent a reconnect gap.

Validation and interleaved measurements are recorded in the [P1 result](../milestones/P1/README.md).
The historical P0 protocol/evidence is retained. Local comparisons are not VPS
capacity or P4–P6 production qualification.
