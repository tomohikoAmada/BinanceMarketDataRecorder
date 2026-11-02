# ADR-0059: SQLite Catalog WAL+NORMAL Synchronization and Shared Availability Watermark

- **Status**: Implemented (Milestone M1.2, 2026-11-02).
- **Deciders**: Binance Market Data Recorder Architecture Team.
- **Supersedes / Extends**: Extends ADR-0010, ADR-0015, ADR-0051, and ADR-0056.

---

## 1. Context and Problem Statement

In the previous production design (P3/P4), two major factors caused severe SQLite write amplification and disk I/O bottlenecks during live market data capture:

1. **`PRAGMA synchronous = FULL` on Routine Transactions**:
   `Catalog` configured all SQLite connections with `PRAGMA journal_mode = WAL` and `PRAGMA synchronous = FULL`. Every small transaction (such as chunk state transitions, manifest registrations, and availability journal batch commits) executed a synchronous `fsync` on the WAL file. On cloud VPS environments with shared storage (e.g., GreenCloud VPS), disk I/O latency spikes caused contention on SQLite transaction locks, stalling the capture event loop.

2. **Availability Journal Queue and Write Amplification**:
   `AvailabilityJournal` recorded health status by emitting a complete snapshot dictionary for every valid source every 10 seconds. For a single asset like BTC (2,233 sources), 2,233 records were queued every 10 seconds (~13,400 rows/min). For `BTC + ETH` (~4,660 sources), this generated ~28,000 rows/min (~40 million rows/day). If disk flushing was delayed, `pending` queues accumulated multiple redundant snapshots for the same healthy source, amplifying memory usage and SQLite commit delays.

---

## 2. Decision and Architecture

To resolve write amplification while strictly preserving data integrity, crash consistency, and honest continuity auditing, we adopt the following architectural rules:

### 2.1 Default SQLite `WAL + NORMAL` Mode

- **Routine Operational Mode**: `Catalog` connections default to:
  ```sql
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  ```
- **Crash Durability Guarantee**: Under SQLite WAL mode, `synchronous = NORMAL` guarantees full atomicity and ACID crash consistency against operating system and process crashes. Transactions committed before an OS crash are preserved. In the event of catastrophic physical power loss, only transactions committed after the last WAL checkpoint might roll back, but the database itself cannot be corrupted.

### 2.2 Synchronous FULL Barrier for Irreversible Deletion Operations

- **Irreversible Actions**: Actions that authorize or commit the deletion of local Raw data are irreversible (e.g. `ArchiveManager` retiring a local source to `LOCAL_DELETE_PENDING` / `LOCAL_DELETED`, or remote archive deletion authorization and commit).
- **Synchronization Barrier**: Before committing any irreversible deletion transaction, the connection temporarily elevates to:
  ```sql
  PRAGMA synchronous = FULL;
  ```
  The transaction is committed, followed immediately by:
  ```sql
  PRAGMA wal_checkpoint(PASSIVE);
  ```
  and the connection reverts to `PRAGMA synchronous = NORMAL`.
  This barrier ensures that local source files are never unlinked unless the Catalog state authorizing or recording their deletion has been strictly `fsync`ed to durable storage.

### 2.3 `AvailabilityJournal` In-Memory Coalescing

`AvailabilityJournal` distinguishes between two classes of events:
1. **State Transition Events**:
   - Outage beginning (`unavailable_since` set, new gap opened).
   - Recovery (`valid` closing an open gap, gap closed, recovery count incremented).
   - Error failure increments (`failure`, `last_error` updated).
   - Lifecycle suspensions and resumptions (`suspend` / `resume`).
   *Rule*: Transition events represent chronological milestones and are strictly preserved in ordered queue `pending` without dropping or reordering.
2. **Periodic Healthy Checkpoints**:
   - Regular heartbeats for already-healthy sources.
   *Rule*: In-memory coalescing (`_pending_checkpoints`). If a periodic checkpoint for `source_id` is already pending in the queue, subsequent healthy heartbeats update the existing entry in-place rather than appending new rows. Queue length never grows linearly with elapsed time.

### 2.4 Shared Monitor Watermark (`source_availability_watermark`)

Rather than inserting/updating thousands of individual identical rows into `source_availability` every 10 seconds for sources whose health status did not change:
1. **Table Schema**:
   ```sql
   CREATE TABLE IF NOT EXISTS source_availability_watermark (
       selection_id TEXT PRIMARY KEY,
       evaluated_through_utc_ns INTEGER NOT NULL,
       updated_at_utc_ns INTEGER NOT NULL
   );
   ```
2. **Watermark Progression**:
   - Each observation cycle in `SelectedAssetCapture` evaluates the full admitted source set for `source_selection_id`.
   - Transitions (unavailability / recoveries) are queued in `pending`.
   - Routine healthy sources update memory timestamps (`observed_at_utc_ns`, `last_valid_utc_ns`) without emitting individual pending rows.
   - When all state transitions up to time $T$ have been persisted in SQLite, `source_availability_watermark` is updated to $T$ (either alongside the last transition batch or as a single-row update every 10 seconds in steady state).
   - Steady-state SQLite writes drop from ~4,660 rows per 10 seconds to **1 row per 10 seconds**.

### 2.5 Restart Semantics and WAL Tail Rollback Reconciliation

1. **Confirmed Healthy Lower Bound on Startup**:
   When a service restarts and reads `catalog.source_availability(selection_id)`:
   For any active, healthy source (`unavailable_since_utc_ns IS NULL` and not dormant), the confirmed healthy lower bound is computed as:
   $$\text{confirmed\_healthy\_bound} = \max(\text{observed\_at\_utc\_ns}, \text{evaluated\_through\_utc\_ns})$$
   The interval between $\text{confirmed\_healthy\_bound}$ and the new process's first successful observation is honestly recorded as `STARTUP_UNKNOWN` (bounded by at most one checkpoint interval + restart downtime).
2. **WAL Tail Rollback Recovery in `spool/recovery.py`**:
   - If a crash occurs and SQLite WAL rolls back uncheckpointed transactions:
     - Any sealed chunk on disk with valid `.zst` + `.json` manifest whose Catalog row is missing or rolled back to `ACTIVE`, `RECOVERED`, or `SEALING` is automatically rolled forward to `SEALED`. Any duplicate active `.partial` is cleaned up.
     - The availability watermark in Catalog reflects only durable transactions up to the rollback point $T_{durable}$.
     - The restarted process honest-unknown gap begins at $T_{durable}$, ensuring zero silent omission of lost monitoring time.

---

## 3. Consequences and Performance Impact

- **Throughput & Disk I/O**:
  - Steady-state availability writes reduced by **99.98%** (from ~4,660 rows/10s to 1 row/10s for BTC+ETH).
  - Queue backlog in `AvailabilityJournal.pending` remains 0 during steady health.
  - Routine write transactions do not incur per-commit disk `fsync` lock delays.
- **Durability & Safety**:
  - Irreversible deletion paths remain strictly protected by synchronous `FULL` barriers.
  - No historical raw frames, manifests, or checksums are modified.
  - Binary compatibility with Raw v1 and Parquet normalizers is 100% preserved.
