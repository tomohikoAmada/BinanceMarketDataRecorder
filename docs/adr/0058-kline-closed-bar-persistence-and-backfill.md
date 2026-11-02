# ADR-0058: WebSocket K-line Closed-Bar Persistence, S12 Direct Deduplication, and Automatic REST Backfill

- **Status**: Implemented (Milestone M1.3, 2026-11-02).
- **Deciders**: Binance Market Data Recorder Architecture Team.
- **Supersedes / Extends**: Extends ADR-0010, ADR-0053, ADR-0054, and ADR-0056.

---

## 1. Context and Problem Statement

During live Binance market data collection across Spot, USD-M, and COIN-M markets, several factors previously caused substantial write amplification, redundant network traffic, and CPU overhead:

1. **Unclosed K-line Update Floods (`k.x == False`)**:
   Binance WebSocket K-line streams push intermediate forming bar updates every 1–2 seconds on Spot and as frequently as every 250ms on futures. Over 95% of these frames represent forming bars with identical open times, updating only the current price, high/low, and running volume. Persisting every forming bar generated hundreds of thousands of ephemeral Raw frames and accelerated spool rotations without providing discrete closed historical bars.

2. **Redundant East-8 Timezone Subscriptions (`S12` vs `S11`)**:
   `S12` (`@+08:00`) subscribed to all 16 Spot K-line intervals. However, any interval that evenly divides 8 hours (`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`) has mathematically identical open and close timestamp boundaries in UTC (`S11`) and UTC+8 (`S12`). Subscribing to these 10 identical intervals in both timezones doubled WebSocket connections and Raw chunks for zero analytical gain.

3. **One-Off Startup Seed Sources Polling Overhead**:
   Startup seed sources (`kline_seed_*`, `continuous_seed_*`, `reference_price_seed`, `rpi_depth_snapshot`) with `cadence_seconds == 0` are designed to execute once on process startup. Previously, they remained in the active `sources` table indefinitely, being repeatedly iterated, sorted, and evaluated by `_poll_rest_loop` and `_observe_availability` every second.

4. **Integrity Risk of Missing Closed Bars on Disconnect**:
   If unclosed frames are filtered and only closed bars (`k.x == True`) are persisted, a network disconnection or WebSocket reconnect spanning an expected close boundary could miss the single closed-bar push for that interval, leaving an unrecovered gap in the historical series.

---

## 2. Decision and Architecture

### 2.1 Closed-Bar WebSocket Persistence & In-Memory Forming Filter

Under configuration `kline_forming_policy = "closed_only"` (the production default):

1. **Valid Forming Bar (`k.x == False`)**:
   - The message is validated by `envelope_for_group()` and tagged in `source_sequence` with `klineClosed = 0`, `openTime = t`, `closeTime = T`.
   - On the capture path in `SelectedAssetCapture`:
     - The source's in-memory alive timestamp (`state.last_success`) is refreshed to the frame's receive timestamp.
     - The source's availability entry is acknowledged as valid.
     - The source's `forming_filtered_count` metric is incremented.
     - The frame is **not** enqueued into `StreamSpool` (zero Raw disk I/O).
2. **Valid Closed Bar (`k.x == True`)**:
   - Tagged in `source_sequence` with `klineClosed = 1`, `openTime = t`, `closeTime = T`.
   - Normal enqueue into `StreamSpool` and Raw persistence.
   - Updates `state.last_closed_open_time_ms = t`.
3. **Malformed Frames**:
   - If a K-line frame lacks required fields, has non-boolean `x`, invalid timestamp types, or open time exceeding close time, it is **never** silently dropped as an unclosed bar.
   - It is tagged with `capture_flags = ("malformed",)`, persisted into Raw, and reported as a source failure in `AvailabilityJournal`.

An optional configuration setting `kline_forming_policy = "all"` is retained for research configurations requiring intermediate intra-bar trajectory capture.

### 2.2 Direct Deduplication of `S12` (`@+08:00`)

By default, `S12` only subscribes to the 6 intervals whose boundaries genuinely differ between UTC and UTC+8:
```python
SPOT_UTC8_INTERVALS = ("6h", "12h", "1d", "3d", "1w", "1M")
```
The other 10 intraday intervals (`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`) are mathematically identical to `S11` and are explicitly documented as covered by `S11`. Source reduction depends on the discovered Spot product count (10 WS and 10 seed sources per Spot product).

### 2.3 One-Off Seed Source Retirement

In `SelectedAssetCapture`:
1. `_active_sources` and `_completed_seed_sources` hold source state; the standard-library `ChainMap` exposes their combined status and lookup view.
2. When a source with `cadence_seconds == 0` successfully completes and persists its response, it is moved from `_active_sources` to `_completed_seed_sources`.
3. Routine per-second tasks (`_observe_availability` and REST polling due evaluation) iterate only over `_active_sources`.
4. Overall coverage reports, status queries (`status()`), and dictionary lookups continue to include `_completed_seed_sources`.
5. Only already-closed K-lines returned by seed sources seed `last_closed_open_time_ms` for the corresponding live stream, including `continuous_seed_*`. An on-demand RPI snapshot request reactivates its retired seed.

### 2.4 Automatic Bounded REST Backfill for Missed Closed Bars

The existing REST scheduler also owns low-priority repair pages:

1. A closed-bar jump or reconnect beyond the next expected close boundary merges a missing range into the source's in-memory pending cursor. The first observed forming bar supplies the starting boundary when no closed bar has arrived yet. Stale closed frames never rewind the cursor.
2. Ordinary polls are admitted first. Repairs share `rest_concurrency`, `shared_request_budget()`, and the existing 418/429 cooldown handling. Each turn requests at most 100 bars (five monthly bars, and at most 200 days for longer intervals), with explicit start/end bounds excluding forming bars. UTC+8 monthly arithmetic preserves local month boundaries; continuous contract routing preserves `CURRENT_QUARTER` and `NEXT_QUARTER`.
3. Each response, including failures, is persisted once to the existing `rest_observations` Raw stream with its logical source identity, `closed_bar_backfill` flag and true REST receive timestamps. Binary Raw v1 is unchanged.
4. Only a contiguous, closed returned prefix advances the pending cursor. Empty, failed or incomplete responses retain the missing range and retry after five seconds; later gap discoveries extend the pending range. Status exposes `backfill_start_ms` and `backfill_end_ms` until complete. A REST outage is not declared repaired merely because a newer WS close arrived.
5. Storage exceptions propagate to the existing supervisor. Shutdown cancels requests still waiting for admission and drains requests already on the wire before closing spools. No separate repair service, task-per-source loop, or disk verification loop is added.

Pending repair cursors are process-local. Seed observations establish the new process baseline; this milestone does not add historical gap recovery across process restarts. Availability tracks current transport/observation health independently of whether an earlier missing range has finished repairing.

---

## 3. Invariants and Compatibility

1. **Raw v1 Binary Format**: All disk-level structures remain 100% compliant with `ADR-0010`. No parser, schema, or golden Go validator changes are required.
2. **Honest Continuity Auditing**: Filtered forming bars are recorded in `forming_filtered_count` metrics and maintain source validity in memory. Missed bars recovered via REST carry explicit `closed_bar_backfill` flags.
3. **Budget and Resource Boundedness**: Backfill requests share the existing global rate limiter, preventing bursts or HTTP 429 bans upon reconnection.
