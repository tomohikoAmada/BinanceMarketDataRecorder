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
   `S12` (`@+08:00`) subscribed to all 16 Spot K-line intervals. However, any interval that evenly divides 8 hours (`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`) has mathematically identical open and close timestamp boundaries in UTC (`S11`) and UTC+8 (`S12`). Subscribing to these 10 identical intervals in both timezones doubled WebSocket connections and Raw chunks for zero analytical gain (for `BTC + ETH`, ~600 redundant sources).

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
The other 10 intraday intervals (`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`) are mathematically identical to `S11` and are explicitly documented as covered by `S11`. This eliminates ~600 redundant sources for dual-asset (`BTC + ETH`) capture.

### 2.3 One-Off Seed Source Retirement

In `SelectedAssetCapture`:
1. `self.sources` is organized via `SourcesView` managing `_active_sources` and `_completed_seed_sources`.
2. When a source with `cadence_seconds == 0` successfully completes and persists its response, it is moved from `_active_sources` to `_completed_seed_sources`.
3. Routine per-second tasks (`_observe_availability` and REST polling due evaluation) iterate only over `_active_sources`.
4. Overall coverage reports, status queries (`status()`), and dictionary lookups continue to include `_completed_seed_sources`.
5. Initial K-lines returned by seed sources seed the `last_closed_open_time_ms` for the corresponding live WebSocket stream.

### 2.4 Automatic Bounded REST Backfill for Missed Closed Bars

To guarantee a gapless closed K-line sequence:

1. **Gap Detection Triggers**:
   - **Sequence Discontinuity**: When a newly received closed bar has `openTime = t` greater than `last_closed_open_time_ms + interval_duration_ms`.
   - **Reconnect Past Boundary**: When a WebSocket connection reconnects (`lifecycle("connected")`), and the current wall clock exceeds the expected close boundary of the next pending bar (`now_ms > expected_close_ms`).
2. **Bounded REST Fetch**:
   - The capture loop schedules an asynchronous, non-blocking background repair task using `shared_request_budget()`.
   - It requests missing bars from the appropriate exchange endpoint (`/api/v3/klines`, `/fapi/v1/klines`, `/continuousKlines`, etc.) bounded by `startTime = expected_next_open_ms` and `endTime = t - 1` (or `now_ms`).
   - Responses are verified, tagged with `capture_flags = ("closed_bar_backfill",)` and authentic REST receive timestamps, and persisted to `rest_observations` in Raw v1 format.
   - Upon successful persistence, `last_closed_open_time_ms` is advanced and `AvailabilityJournal` is marked valid.

---

## 3. Invariants and Compatibility

1. **Raw v1 Binary Format**: All disk-level structures remain 100% compliant with `ADR-0010`. No parser, schema, or golden Go validator changes are required.
2. **Honest Continuity Auditing**: Filtered forming bars are recorded in `forming_filtered_count` metrics and maintain source validity in memory. Missed bars recovered via REST carry explicit `closed_bar_backfill` flags.
3. **Budget and Resource Boundedness**: Backfill requests share the existing global rate limiter, preventing bursts or HTTP 429 bans upon reconnection.
