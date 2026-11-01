# Binance Market Data Recorder — Production Milestone Plan (M1–M4)

> **Status Notice (November 2026)**: The previous October 26 – November 1 `P0–P6` milestone plan has been **ABOLISHED and SUPERSEDED** by this streamlined Python engineering and VPS qualification plan (`M1` → `M2` → `M3` → `M4`).
>
> All valid technical deliverables and lessons from `P0–P3` and `P4` diagnostic runs (`Run A/B/C` on `greencloud-tokyo-01`) are retained as the baseline for `M1`–`M4`. No language rewrite (Go/Rust/C++) is performed; all bottlenecks are resolved by fixing architectural amplification, SQLite/spool I/O frequency, source duplication, and over-defensive redundancy in Python 3.12.

---

## 1. Retained Baseline & Why the Previous Plan Was Superseded

### 1.1 Retained Foundation (Completed in P0–P3)
- **P0 (`113cdb3` / `43aab9f`)**: Merged local workspace and remote `main`, inventoried Spot / USD-M / COIN-M public sources, and recorded the isolated VPS benchmark baseline.
- **P1 (`b292d0d`, [`ADR-0052`](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md))**: Replaced per-frame full L2 orderbook reconstruction on the capture path with sequence-only continuity checks (`U/u/pu`), long-lived WebSocket receive tasks, and bounded vectored raw writes (`SpoolWriter.append_many`).
- **P2 (`1c1785c` / `464ed56`, [`ADR-0053`](adr/0053-single-rest-representation-and-required-information.md), [`ADR-0054`](adr/0054-d06-fixed-cadence-and-daily-trade-supplement.md), [`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md))**: Implemented multi-asset Spot/USD-M/COIN-M product discovery, single primary REST representations, measured D06 `historicalTrades` polling cadences (`2s` USDT / `3s` USDC / `30s` COIN-M) + independent daily trade file supplement, synchronous `Retry-After` gate ordering before persistence `await`, and owner-approved omission of dated premium-index klines ([issue #585](https://github.com/binance/binance-connector-python/issues/585)).
- **P3 (`30e8a4a` / `30ebbd6`, [`ADR-0051`](adr/0051-production-continuity-and-incremental-verification.md), [`ADR-0056`](adr/0056-source-recovery-and-bounded-background-verification.md))**: Implemented per-source availability interval tracking, clock-discontinuity boundary guards, and incremental new-file verification (`raw-independent.v2`).
- **ADR-0057 ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md))**: Owner-approved initial-release omission of USD-M dated public `adlQuantile` (which returns HTTP `400 / -1121` on listed dated futures) with `UNKNOWN` official applicability.

### 1.2 Root Causes Exposed by P4 Diagnostic Runs & Cross-Verification
During the November 1 P4 diagnostic runs (`Run A/B/C`) on `greencloud-tokyo-01` and subsequent deep architectural profiling, four primary bottlenecks and three layers of over-engineering were identified:

1. **60-Second Spool Rotation & Small-File Storm (`RotationPolicy.seconds = 60.0`)**:
   - Single-asset BTC opens ~80 active spool writers (`SelectedAssetCapture`), rotating every 60 seconds and generating **~80 sealed `.zst` files + 80 `.json` manifests per minute** (`874` sealed chunks in 55 minutes during P4 Run C; projected **115,200 `.zst` files + 115,200 manifests = 230,400 files/day** for BTC alone, and >400,000 files/day for BTC+ETH).
   - Every seal triggers synchronous SQLite `Catalog` transitions (`ACTIVE` → `SEALED`), Zstd compression, full decompression readback, dual SHA-256 digests, directory `fsync`, background subprocess verification, and archive copy/readback/delete transactions.
2. **SQLite `Catalog` Write Amplification & `AvailabilityJournal` Backlog**:
   - `Catalog` opens SQLite in WAL mode with `PRAGMA synchronous = FULL` under a single process-wide `threading.RLock()`, forcing OS `fsync` stalls on every small transaction while blocking concurrent async callers via `to_thread`.
   - `AvailabilityJournal.valid()` enqueues a checkpoint row for **every healthy source** (`2,233` sources for BTC) every 10 seconds (`~223 rows/s`), while `flush_availability_journal()` drains only `64` rows per pass (`~6.4 rows/s`), causing the `52,997`-row backlog that halted P4 Run C after 55 minutes.
3. **K-Line Source Explosion & Unretired Seed Sources (83% of All Sources)**:
   - Out of `2,233` BTC sources, **1,845 (82.6%) are K-line sources** (`S11`, `S12`, `D21`, `D22`, `D23`).
   - `S12` (`@kline_<interval>@+08:00`) subscribes to all 16 intervals across 15 Spot pairs (`240` WS streams + `240` REST seed sources = `480` sources), even though the **10 intervals that divide 8 hours** (`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`) have identical bucket boundaries to UTC (`S11`).
   - All `615` one-shot `kline_seed_*` REST sources remain in `SelectedAssetCapture.sources` permanently after completing their single startup request, getting scanned every 1 second by `_observe_availability()` and `due = sorted(...)`.
4. **Per-Frame Hot-Path Overhead**:
   - Every incoming frame runs strict Pydantic `EventEnvelope` validation, multiple `.model_copy()` allocations, and `_ImmutableEnvelopeSnapshot` deep copying (`_freeze_json_value`), while repeating 17 verbose ASCII keys in every frame's CBOR map (`582 B` metadata wrapper around a `140 B` `bookTicker` payload = `5.16x` uncompressed overhead).
   - Every REST persistence call in `SelectedAssetCapture._persist_rest()` re-parses `result.envelope.raw_payload` via `json.loads()` and `json.dumps()` and hops threads through `run_owned_blocking_call` (`asyncio.create_task(asyncio.to_thread(...))` + `asyncio.shield`).
5. **Over-Defensive Multi-Pass Verification & Codebase Bloat**:
   - The same chunk is hashed during write (`_raw_digest` + per-frame `CRC32C`), compressed with Zstd checksum, immediately decompressed and hashed again in `seal_chunk_file()`, then decompressed and CBOR-re-encoded frame-by-frame (`_canonical_cbor(value) != body`) in a background `verification_worker` subprocess (`raw-independent.v2`), and hashed again during archive copy and deletion.
   - Every startup calls `seal_capture_startup()` across all active writers, creating dozens of 0-record `.zst` chunks and manifests before any market data arrives.
   - `src/` carries ~21,500+ lines of legacy single-symbol collectors (`SpotCollector`, `UsdMCollector`, `UsdMSideDataManager`), retired V5 acceptance harnesses (`service/acceptance*.py`), legacy reconnect auditors (`audit/reconnect_boundaries.py`, `spool/legacy_reconnect.py`), and a 5,106-line monolithic `Catalog` class.

---

## 2. New Milestone Plan Overview (`M1` → `M2` → `M3` → `M4`)

```text
M1: Primary Bottleneck Remediation (Spool Rotation, SQLite/Journal, K-Line Dedup, Hot-Path CPU)
  -> M2: De-Overengineering & Codebase Consolidation (Verification Streamlining, Legacy Removal, Catalog Split)
  -> M3: Local Benchmark & Stability Convergence (A/B Metrics, Regression Suite, Deployment Freeze)
  -> M4: GreenCloud VPS Staged Qualification (Continuous 2h -> 12h -> 24h Run)
```

---

## M1 — Primary Performance & Resource Bottleneck Remediation

### Goal
Eliminate the file-seal storm, SQLite `AvailabilityJournal` backlog, redundant K-line source explosion, and hot-path Python object churn so the Recorder runs with low, steady CPU and IOPS on a shared VPS.

### Tasks
- [ ] **M1.1 — Spool Rotation & Batching Policy (`spool/stream.py`, `config/models.py`)**:
  - Change default `RotationPolicy.seconds` from `60.0` to `600.0`–`900.0` (10–15 minutes) with `max_bytes = 128 * 1024 * 1024` (128 MiB), while keeping `60.0` configurable for unit tests that explicitly test fast rotation.
  - Retain deterministic per-stream rotation phase staggering (`rotation_phase_offset_ns`) so active writers never seal simultaneously at `T + 600s`.
  - Expected impact: reduces sealed `.zst` + `.json` file creation, SQLite seal transactions, and archive file count by **90%–93%** (from ~230,400 files/day to ~15,000–23,000 files/day for BTC).
- [ ] **M1.2 — SQLite `Catalog` & `AvailabilityJournal` Write De-Amplification (`storage/catalog.py`, `collector/availability.py`, `collector/coverage.py`)**:
  - Change `Catalog` WAL mode pragma from `PRAGMA synchronous = FULL` to `PRAGMA synchronous = NORMAL` (and `PRAGMA temp_store = MEMORY`, `PRAGMA mmap_size = 268435456`), eliminating per-commit disk `fsync` stalls while preserving WAL crash consistency.
  - Refactor `AvailabilityJournal`:
    - **Do not** append per-source SQLite rows every 10 seconds when a source remains continuously `VALID` / healthy.
    - Emit per-source `source_availability_journal` rows **only on state transitions** (`UNAVAILABLE` start, `RECOVERY` close, `FAILURE` observation, and `LIFECYCLE` start/stop).
    - Track steady-state liveness across healthy sources via a single process-level high-water heartbeat timestamp (`capture_runtime_heartbeat` / `coverage_checkpoint`) updated once per monitor pass.
  - Expected impact: reduces `AvailabilityJournal` SQLite row writes from **~223 rows/sec to <0.1 rows/sec** during normal operation, permanently eliminating the 52,997-row backlog observed in P4 Run C.
- [ ] **M1.3 — K-Line Source Deduplication, Closed-Bar Persistence & One-Shot Seed Retirement (`binance/public_sources.py`, `collector/coverage.py`)**:
  - In `S12` (`spot_kline_utc8`, `@kline_<interval>@+08:00`), remove the 10 sub-8h intervals whose bucket boundaries are mathematically identical to UTC (`S11`): `1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`. Retain only the 6 intervals where `UTC+8` alignment differs from UTC (`6h, 12h, 1d, 3d, 1w, 1M`).
  - **WebSocket K-Line Closed-Bar Filter (Owner Decision)**: For WebSocket K-line streams (`S11`, `S12`, `D21`, `D22`, `D23`), update in-memory source liveness/availability timestamps on every valid incoming WebSocket push (`k.x == False` or `k.x == True`) so long-interval streams (`4h`–`1M`) maintain continuous liveness proof, but **only persist closed K-line bars (`k.x == True`)** (with `1s`/`1m` configurable if real-time forming bars are ever desired). This eliminates >95% of redundant forming-bar disk writes and CBOR serializations while preserving full bar history and liveness monitoring.
  - Once a one-shot `kline_seed_*` REST source completes its initial seed persisted response, transition it to a retired/completed set so it is no longer iterated every second in `SelectedAssetCapture._poll_rest_loop()` (`due = sorted(...)`) or `_observe_availability()`.
  - Expected impact: immediately removes **300 redundant sources per asset** (`600` sources across `BTC + ETH`), removes **1,230 completed seed sources** (`BTC + ETH`) from the per-second scheduler loop, and cuts K-line frame persistence volume by **>95%**.
- [ ] **M1.4 — Per-Frame Hot-Path CPU & Allocation Streamlining (Preserving Raw v1 Format Compatibility) (`domain/event.py`, `spool/writer.py`, `spool/format.py`, `collector/coverage.py`)**:
  - **Preserve Raw v1 on-disk format 100%** (so all existing readers, Parquet normalizers, replay consumers, Go golden verifiers, and historical chunks remain 100% compatible without migration).
  - Construct `EventEnvelope` on the hot capture path via `model_construct()` (or a lightweight `@dataclass(slots=True)`) and replace `.model_copy(update=...)` in `SpoolWriter.append_many()` with direct construction or fast field assignment before serialization.
  - Remove `_ImmutableEnvelopeSnapshot` deep-copying (`_freeze_json_value`) on every frame; compute incremental seal statistics directly from the frame being serialized in `append_many()`.
  - In `SelectedAssetCapture._persist_rest()`, pass pre-parsed metadata from `RestCaptureResult` instead of calling `json.loads(result.envelope.raw_payload.decode("utf-8"))` a second time, and batch REST persistence writes per poll cycle rather than hopping threads once per REST source.

### M1 Acceptance Criteria
- All unit and contract tests pass.
- In a 10-minute local/synthetic BTC capture run:
  - `AvailabilityJournal` pending backlog stays `0` (`<= 64` transient);
  - SQLite transaction rate drops by `>90%`;
  - Active scheduler loop excludes completed `kline_seed_*` sources and duplicate `S12` intervals;
  - Hot-path per-frame CPU time in `hotpath_bench.py` / `compare.py` drops by `>= 40%`.

---

## M2 — De-Overengineering, Verification Simplification & Codebase Consolidation

### Goal
Remove redundant multi-pass verification/hashing, startup zero-frame file storms, and obsolete legacy modules so the codebase is low-coupling, high-cohesion, and maintainable.

### Tasks
- [ ] **M2.1 — Streamline Local Verification & Eliminate Redundant Re-Encoding (`spool/seal.py`, `spool/format.py`, `service/verification_worker.py`)**:
  - In `spool/format.py` (`decode_frame`), remove the mandatory `_canonical_cbor(value) != body` re-encoding check on every read frame; `CRC32C` already guarantees frame byte integrity.
  - Consolidate same-host seal verification: `seal_chunk_file()` already verifies the uncompressed digest, compresses with Zstd checksum, and verifies the compressed `.zst` output on seal. Make the incremental verifier (`raw-independent.v2`) verify compressed file size, SHA-256, Zstd frame integrity/CRC, and manifest metadata without re-encoding every frame's CBOR dictionary in a separate subprocess, or reuse the seal-time verification proof when the file descriptor/inode is unchanged.
  - Keep full target readback and SHA-256 verification across trust boundaries (when archiving to an external volume or remote archive target).
- [ ] **M2.2 — Eliminate Startup Zero-Frame Chunk Storms (`collector/coverage.py`, `spool/stream.py`)**:
  - Replace `seal_capture_startup()`'s creation of empty 0-record `.zst` files + `.json` manifests across all 80 writers on startup with a single `Catalog` startup boundary record (`capture_startup` lifecycle event in SQLite), avoiding 80 empty `.zst` files on every service start.
- [ ] **M2.3 — Unify Collector Architecture & Retire Obsolete Modules (`collector/`, `service/`, `spool/`, `audit/`)**:
  - Decouple `SelectedAssetCapture` (`collector/coverage.py`) from `UsdMStreamCollector` so Spot, USD-M, and COIN-M WebSocket connections use a clean, market-agnostic `PublicWebSocketStream` transport without routing Spot/COIN-M through `binance/usdm/websocket.py`.
  - Retire and remove dead/superseded code paths (while keeping public CLI entry points and contract-tested schemas intact):
    - Legacy single-symbol collectors (`collector/spot.py`, `collector/usdm.py`, `collector/usdm_side.py`) after migrating any remaining unit test fixtures to the unified capture path;
    - Retired V1–V5 whole-history online acceptance harnesses (`service/acceptance.py`, `service/acceptance_v5.py`, `service/acceptance_Qualification*`, `audit/reconnect_boundaries.py`, `spool/legacy_reconnect.py`).
- [ ] **M2.4 — Modularize `Catalog` (`storage/catalog.py`)**:
  - Split the 5,106-line `Catalog` class into focused repository mixins/modules under `storage/` (`chunks`, `availability`, `archive_tx`, `normalization`, `schema`) sharing the same SQLite WAL connection and lock, improving cohesion and readability without changing the SQLite schema.

### M2 Acceptance Criteria
- Zero 0-record `.zst` files created on clean startup.
- Background verification CPU/I/O overhead reduced by `>80%` with all corruption/truncation fault-injection tests still failing closed.
- Net reduction of `>= 15,000` lines of redundant/legacy source and test code, with `tests/verify_m0_contracts.py`, `ruff`, `mypy`, and the test suite passing.

---

## M3 — Local Benchmark & Pre-VPS Stability Convergence

### Goal
Prove with reproducible local A/B measurements and fault-injection tests that the `M1` + `M2` build achieves the expected resource reductions and 100% data integrity before deploying to the VPS.

### Tasks
- [ ] **M3.1 — Fixed-Input A/B Performance Benchmark**:
  - Run `hotpath_bench.py` and the 1-asset (`BTC`) / 2-asset (`BTC` + `ETH`) local steady-state benchmark (`compare.py`), recording:
    - Per-frame CPU (`μs/frame`) and memory allocation (`B/frame`);
    - Sealed file count per hour and SQLite write transactions per minute;
    - `AvailabilityJournal` max pending depth (`0` steady backlog);
    - Peak RSS and background verifier CPU share.
- [ ] **M3.2 — Data Integrity & Fault-Injection Regression Gate**:
  - Verify mid-frame truncation (`SIGKILL` simulation), sequence-gap recording, `429/418` `Retry-After` shared-pool cooldown, clock-discontinuity interval accounting, and archive copy/readback/delete safety.
- [ ] **M3.3 — Freeze VPS Deployment Configuration & Observation Scripts**:
  - Update `docs/milestones/P4/recorder.toml` and `observe.py` for the streamlined `M1`+`M2` build, ensuring capture metrics, journal depth, RSS, CPU%, disk runway, and archive backlog are sampled cleanly without interfering with the live writer.

### M3 Acceptance Criteria
- Local A/B benchmark confirms `>= 85%` reduction in sealed files/hour and SQLite transactions/min, and `>= 40%` reduction in hot-path CPU.
- Full test suite, `ruff check .`, `mypy`, and `python3.12 tests/verify_m0_contracts.py` pass on a clean worktree.

---

## M4 — GreenCloud VPS Staged Qualification (`BTC + ETH` Full Scope: `2h` → `12h` → `24h`)

### Goal
Deploy the frozen `M3` wheel to `greencloud-tokyo-01` configured directly for the target **`BTC + ETH` two-asset scope** (Spot quote pairs + USD-M perpetual/dated + COIN-M perpetual/dated) and qualify continuous real-network capture across **2 hours**, **12 hours**, and **24 hours** in one uninterrupted run with normal background verification and archiving enabled.

### Tasks
- [ ] **M4.1 — Preflight & `BTC + ETH` Deployment on `greencloud-tokyo-01`**:
  - Verify host preflight (`systemctl is-active` / `is-enabled`, disk free space above reserve, runtime-only quiet-window mask of unattended apt upgrades so `apt-daily-upgrade` cannot restart systemd mid-run).
  - Install the locked Python 3.12 wheel into an isolated environment with `selected_assets = ["BTC", "ETH"]` and record `T0` (`UTC`, `CLOCK_BOOTTIME`, `MainPID`, `InvocationID`).
- [ ] **M4.2 — Stage 1: Continuous 2-Hour Checkpoint (`T0 + 2h`, `BTC + ETH`)**:
  - Observe live `BTC + ETH` capture for 2 hours without stopping the service.
  - Gate criteria at `2h`:
    - Process `MainPID` and `InvocationID` unchanged (`NRestarts == 0`);
    - `AvailabilityJournal` pending backlog `< 100` rows;
    - Host average CPU within GreenCloud shared-KVM budget (`< 25%` sustained), RSS stable without unbounded growth, swap `0`;
    - All sealed chunks verified with `0` corruptions/errors; archive backlog bounded;
    - Per-source continuity `>= 99.5%` across `BTC + ETH` sources (or any external exchange/network gap honestly recorded and automatically recovered).
- [ ] **M4.3 — Stage 2: Continuous 12-Hour Checkpoint (`T0 + 12h`, `BTC + ETH`)**:
  - Keep the same `BTC + ETH` process running from `2h` through `12h`.
  - Gate criteria at `12h`:
    - Same `MainPID` (`NRestarts == 0`), flat RSS trajectory, zero SQLite/verifier/archive backlog accumulation, all sealed chunks verified, per-source continuity `>= 99.5%`.
- [ ] **M4.4 — Stage 3: Continuous 24-Hour Qualification & Planned Rotation (`T0 + 24h`, `BTC + ETH`)**:
  - Continue the same `BTC + ETH` process through `24h` (covering the UTC midnight boundary, 24-hour WebSocket planned session rotation, and daily trade supplement import).
  - Gate criteria at `24h`:
    - Clean 24-hour WebSocket session rotation with durable reconnect-boundary accounting;
    - Daily report (`report daily`) and incremental verifier (`0` failed files, `0` unverified backlog) complete cleanly;
    - Normal service stop/drain seals all active `.partial` files cleanly with `0` residual partial files.
