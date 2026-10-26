# Storage Forecast Live A/B Result

Classification: LIVE_AB_PASS

Final executor verdict: READY_FOR_INDEPENDENT_EVIDENCE_REVIEW

This is evidence only. No source code, commit, branch, PR, merge, GitHub
state, BBO work, next optimization, M22.9 work, or burn-in credit was
created by this run.

## Required result fields

LIVE_AUTHORITY_MATCH=YES

MAIN_SHA=138e3557d19b966f04da9f22cfda96144e86aa7e
MAIN_TREE=f21736d5e253385383c0a265bd508a981b907d8e

CANDIDATE_SHA=7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5
CANDIDATE_TREE=a4a1f4a145ca6aef98b61b82b47321484e84803b
CANDIDATE_PARENT=cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad

HOST=vps-b5bfe3f8 (vps-b5bfe3f8.vps.ovh.net)
VPS_CPU=2 vCPU; Intel Core Processor (Haswell, no TSX); x86_64
VPS_RAM=3911552 KiB (approximately 3.73 GiB); swap=none
DATA_ROOT=/var/lib/binance-market-data-recorder
ARCHIVE_ROOT=/srv/recorder-data/recorder-archive (ABSENT; no registered target)

BASE_SOURCE_SHA=138e3557d19b966f04da9f22cfda96144e86aa7e
BASE_SOURCE_TREE=f21736d5e253385383c0a265bd508a981b907d8e
BASE_WHEEL_SHA256=da7e2c9a5e9c04c0d05c4b25503e192ce17d9521195ccf3e3b0da2a97a6ad4ee
BASE_DEPLOYMENT_IDENTITY=61bda8460afa339f1f9d68d3eb54c77445bf15439b74979c5bd24a9201972ec9

CANDIDATE_SOURCE_SHA=7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5
CANDIDATE_SOURCE_TREE=a4a1f4a145ca6aef98b61b82b47321484e84803b
CANDIDATE_WHEEL_SHA256=a7b59906a81659ae0d99f9416d3005622914cd04c1dc9e38870a85442ac49429
CANDIDATE_DEPLOYMENT_IDENTITY=04a3b30c8b565d6350e2bd9ae24aef5a71623e0b2dfd677b77d7f9adbb95786b

LOCK_SHA256_MATCH=YES; both=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
CONFIG_SHA256_MATCH=YES; both=5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb
UNIT_SHA256_MATCH=YES; both=69f3c4c2c77a3e6fc4ee397d26ecb2927a741a6165024b2f6656727d0b398b83
DEPENDENCY_IDENTITY_MATCH=YES; same 28 locked distributions, pip check passed, no editable install

BASE_READY=YES
BASE_WARMUP_SECONDS=180.000191030 actual; planned=180
BASE_MEASURED_SECONDS=600 nominal; wall=600.000288005; BOOTTIME=600.000266665
BASE_PID_STABLE=YES; PID=570840; proc_starttime_ticks=129478288 at both boundaries
BASE_CPU_SECONDS=326.0799999999999
BASE_RAW_EVENTS=232797
BASE_RAW_PAYLOAD_BYTES=50015354
BASE_CPU_SEC_PER_MILLION_EVENTS=1400.705335549856
BASE_CPU_SEC_PER_GIB=7000.365007311953
BASE_EVENTS_PER_SEC=387.9948137592561
BASE_PAYLOAD_BYTES_PER_SEC=83358.88332037468

CANDIDATE_READY=YES
CANDIDATE_WARMUP_SECONDS=180.000187418 actual; planned=180
CANDIDATE_MEASURED_SECONDS=600 nominal; wall=600.000166452; BOOTTIME=600.000164235
CANDIDATE_PID_STABLE=YES; PID=654802; proc_starttime_ticks=129749180 at both boundaries
CANDIDATE_CPU_SECONDS=354.0
CANDIDATE_RAW_EVENTS=321053
CANDIDATE_RAW_PAYLOAD_BYTES=68306095
CANDIDATE_CPU_SEC_PER_MILLION_EVENTS=1102.621685516099
CANDIDATE_CPU_SEC_PER_GIB=5564.724578326429
CANDIDATE_EVENTS_PER_SEC=535.088184889169
CANDIDATE_PAYLOAD_BYTES_PER_SEC=113843.46008421396

CPU_PER_MILLION_DELTA_PERCENT=-21.280967700230
CPU_PER_GIB_DELTA_PERCENT=-20.508079614220

STREAM_MIX_COMPARABLE=YES_FOR_CORE_STREAM_SET_WITH_MATERIAL_VOLUME_DIFFERENCE
STREAM_MIX_NOTES=Both arms had Spot and USD-M core agg_trade, book_ticker, and diff_depth. Base Spot/UM event share was 24.657104688%/75.342895312%; candidate was 24.190398470%/75.809601530%. Candidate volume was +37.911141466600% Raw events and +36.570252007014% Raw payload bytes. Combined diff_depth was base 11880 events/16396158 bytes (5.103158546%/32.782249227%) and candidate 11882 events/21247224 bytes (3.700946573%/31.105897651%). USD-M book_ticker dominated both: base 163557 events/26781042 bytes; candidate 228617 events/37446011 bytes. Candidate also recorded 13 liquidation events; base recorded none in-window. Candidate had one USD-M side-data basis_5m REST 418/rate-limit failure; core collectors remained active.

BASE_CAPACITY_MONITOR_HEALTH=PASS; 103 observations; expected cadence=5.0s; max gap=8.625827303s; hard reserve not reached; forecast state=EMERGENCY
CANDIDATE_CAPACITY_MONITOR_HEALTH=PASS; 103 observer observations; expected cadence=5.0s; max gap=10.929403423s; hard reserve not reached; forecast state=EMERGENCY
CANDIDATE_SQLITE_TRANSACTION_ERRORS=0

BASE_BACKPRESSURE=NONE_OBSERVED
CANDIDATE_BACKPRESSURE=NONE_OBSERVED
BASE_CORE_GAPS=0
CANDIDATE_CORE_GAPS=0
BASE_RESYNCS=0
CANDIDATE_RESYNCS=0
BASE_UNEXPECTED_RESTARTS=0
CANDIDATE_UNEXPECTED_RESTARTS=0
BASE_OOM=NO
CANDIDATE_OOM=NO
BASE_TERMINAL_FAILURE=NO
CANDIDATE_TERMINAL_FAILURE=NO

BASE_CATALOG_INTEGRITY=PASS; read-only query_only audit; PRAGMA integrity_check=ok; path health PASS
CANDIDATE_CATALOG_INTEGRITY=PASS; read-only query_only audit; PRAGMA integrity_check=ok; path health PASS
BASE_ARCHIVE_HEALTH=OK_NO_REGISTERED_TARGET; 51679 unarchived files / 7319924677 bytes; zero archive transactions
CANDIDATE_ARCHIVE_HEALTH=OK_NO_REGISTERED_TARGET; 51809 unarchived files / 7342184155 bytes; zero archive transactions

BASE_RSS_START_END=284971008 -> 285356032 bytes; max=285356032
CANDIDATE_RSS_START_END=274821120 -> 284155904 bytes; max=284155904
BASE_FD_START_END=42 -> 43; max=43
CANDIDATE_FD_START_END=44 -> 38; max=46
BASE_THREADS_START_END=8 -> 8; max=8
CANDIDATE_THREADS_START_END=8 -> 8; max=8

LIVE_AB_CLASSIFICATION=LIVE_AB_PASS
REGRESSION_SIGNAL=NO; both normalized CPU metrics improved; no candidate-only core, capacity-monitor, SQLite, Catalog, or stability regression
INTEGRITY_REGRESSION=NO

FINAL_SERVICE_RESTORED_TO_MAIN=YES
FINAL_RUNNING_SOURCE_SHA=138e3557d19b966f04da9f22cfda96144e86aa7e
FINAL_READY=YES
FINAL_ARCHIVE_HEALTH=OK; archive command transaction_count=0; no registered target; no archive timer/worker unit observed

EVIDENCE_ROOT=/opt/binance-market-data-recorder/m23-profiling/STORAGE-FORECAST-LIVE-AB-20260901T092228Z
EVIDENCE_ARCHIVE=/opt/binance-market-data-recorder/m23-profiling/STORAGE-FORECAST-LIVE-AB-20260901T092228Z.tar.gz
EVIDENCE_ARCHIVE_SHA256=17102d2858a96fda19d84ed67f8c47bc9a07aee60fd22d76ab96866ec1bfd804
RESULT_DOCUMENT=STORAGE_FORECAST_LIVE_AB_RESULT_20260901T110017Z.md

PR_CREATED=NO
MERGED=NO
BRANCH_DELETED=NO
BBO_STARTED=NO
NEXT_OPTIMIZATION_STARTED=NO

FINAL_EXECUTOR_VERDICT=READY_FOR_INDEPENDENT_EVIDENCE_REVIEW

## Authority

The live authority check was performed before VPS mutation using GitHub
ls-remote and the GitHub commit API. Main resolved to
138e3557d19b966f04da9f22cfda96144e86aa7e with tree
f21736d5e253385383c0a265bd508a981b907d8e. The candidate branch
perf/bounded-storage-forecast resolved to
7fa4ecc5720c3c33beabdf135dd39bfe4f3da0b5 with tree
a4a1f4a145ca6aef98b61b82b47321484e84803b and parent
cdeb895f55b85a8d2db63e8a1d85f3abc56e46ad. The candidate parent’s parent
was current main. No GitHub state changed.

## Measurement and denominator method

The existing current-main profiling campaign method was reused: process CPU
from Linux /proc/<pid>/stat utime plus stime divided by CLK_TCK=100, exact
monotonic/BOOTTIME boundaries, 60-second low-frequency resource/systemd
observations, service readiness, Catalog facts, and bounded journal checks.
No py-spy, perf, strace, or profiler was used.

Each arm had a planned 180-second warm-up followed by a strict 600-second
half-open window:

- Base: 2026-09-01T09:53:52.477034Z through
  2026-09-01T10:03:52.477322Z; UTC nanoseconds
  1788256432477033994 through 1788257032477321999.
- Candidate: 2026-09-01T10:23:39.750879Z through
  2026-09-01T10:33:39.751045Z; UTC nanoseconds
  1788258219750878782 through 1788258819751045234.

The primary Raw denominator was calculated after each planned normal stop and
Raw sealing. Every manifest metadata record was read, then only sealed chunks
whose manifest receive-time range overlapped the window were decoded. A
frame was included iff envelope.receive_time_utc_ns was in
[window_start_utc_ns, window_end_utc_ns). RAW_EVENTS counts every included
Recorder Raw-v1 frame, including duplicates. RAW_PAYLOAD_BYTES is the sum of
len(envelope.raw_payload) over the same frames, excluding Raw headers,
CBOR/envelope overhead, compression, and network framing. Selected file
size/SHA-256 matched the manifest and selected decoded frames passed Raw-v1
validation. Catalog SEALED transitions were retained as a cross-check, not
used as the primary denominator.

Seven old zero-record manifests had null receive-time ranges. The initial
analyzer flagged them; a separately preserved validation decoded all seven
files to zero frames with matching stored size/SHA-256. They were outside both
windows, cannot contribute to either denominator, and were excluded from
overlap selection. The final reviewed malformed-manifest count was zero.

## Complete Raw stream mix

The following are included Raw events and exact Raw payload bytes in each
window. A zero means that stream was not observed in that arm’s measured
window.

| market | stream | base events | base payload bytes | candidate events | candidate payload bytes |
|---|---|---:|---:|---:|---:|
| spot | agg_trade | 4,579 | 767,366 | 6,991 | 1,171,926 |
| spot | book_ticker | 46,824 | 5,010,948 | 64,673 | 6,920,011 |
| spot | diff_depth | 5,998 | 4,533,452 | 6,000 | 6,304,199 |
| um_perpetual | agg_trade | 5,324 | 891,680 | 8,267 | 1,385,706 |
| um_perpetual | basis_5m | 2 | 2,212 | 0 | 0 |
| um_perpetual | book_ticker | 163,557 | 26,781,042 | 228,617 | 37,446,011 |
| um_perpetual | diff_depth | 5,882 | 11,862,706 | 5,882 | 14,943,025 |
| um_perpetual | funding_history | 2 | 28,038 | 1 | 14,019 |
| um_perpetual | global_long_short_ratio_5m | 2 | 2,152 | 1 | 1,076 |
| um_perpetual | liquidation | 0 | 0 | 13 | 2,704 |
| um_perpetual | mark_price | 600 | 109,200 | 600 | 109,200 |
| um_perpetual | open_interest | 10 | 8,370 | 2 | 1,674 |
| um_perpetual | open_interest_statistics_5m | 2 | 2,238 | 1 | 1,119 |
| um_perpetual | premium_index_snapshot | 9 | 9,180 | 2 | 2,040 |
| um_perpetual | taker_buy_sell_volume_5m | 2 | 2,454 | 1 | 1,227 |
| um_perpetual | top_long_short_account_ratio_5m | 2 | 2,156 | 1 | 1,078 |
| um_perpetual | top_long_short_position_ratio_5m | 2 | 2,160 | 1 | 1,080 |

Event/message kinds were aggTrade for agg_trade, depthUpdate for diff_depth,
bookTicker for USD-M book_ticker, markPriceUpdate for mark_price, and
forceOrder for the 13 candidate liquidation events. Spot book_ticker and
side-data payloads reported no event-kind field. The full raw analyzer JSON
also retains the per-kind byte counts.

## Health, storage, and restoration notes

Both measured arms had zero operational core gap/resync events, no observed
backpressure, no unexpected systemd restart, OOM, terminal failure, Python
traceback, or SQLite transaction error. Active partials were zero after each
normal stop. The bounded Catalog audits were read-only and performed no
repair, delete, archive mutation, compaction, or VACUUM.

Both capacity monitors stayed alive and produced 103 observed samples against
the 5-second expected cadence. The host’s forecast state was EMERGENCY in
both windows, but actual_hard_reserve_reached was false and no emergency stop
occurred. No archive root, registered external target, archive worker, or
archive timer was present; the expected unarchived local backlog was not
deleted or mutated.

The candidate arm emitted one USD-M side-data RateLimitBanError for
basis_5m with a shared REST cooldown and explicitly retained core collectors.
This was reported as side-data/environmental variation and not counted as a
core market-data gap. After candidate evidence, the candidate virtual
environment was preserved at its identifiable path and current main was
restored. The first restoration readiness invocation encountered the same
external cooldown while reconnecting; one normal current-main restart then
returned READY. Final status showed both Spot and USD-M core collectors
READY/synchronized, Catalog accessible, NRestarts=0, and unchanged data,
config, and unit roots. The preserved e074d41 rollback artifact was not used.

The result is an adjacent two-window live observation, not a randomized
controlled trial. It does not establish statistical significance or a precise
whole-process CPU saving independent of traffic mix. No burn-in credit is
claimed.

No merge recommendation is made. The only handoff status is
READY_FOR_INDEPENDENT_EVIDENCE_REVIEW.
