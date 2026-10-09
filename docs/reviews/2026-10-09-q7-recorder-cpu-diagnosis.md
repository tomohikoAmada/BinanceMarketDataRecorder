# Q7 Recorder CPU diagnosis — October9, 2026

Actual VPS recorder CPU utilization is higher than the retained original Q4/Q5
runs. The strongest measured explanation is increased market-data volume,
primarily book-ticker messages. This is an observational comparison across
separate times, not a same-input code regression or optimization benchmark.

| VPS interval | Recorder CPU, one-core scale | Sealed frames/s | Decoded Raw MB/s |
| --- | ---: | ---: | ---: |
| Original Q4, source23ea5c4, 241 samples /7215.828s | 45.45% | 274.71 | 0.214 |
| Original Q5, source23ea5c4, 1439 samples /43234.683s | 52.38% | 323.07 | 0.251 |
| First optimized NONFORMAL, sourcee05268f, 52 samples /1538.818s | 112.65% | 759.43 | 0.570 |
| Corrected NONFORMAL, source49b4a2d, 77 samples /2284.733s | 75.96% | 672.79 | 0.506 |
| Current corrected Formal prefix, 128 samples /3826.530s | 95.63% | 812.80 | 0.606 |

CPU is the same-incarnation main process /proc utime+stime delta divided by
wall time, not lifetime ps percentage or host load average. Current Formal prefix
is October8 23:56:06.115135UTC→October9 00:59:52.644869UTC (Shanghai07:56→08:59).
The prefix includes several seconds before actual source T0; it diagnoses runtime
load and grants no accepted duration. Four virtual CPUs mean one-core100% is
roughly one-quarter of total nominal VPS capacity; Python writer threads can make
process utilization exceed100%.

The current interval has2.516× the Q5 sealed frame rate and2.415× decoded byte rate,
but1.825× Recorder CPU. Approximate CPU/record is1.177ms versus1.621ms (about27%
lower); this is a workload-normalized observation, not proof that changed code
caused an improvement. Different stream mix, frame sizes, market activity,
co-resident load and host scheduling remain confounders.

## Main measured workload change

| Archived/sealed stream, two configured symbols combined | Old Q5 frames/s | Current frames/s |
| --- | ---: | ---: |
| USD-M book ticker | 217.14 | 604.26 |
| Spot book ticker | 51.05 | 140.18 |
| USD-M diff depth | 19.54 | 19.64 |
| Spot diff depth | 19.27 | 19.94 |
| USD-M aggregate trade | 7.43 | 16.28 |
| Spot aggregate trade | 6.53 | 10.38 |

Book-ticker traffic268.19→744.44frames/s accounts for roughly97% of the increase
in total sealed-frame throughput. The configured four products/all14 auxiliary
features are retained, not an enlarged product set. Per-event Raw preparation,
encoding, queue handoff, writer work and metrics run more often when more quotes
arrive. The20-second thread sample distributes CPU between the main event-loop
thread (~52%) and several worker threads; it does not identify exact Python
function attribution. No intrusive profiler was attached.

## Recorder versus acceptance overhead

At October9 01:00UTC, a read-only20.092s /proc+cgroup sample measured:

- Recorder samePID260007/inv07803/NRestarts0:105.62% of one core;
  current cgroup swap0. Current service stateRUNNING/four ready products.
- Native observer inv609f:0% during that interval between observations.
  Its cumulative cgroup750.469CPU seconds over roughly3838seconds implies about
  19.6% of one core since dispatch, including short-lived verification children;
  instantaneous0% does not mean its full-period cost is zero.
- Read-only resource sampler inv0d4a:0.21%; automatic terminal waiter inv3948:0.04%.
  Archive worker was inactive for this short snapshot; its recurring cost is not
  asserted zero. No full terminal audit had started in this pre-target snapshot.
- Host/proc sample:22.18% busy,1.13% iowait,10.57% steal. Host and cgroup counters
  have separate measurement boundaries; they are not a perfectly synchronized
  accounting reconciliation. Steal is measurable shared-VPS contention, not
  evidence by itself that a server upgrade or language change is required.

The dominant CPU consumer in this short snapshot is Recorder itself, not the
lightweight sampler/waiter. Acceptance verification adds separate CPU bursts;
these must not be presented as Collector CPU or ordinary production overhead.
Original Q4/Q5 also ran acceptance, so observer cost alone does not establish a
new code regression.

## Code scope and evidence limits

The source23ea5c4→49b4a2d package diff touches CLI, normalization/replay,
checkpoint, Catalog and V5 acceptance. It does not change Collector transport,
Raw writer/queue/format, metrics or service-runtime source. Q6's demonstrated
large speedup applies to derived offline processing, not live ingestion or full
Raw audits. The Catalog adds a chunks state/creation index with measured bounded
commit cost; no changed per-event Collector loop was found. Checkpoint snapshot
reuse and causal admission changes affect their respective bounded operations.
This code review supports the traffic explanation but cannot exclude every
interaction or attribute CPU to individual functions without profiling/equal input.

Volume comparisons use committed sealed Catalog rows created within each CPU
interval, summed record_count/uncompressed_bytes/stored_bytes; all unique historic
rows remain retained. Active/unsealed tails and chunks crossing boundaries make
this an approximate arrival-throughput comparison. Archive-transactions join is
one-to-one by unique chunk_id and reproduces exact counted-frame totals; it reads
metadata only, no Raw. Queries were read-only/query_only/250ms busy timeout/
3–4s progress cutoff; the largest volume SQL cost was0.080s.

Metric-batch commit-time windows were deliberately rejected for arrival-rate
comparison: collector aggregates flush at rollover/shutdown, while archive
batches commit continuously. A0 accepted-event count in a commit-time window is
not evidence of no capture; the retained exploratory result preserves this
scope limitation.

The current Formal prefix's128 recorded auxiliary checks were PASS at read time;
this is not the final denominator, actual7200s target, terminal integrity or final
acceptance. No service/source/config/acceptance control was changed, no restart,
Raw scan, newT0 or test interruption occurred. Continue the existing own7200s
and automatic terminal. If an equal-input live-path regression is later suspected,
profile an isolated representative replay before choosing a patch/stack change.

Private diagnostic controls/output:
`/private/var/tmp/bmdr-q7-cpu-diagnostic-m7i7a1hi/` (actual.json,
historical-comparison.json, metric-threads-actual.json, volume-actual.json,
stream-volume-actual.json and exact read-only scripts).
Historical samples are the frozen Q5 full control bundle/predecessor Q4, original
Q7 NONFORMAL controls and corrected Q7 NONFORMAL controls. Current CPU diagnosis
is separately authorized by the owner's CPU question, not an hourly duplicate
poll of the healthy Formal phase. No new Formal acceptance credit is granted.
