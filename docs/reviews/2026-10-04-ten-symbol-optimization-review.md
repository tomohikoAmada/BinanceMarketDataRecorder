# Ten-symbol optimization review

Reviewed October 4, 2026; local measurements completed before 12:55 UTC.
Local source HEAD: `1f81eb79def8cc981a8edc869bfe42b5775a9a19`, macOS arm64,
Python 3.12.9. These are code inspection and synthetic offline prototypes,
not deployed patches or ten-symbol VPS qualification. Current Q5 is untouched.
The referenced [archive discussion](chatgpt-conversation://6ac24829-c7a0-83ee-9e95-1b34dbc174a7)
is context; its claims were checked against repository code and evidence.

## Scope and actual scaling

Assume ten operator-selected symbols in **each** market: 20 ProductKeys versus
the current four. A single-market selection would instead have ten ProductKeys.
The existing product topology already supports finite configured lists; no
exchange framework, automatic discovery or new process topology is needed.

With the current all-enabled feature set, code-derived counts are:

| Component | Current two symbols per market | Ten symbols per market |
|---|---:|---:|
| ProductKeys / core logical WS streams | 4 / 12 | 20 / 60 |
| Auxiliary owners | 26 | 122 |
| Auxiliary REST owners / WS streams | 22 / 4 | 102 / 20 |
| Five-minute REST cursors | 12 | 60 |

USD-M global fundingInfo/exchangeInfo remain two process-global owners, not
twenty copies. Spot exchangeInfo is currently product-specific and requests
exactly that symbol; changing it to a batch changes Raw/provenance semantics.
Owner counts are not requests/second, event rates, socket counts or CPU estimates.
See `service/runtime.py`, `collector/usdm_side_data.py`,
`binance/usdm/side_data_rest.py` and `binance/spot/exchange_info.py`.

Important admission limitation: `tools/qualification_auxiliary_gate.py` explicitly
pins BTCUSDT/ETHUSDT in both markets and reports 26 contexts. It will reject a
ten-symbol profile. Future expansion needs a reviewed profile-bound gate with
derived owner/cursor expectations while preserving the existing frozen gate.
Passing four-product Q7–Q10 does not certify twenty-product capacity.

MS3's 14-product/42-stream fixtures cover isolation, backpressure, ownership,
shared gate and archive behavior. They are correctness evidence, not proof of
real sustained twenty-product arrival rates or VPS headroom.

## Confirmed reductions in local prototypes

All comparisons below used unchanged source and temporary files/SQLite only.
Timings are median wall seconds, five repetitions for reader/SQL and nine for
snapshot/winner. No full Recorder speedup is inferred.

| Candidate | Actual reduction / observed timing | Safety boundary |
|---|---|---|
| Buffer temporary NDJSON line reads | 2,000 rows / 2,054,890 bytes: instrumented FileIO read calls 2,054,891 → 33 with a 64 KiB buffer; actual `_read_documents` 0.844345s → 0.004428s, all rows equal. | Scratch files only; bound aggregate reader memory and close/flush writers before reading or eviction. Keep malformed-row rejection. Persistent Raw/fsync semantics are unchanged. Buffered scratch writes remain a separately measured candidate. |
| Index oldest unowned SEALED selection | Actual Catalog schema with 20,000 LOCAL_DELETED + 50 SEALED rows: full chunks scan/temp ordering → indexed state lookup, same oldest tied row. Twenty actual method calls 0.009155s → 0.000159s. | Additive `(state, created_at_utc_ns, chunk_id)` index; preserve local/remote exclusion and transaction validation. Production migration/write cost, owned-row/concurrent transition cases and remote history cost still need validation. |
| One Checkpoint book snapshot/hash | 2,000 levels per side: three canonical mappings → one; two logical hashes → one; 0.005964s → 0.002194s. Complete mapping and both returned identities equal. | Freeze one save's snapshot and derive all file/Catalog identity from it, including market/symbol/update ID. Preserve the distinct checkpoint-document hash and restore verification. Construction timing excludes file/SQL commits. |
| Reuse ordered dedup variants | 200 variants / 3,095 candidates, fixed random seed 20261004: 3,095 sorting-key evaluations and copied sorted lists → zero repeated sorting; 0.003844s → 0.003433s. Entire output, all provenance, duplicate counts and conflict flags equal. | Applies only after full internal sort-key validation and sorting. Preserve stable tie order, product/stream identity and complete duplicate-source output. |

These establish local work reductions and compared-result equivalence. An
implementation still needs its targeted regression/fault tests and representative
measurements before release. The latter three are not universally faster for
every workload; index writes and buffering consume resources. Checkpoint,
normalization/replay scratch and dedup primarily improve derived/offline work;
they do not establish faster live WS capture.

## High-value candidates requiring additional evidence

1. **Fuse stored hash and decoded validation within one required read.**
   `spool/seal.py:validate_sealed_artifact` currently reads the compressed artifact
   for its stored hash, then again for decoded identity. V5 Raw qualification has
   analogous separate stored/decoded passes. A correct combined validator can
   reduce reads while keeping both hashes, sizes, CRC/canonical/frame statistics
   and sequencing. Prove complete compressed-byte accounting, EOF/read-ahead,
   concatenated/trailing data, short reads, truncation, cancellation and
   path/fd/mutation behavior. It does not remove either independent LIVE pass.
2. **Consolidate repeated archive validation within a controlled session.**
   On ArchiveManager's fresh success path, reserve, copy and verify each call
   full source bundle validation: six compressed-source read passes, plus one
   copy read and one pre-delete stored-hash read. Target readback and pre-delete
   commit validation add two target reads. These are path-specific static counts,
   not measured I/O or evidence that every pass is redundant. Retry/resume and
   boundary mutations must still be detected; copy/write hashes cannot replace
   post-fsync full readback. Remote transfer has additional receipt/deletion
   boundaries and requires its own review.
3. **Bound normalization merge fan-in (F4).** `_external_sort` opens every run;
   replay already caps fan-in at 32. A hierarchical merge can bound descriptors
   and buffers for growing input, but adds I/O and may slow small jobs. Preserve
   exact global ordering, dedup/provenance and failure cleanup.
4. **Reduce chunk fixed costs with a measured rotation policy.** Defaults are
   60s/128 MiB. Under continuously active, time-triggered core streams, 60 streams
   imply roughly 3,600 chunks/hour before auxiliary/boundary/size-triggered chunks.
   Longer or stream-specific periods can reduce manifests, Catalog commits and
   seal/archive setup. Existing fsync scheduling is separate, but that alone does
   not prove unchanged safety: measure active reserve, compression staging,
   recovery duration, delayed archival and boundary semantics. Preserve <=1s
   configured durability and size/reserve limits. Do not change active Q5.
5. **Make archive capacity admission reflect sustained arrival and recovery.**
   Timer defaults are 60s cadence, 50s worker budget, plus scheduling jitter;
   drain is single-owner and bounded. Measure actual bytes/files retired per
   wall second, ingest, backlog slope, co-resident I/O and outage recovery. An
   adaptive duty cycle may help, but blindly raising workers/budgets can harm
   capture or grow temp space. `mu > lambda` during normal operation does not
   guarantee capacity during outages, audit pauses or storage failure; a fixed
   2x margin is a proposal, not an established safety gate.
6. **Measure REST wait, Catalog history cost and aggregate queue pressure.**
   Shared USD-M lock/cooldown and bounded queues already exist. Measure core
   snapshot wait versus side-data/catch-up wait, request age, capture lag,
   per-stream occupancy and total payload memory. Evaluate a scheduler only for
   proven contention; preserve cooldown, cancellation, fairness and endpoint
   semantics using current official sources. Catalog remote-schema reads also
   validate all remote rows before candidate selection; an index alone does not
   bound that history cost. Snapshot-bound summaries/cursors need corruption,
   mutation and ownership tests. Keep SQLite WAL/FULL and current writer authority.

Ordinary V5 observations already consume bounded deltas, and completed verify
replays frozen proofs. Do not call these missing optimizations or use historical
VERIFIED state/unchanged timestamps to omit required full terminal checks. Review
incremental normalization and Decimal reuse under the same output/mutation gates
already listed in Q6. WS aggregation, broader parallelism and hardware/native
rewrites remain conditional on a measured bottleneck.

## Before ten-symbol production admission

Use the selected symbol mix and real payload-size/rate distributions, not a
fivefold extrapolation from BTC/ETH or tiny synthetic events. Compare ordinary,
burst, synchronized resync/seal, auxiliary catch-up and archive recovery loads.
Measure Recorder plus observer/archive CPU/RSS, event-loop lag, queues, persisted
lag/gaps, REST waits, transaction/WAL costs, fd limits, temp/reserve and complete
full-audit windows over the retained growing corpus. Keep the existing bounded
observer work/deadline rules; measure admission rather than simply multiplying
them by five. Reforecast R-078 and the unresolved prior 38h reserve scenario.

Checks run on unchanged production code: 32 existing offline tests passed in
2.48s across Checkpoint, normalization pipeline, MS3 multi-product/archive and
USD-M shared REST gate. These are baseline regressions, not tests of a shipped
optimization. No online endpoint, VPS command, deployment, real external-volume
test, ten-symbol live test or full-suite/CI rerun was performed for this review.
The performance/SHA policy is recorded in `AGENTS.md`; implementation remains
after current Q5's full closeout as planned in Q6.

The owner subsequently requested explicit Q6 tests and an adoption decision for
all five directions (combined hash/decompression, small chunks, bounded merge,
archive duty cycle and REST queues). The current milestone plan records those
tests and requires a practical workload benefit: unsafe/unstable candidates are
rejected regardless of speed; candidates with negligible performance and resource
benefits retain the original implementation. Record measured rejection/deferral
as an evaluation outcome rather than forcing every proposed change into release.
