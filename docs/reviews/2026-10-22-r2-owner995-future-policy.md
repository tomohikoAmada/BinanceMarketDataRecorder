# Independent future N2 owner995 private-control review

Verdict: **ACCEPT_UNBOUND_POLICY_IMPLEMENTATION_LOCAL_OFFLINE**. No remaining blocking code finding in the reviewed policy scope. This is not a physical-binding, execution, N2 admission, ARM, own24h, saved-data, notification-delivery or restoration acceptance. N1 independent final/restored receipt/main must precede any new N2 operation.

The human-authorized scope is retained: BTCUSDT N1 first; then BTC+ETH N2 with Spot+USD-M/all14 and its own86400s; nested4/8 and smaller steps near measured capacity/headroom boundary. No transferred time, production source/wheel rebuild, or main/R3 deployment.

## Exact final bytes

Reviewed parent draft: `/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/capacity-n2-owner995-UNBOUND-20261022`.

Immutable reviewed snapshot: `candidate-v3-final/` under this report directory.

- controller.py: `cde4cf06166d00d8cd5c0ce4559c346f36dbcb1eddb4df872d7ee85f9d8f8bbd`
- qualification_owner_995.py: `187224ed556f2f9f3664095b4ee34aa83be76ac57619a5263abde55b25afe608`
- qualification_scoped_corpus.py: `2ce6d5815e9e6fe507382d7f0d143e2811062de9e0fb5af7ce6d731f8fc1501d`

The owner995 module and causal selector match their W/tools sources byte-for-byte. The initial MANIFEST_UNBOUND.json is a retained draft, not an effective freeze; this review's own input/final manifests identify the actual bytes. Common/config/identity/cohort/forecast/unit/scope remain old N1 placeholders by design and are not admitted for execution. The original N1 13 helper files retain their initial hashes. The reviewer changed only files in this exclusive review directory.

## Findings fixed before acceptance

1. The initial new write-safety check rejected a legitimate STARTING publication with capacity=None before production startup recovery/census had completed. The frozen release publishes that state before its first capacity census. Final code allows it only while no owner-start exists and status is STARTING, records `write_safety_verified=False`, and still checks explicit service/shutdown/product failure first. The real strict auxiliary gate remains BLOCK, so that pending state cannot create T0. A formal missing capacity remains critical.
2. The initial finish loop accepted a first observation delayed61s beyond the promised60s finalization reserve. Final code durably retains/validates the actual observation, publishes `finalization-timing-failure.json`, and fails the timing control instead of fabricating a timely target. It retains the completed own duration/data and explicitly disallows automatic recapture. A clock-invalid delayed observation remains a clock failure; an older row cannot conceal it.
3. The initial fixed-slot evaluator never read utc_ns, accepting absent/bool/day-shifted UTC as100% PASS. Final code verifies integer UTC/BOOTTIME projection with the existing <2s bound, integer schedule, and integer skipped-slot evidence matching actual delay. Duplicate slots and contradictory gate records remain invalid.

All original draft snapshots, reproductions and outputs remain retained. No failed result was overwritten as a success.

## Accepted behavior and evidence

Fixed monitoring uses exactly2880 nominal30s slots in the own86400s interval. Missing and late slots are failed; duplicate/impossible evidence cannot produce a trusted percentage. Official allow_empty_recovery semantics remain unchanged, strict status stays separately visible, and probes/finish rows cannot dilute the denominator. Threshold comparison is inclusive integer995/1000. Saved-frame conformance stays a separate full selected-data measurement.

Recoverable readiness/heartbeat/auxiliary degradation does not impose a single60s episode veto. A420s continuous core episode produced2866/2880PASS and continued into one audit dispatch. Core recovery while auxiliary freshness remained BLOCK also completed without the old cross-coupled veto. An actual2865/2880 below-threshold monitor result remained BLOCK and still dispatched the saved-data audit. There is no inherited first_bad+60 deadline that can create an overdue zero-sleep recovery loop.

The finish loop observes through at most minimum+60s, and can preserve a nonready last trusted endpoint. In a persistent BLOCK case, the last trusted row was minimum+55s and control returned at minimum+60s; both official and strict endpoint statuses remained BLOCK, while own target completion and audit dispatch remained separate. Thus the endpoint timestamp is the last trusted observation, not an invented observation at the instant of stop. Exactly+60s trusted observation was accepted; +61s was rejected as a timing-control failure. Finish rows did not change2880.

Runtime RSS, host available memory, current/lifetime swap remain recorded capacity diagnostics and do not vote in the monitor rate. Their former admission/warm gate is preserved. An old swap peak did not keep poisoning later runtime observations. Fresh filesystem free space must still exceed10GiB on both filesystems; exactly10GiB triggered critical pause. Runtime process/publication identity, clock and evidence checks remain active.

Actual sample() call sites rejected explicit service failure, product terminal failure, shutdown, hard reserve, missing formal capacity, replacement invocation/PID, unreadable publication and malformed JSON, retaining diagnostics. Readable core degradation remained an ordinary quality observation. Actual __main__ exception handling wrote a FAILED receipt with PAUSE_AFFECTED_CAPTURE and requires_owner_notification=true. The existing native ExecStopPost restoration path remains unchanged. The notification flag is a request to the existing observer path, not proof that a human received an alert.

The scoped selector equals the accepted causal START metadata repair. Existing scoped-terminal retains __main__ protection, same-run/target/start/cohort bindings, both full released LIVE passes and completed immutable verification. This review did not re-audit Raw or rerun the already accepted genuine nested-spawn corpus fixture. It does not claim that any partial/blocked native audit can be admitted merely because a995 policy literal exists. A trustworthy full saved-frame denominator and independent final assessment remain mandatory.

## Independent checks

Thirty repository unit cases PASS, targeted Ruff PASS and targeted mypy PASS on the final module/tests. Independent fixtures add:

- 13 executions of the exact complete capture() controller with an86400s fake clock, plus bounded warm-up/finalization. These cover healthy cadence,420s episode, core recovery with stale auxiliary,15 bad slots, persistent core/auxiliary endpoint BLOCK, exact+60s and over-bound+61s observations, clock-invalid delay, resource diagnostics, a delayed/missing slot, and actual filesystem reserve failure.
- 7 separate monitor evidence mutations: UTC day jump, missing UTC, bool UTC, float schedule, impossible skipped count, duplicate slot, empty stream. All malformed cases are rejected. The empty stream honestly yields0/1000/BLOCK with every nominal slot missing.
- 14 calls through the real sample() function for healthy, degraded, explicit critical and pending-start states, plus the actual __main__ critical failure receipt entry.
- 4 additional real auxiliary/warm/resource/evidence-write boundaries: pending STARTING cannot pass strict start, warm peak veto remains, runtime historical peak is diagnostic, and failed control evidence write retains the initiating observation/error and propagates.

The full-controller fixture replaces only environmental side effects, package config loading and observation inputs. It executes the real scheduling, validation, endpoint, rate, target and dispatch logic. External subprocesses are forbidden. Fsync is explicitly stubbed only in the synthetic full-clock scheduling fixture; this establishes control semantics, not production durability. Separate evidence-write checks retain the actual write_sample error path. No synthetic duration grants real-time credit.

## Reuse for later isolated Linux QA

Three scripts are retained with exact hashes and outputs in FINAL_MANIFEST.json:

1. `controller_independent_faults.py CANDIDATE_DIR [FROZEN_SOURCE_SRC_DIR]`
2. `critical_actual_callsite_faults.py CANDIDATE_DIR [FROZEN_SOURCE_SRC_DIR]`
3. `admission_evidence_boundary_faults.py CANDIDATE_DIR [FROZEN_SOURCE_SRC_DIR]`

Run using the intended isolated Python with `-B`. They create synthetic temporary evidence only beneath their own script directory, forbid external service commands and do not open production Raw. Supply the exact intended frozen release src directory for explicit package binding, or run under its installed environment. They are suitable for repeating policy tests against the later bound control copy; they do not replace the separate import/origin, current admission, native systemd, real nested-spawn, restore and physical binding checks.

## Remaining work outside this acceptance

The parent must independently bind and review new N2 config/identity/cohort/scope/window/T0/forecast/policy predecessor and effective native arguments. New current disk/time/memory admission must use actual N2 dimensions; no stale N1 product-count assertion or forecast may be carried into an executable control set. Existing actual source c3686dd/wheel68460be4 remain frozen. Native notifications/restoration and the complete own24h/dualLIVE/verification/independent saved-frame995 assessment/restored receipt are actual-stage obligations. N2 has not started in this review.
