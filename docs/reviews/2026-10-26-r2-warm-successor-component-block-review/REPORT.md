# Independent exact warm-successor component review

Decision: **BLOCK** for the exact 13-member CODE_MANIFEST `2f75d522c2782144c2ddff692010c1825e874f9c06b1a442110a561543958282`. Two operational control defects and a narrower receipt-validation defect are reproduced. No component copy, ARM, launch, new T0, Linux qualification or accepted capacity is authorized by this report.

The reviewed candidate is `/private/var/tmp/bmdr-r2-warm-successor-code-20261026-lrdrj2v4`. Subject bytes are controller `eb8f321a8c1433987f72e42d2dea1afc2ddc217a2e4e016ac4d5d3a2a9367c4f`, warm_readiness `9c173bf88382dba65985259b0bd3ad62695e4f741b314724201e578c6900b7fc`, and read-only warm_completion `f8c7fc8569d6a0b32cca4cb0e24a5b975e376be2a90f5b97872ce8c4e1d3287d`.

All 13 bound inputs, 12 original runtime references and six exact source modules were independently SHA/byte read back. Every non-capture controller function and the complete `next_bt` formal-monitor/target/stop/audit tail are AST-identical to original `3b3f8d50`. Changes are confined to the warm/start prefix and helper import. Source 20e57, the failed P3 runtime and old closed guard 194e were not modified. Old common constants are present only as OFFLINE references and explicitly forbid executing the failed scope.

## Reproduced findings

### F1 (P1): direct child exit does not establish owned-tree cleanup

`warm_readiness.py:143-200` owns a newly created process group, but after SIGTERM it treats direct `proc.communicate()` completion as `owned_group_TERM_reaped`. It sends SIGKILL only if that direct communicate call itself times out. Normal direct-child success does not inspect or drain the remaining group at all.

Two real LOCAL POSIX fixtures establish the gap. A descendant ignores TERM, closes inherited stdout/stderr and remains asleep. In the timeout fixture the wrapper is killed by TERM, pipes reach EOF, the receipt reports `owned_group_TERM_reaped`, and the descendant remains in state `S`. In the normal-exit fixture the wrapper exits zero after spawning the same closed-pipe descendant; the receipt reports `normal_exit` and `command_ok` accepts it while the descendant is still running. The timeout path still rejects T0 because `timed_out=true`; the finding concerns false cleanup completion and residual owned activity. The normal-exit path additionally accepts the command.

Both fixtures SIGKILLed only their own newly created groups in finally cleanup and terminated/reaped their own unrelated sentinels. The sentinels were unaffected by candidate signalling. The four known wrapper/descendant PIDs were absent at the final local ps check. These are real macOS POSIX executions with MONOTONIC explicitly standing in for BOOTTIME, not Linux or Recorder/service actions. Exact command stdout/stderr and receipts are retained in the two OWNED_GROUP result files.

Minimal correction must establish bounded cleanup/emptiness of the exact owned tree even when the leader exits normally or early and descendants close pipes. Leader reaping and EOF are insufficient. Preserve an ownership mechanism that avoids PID/PGID reuse and unrelated signalling, retain original failure/bytes, and qualify noncooperative/closed-pipe/leader-first cases. This report does not prescribe unrelated timeouts, queues or production changes.

### F2 (P1): current bad live publication is read and ignored before T0 commit

`WarmStart.fresh()` at `warm_readiness.py:363-379` checks the cached strict/source-paired publication for auxiliary quality, write safety and timestamp freshness. Its subsequent `snapshot()` at lines 113-116 rereads the current service publication but retains only PID, RUNNING status and service instance. It neither compares that current publication to the selected pair nor re-evaluates its dynamic content. Repeating fresh calls around owner-start/commit therefore repeats the same cached proof.

Three complete warm-prefix fixtures change current service_state immediately when start-observation is durably published, after a valid start pair was selected: (1) wrong published deployment identity; (2) capacity observed_at stale by one hour; (3) actual hard reserve reached, state HARD_RESERVE and free_bytes=1. They preserve PID/invocation/service instance and the config/identity files. The same unchanged source evaluator, executed against each newly changed publication inside the fixture, returns FAILED (identity/staleness) or NOT_READY (hard reserve). Despite that, the exact candidate prefix calls fresh repeatedly and emits owner-start-commit with no failure. Every generated control and the last live publication are preserved in INDEPENDENT_ADVERSARIAL_FULL_FLOW_RESULTS.

This is not merely a change after the final read: candidate snapshot actually reads the newly bad live publication and ignores the changed source predicates. The two old before-pair holes are covered by the supplied suite, but this after-pair bypass reintroduces the current-readiness problem at the decision boundary and can hide an actual write/disk safety stop.

The narrow remedy is an explicit live-publication-to-pair guard at each eligibility/commit decision. Before commit, publication advancement can obtain another bounded current pair; after owner-start publication, any mismatch must prevent accepted eligibility rather than accepting the cached proof. Do not make the entire dynamic publication part of the earlier static same_guard, which would incorrectly reject ordinary heartbeat evolution. Preserve one early heavy CLI, the original whole-warm 1200s, strict endpoints, and existing pair caps.

### F3 (P2): contradictory stored static/clock proof is accepted

`full_result()` at lines 219-225 accepts any truthy installed_dependencies map and Python `True > 0` as a RECORD-file count. `load_attestation()` at lines 254-259 checks partial lower/upper clock bounds but does not validate complete command before/after ordering or interval coherence.

Three more complete-prefix fixtures reach owner-start-commit with (1) installed_dependencies claiming exact_match=false, editable=true and locked_distribution_count=0; (2) installed_artifact.record_verified_files=True; (3) command receipt after.utc earlier than before.utc. Current synthetic clocks remain normal in the third case, so clock_guard does not reject the contradictory recorded command interval. The cases preserve bytepack hashes and all normal references through the provided fixture custody model.

An honest unchanged full canonical verifier normally cannot emit the first contradictory dependency result. This is a proof-validator/malformed-receipt gap, not observed installed corruption and not a promise of security against arbitrary root. The requested root-receipt failure tests require rejecting contradictory evidence rather than treating a READY label as sufficient. Require source-shaped static proof fields and exact types, positive nonboolean counts, ordered/coherent command clocks and budget/witness interval bindings. No repeat installed/Catalog scan is necessary.

## What the existing executions establish

The supplied actual-prefix suite independently reproduces 57/57 PASS, and the boundary suite independently reproduces 20/20 PASS. These are suitable LOCAL tests of the exact candidate prefix and real VpsReadinessEvaluator with explicit OS, process, environment and expensive-static providers. They establish the intended one-full-CLI placement at first strict PASS; original1200 deadline/no extension; >=300s first/end strict span; >=12 primary rows; recoverable middle BLOCK retention; bounded3/30s pairs; first/end/start publication evidence; rejection of before-pair stale capacity and wrong published identity; many topology, heartbeat, profile, Catalog/startup, environment, disk/resource, clock/incarnation and durability faults. They also retain an initiating command error when secondary evidence writing fails. They do not establish the untested47-case design as executed or negate the new counterexamples.

The six additional complete-prefix counterexamples run the same controller warm/start prefix, warm_readiness and warm_completion with unchanged exact source evaluator. Static/system/clock providers are explicitly mocked. Actual generated controls, source results, events and final current publications are retained, rather than relying only on AST checks. The two additional owned-group cases are real LOCAL POSIX subprocess tests. Total executions are 77 supplied passing cases plus six independently reproduced full-flow acceptance counterexamples plus two independently reproduced process-tree failures; this is not an 85-case qualification pass.

## Source contract and attestation scope

The child genuinely reuses source VpsReadinessEvaluator. Only installed-artifact/dependency scans and Catalog integrity are earlier static subproofs, declared as conditional earlier reuse and never relabelled stock fresh full readiness. Identity/config small-file hashes, current permission/control chain, install contract/effective properties, runtime PID/activity, effective process environment, source schema/heartbeat/liveness/status, published identity/profile, Catalog-open/startup-recovery, product topology/core persistence, and capacity fields/age/profile/reserve remain source predicates. SOURCE_PREDICATE_MAP records the exact split.

The exact source READY path calls both identity and guarded Catalog callbacks; the child records flags. No actual unchanged-source skipped-callback READY path was established. Parent protocol checks omit some returned flags/budget fields, but this static observation is not counted as a separately reproduced finding. Root-owned evidence and code are a trust boundary; a receipt validator cannot defeat arbitrary root rewriting code and receipts.

The current cheap-source pairing improves the old candidate by rejecting stale capacity and mismatched published identity before pair selection. F2 is specifically the later live-state advancement gap. Static Catalog-file identity allows legitimate content growth and grants only earlier integrity attestation, not fresh integrity of later appended contents. Both distinctions must remain explicit after correction.

## Scope and next gate

This is exact component code/LOCAL review only and it is BLOCK. No candidate or production file was edited, no SSH/network/Raw/manifest payload opened, no service control or old failed window reused, no new formal T0/capture or saved-data correctness denominator created. Source, wheel, production deployment, failed P3 controls and earlier BLOCK reports remain preserved. No CPU/network root cause, stability fix, current-source N1 qualification,24h acceptance,N2 capacity receipt,N4 eligibility or hardware maximum is inferred.

Correct only the affected component defects in a distinct frozen successor and independently re-review. Future operational eligibility additionally requires explicit fresh scope/constants/reader export of budget/binding/first/full/end/start attempts/witness/commit, actual Linux process control qualification and exact imports/subjects, fresh custody/admission/window/native bindings. Old Q3/194e is CLOSED and cannot be extended or reused. A future accepted full measurement must start a new own86400 with unchanged995/2880 and full saved-data/restored closeout; this review supplies none of that credit.
