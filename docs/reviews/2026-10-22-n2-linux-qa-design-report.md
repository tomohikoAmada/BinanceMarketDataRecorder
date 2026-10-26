# Independent narrow N2 isolated Linux QA design review

Decision: **ACCEPT_QA_RUNNER_OPERATOR_DESIGN_LOCAL_OFFLINE**, limited to candidate-v2. No remaining blocking finding in the reviewed orchestration and fixture transplantation. Actual runtime/deployment binding, Linux QA execution, effective native behavior, physical ownership and N2 admission/ARM/T0 remain unaccepted here.

Exact reviewed sources are qa-runner.py SHA256 `8308129c1ba0f912a46e4a9bba5f7a5708ce4af16a79850d916fb59fa2a37b29` and qa-operator.py SHA256 `54cd52d3e226bd38bf7925c1a5ed037cee38e32308fe5790fafb117d78dac5f8`, retained in `candidate-v2/`. DESIGN.json and all six fixtures/config/receipt remain identical to candidate-v1. This report accepts those exact files; later byte changes require a narrow supplement.

## Scope

Only this exclusive review directory was written. No SSH, network, native unit, production Raw, production config, Recorder start, OS masking, deployment or actual QA command was executed. Operator SSH calls were replaced completely. Generated remote programs were compiled unchanged and executed only with path mapping into synthetic local directories, root ownership/host commands substituted. The runner orchestration tests use the real frozen c368 package preloaded locally and synthetic sys.executable/sys.prefix to pass host-specific guards; they are explicitly not clean-import or Linux-runtime acceptance.

No accepted policy unit/full-clock proof, original N1 audit, production source/wheel/CI, or independent selected-data proof was repeated. The six fixture bodies were not executed by this review; their accepted logic was reused by exact byte/diff inspection. Runner subprocess responses are synthetic success/nonzero/TimeoutExpired results. This tests packaging and control behavior without fabricating real elapsed duration or actual fixture completion.

## Two initial findings corrected

The original generated launch executed setupQ/pin-unit-exec.py even though QA_MANIFEST bound the transferred q/runtime/pin-unit-exec.py. The final launch executes the latter using system Python. It is now one of the fourteen hash-checked runtime members before execution; canonical Python then runs qa-runner.py. The initial failure is independently reproduced in operator-v1-run.log and retained with candidate-v1.

The original timeout path skipped stdout/stderr flush/fsync because those statements followed subprocess.run(). The final runner wraps that child in try/except/finally, appends an explicit TIMEOUT case, flushes/fsyncs both logs in finally, then publishes an exclusive FAILED qa-result. The original failure and corrected boundary are retained in runner-v1-run.log and runner-v2-run.log. The candidate fixture/policy logic was not altered to satisfy these tests.

## Fixture transplantation

DESIGN.json's six original source hashes and copied QA hashes were independently checked. Four fixture scripts are byte-for-byte copies: controller_independent_faults, critical_actual_callsite_faults, admission_evidence_boundary_faults and publication_binding_faults. finish_actual_fullflow changes only candidate/source CLI arguments and the copied config path. common_topology_forecast_faults changes only the copied actual N1 receipt and config paths. All six compile. Exact diffs and fixture-transplant-results.json are retained.

The copied config remains SHA256 `5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`; the copied actual published N1 receipt remains `a7938d4ae2ab564945f2dd0434132bf4978f69756103f66838b5fb75eacc29ac`. These bindings do not retroactively extend N1 publication evidence or grant N2 duration.

## Operator and generated-program checks

Eleven independent local cases pass. Missing actual binding, pending acceptance, runtime hash mismatch and an extra runtime Python member cause zero external calls. The copy, launch and read generated programs compile as emitted. A complete generated copy puts twenty-four payload files in the synthetic exclusive QA directory: fourteen runtime members, runner, six fixture scripts, copied config/receipt and QA_MANIFEST. All contents round-trip against their hashes. Repeated copy is rejected before mutation.

Generated launch requires the exact accepted deployment-result SHA/result, compares every QA payload hash and the complete QA_MANIFEST, requires neither prior qa-result nor launch-attempt, and requires the unique native unit to be not-found. It writes an exclusive launch attempt before the one systemd-run. Synthetic repeated launch is rejected and emits no second native command.

The native command is Type=exec, RuntimeMaxSec=1200, TimeoutStopSec=120, UMask0027, CPU/Memory accounting, archive-local TMPDIR and PYTHONDONTWRITEBYTECODE. System Python runs the manifest-bound runtime pin, followed by the canonical installed interpreter and exact runner. Generated effective readback includes actual InvocationID, ExecStart, timeouts, KillMode and UMask at dispatch. No Recorder start, production config mutation, pip/venv/source rebuild or OS masking command appears in this path. The pin's actual Linux DBus load/reference/unreference is a future native obligation.

Read generation stays inside this one QA directory, exporting file bytes/hash/size, this unit's state and unit journal, plus a read-only Recorder state query. It does not enumerate or open production Raw. Actual root:988/mode and directory permission evidence should be collected directly during physical readback; this design review does not infer physical ownership from synthetic chown calls. The existing read output's hash/size fields alone do not independently establish those permissions.

## Runner boundary checks

Four exact-runner orchestration cases pass: all synthetic fixture children succeed; first fixture returns nonzero; first fixture times out240s; payload tampering occurs before imports/fixtures. Success executes six symbolic children and fsyncs all twelve log files. Nonzero and timeout stop after the first child, fsync both logs and publish an exclusive FAILED result. Timeout preserves its case/240s bound. Tampered common is rejected with no child execution. Tests use a0027 local process umask and verify640 log modes; actual Linux uid/gid/mode still needs physical readback.

The real runner design compiles thirteen Python payloads and imports twelve non-pin modules from its complete runtime directory. It checks that the canonical package origin lies inside resolved sys.prefix and passes that installed package directory to each fixture. Pin is reserved for the outer native system Python process. The runner checks hashes before imports, uses per-fixture240s timeout, and always finishes logical success/failure handling with an exclusive qa-result. A native hard kill can leave no qa-result; missing/incomplete/native-failed output must not be promoted to PASS.

The1200s unit cap is an overall independent bound, while six times the individual240s maxima could exceed it. Consequently actual PASS requires all six children and final output before the overall unit deadline; the per-child bound is not a promise that every combination of six maximum durations can complete.

## Required later actual binding

ACTUAL_RUNTIME_BINDING.json was synthetic in these tests. The real file must follow the independently accepted actual N2 stopped deployment and identify the exact fourteen runtime members and actual deployment-result hash. The actual operator copy/readback, root-controlled permissions, canonical installed origin, outer pin/native invocation/effective properties, six true fixture returns/log hashes, final QA result and owned journal remain pending independent assessment. This design acceptance cannot establish any of them.

Any later minimal replacement of common's UNBOUND deployment SHA must be separately bound to that accepted deployment and actual QA manifest. No source/wheel rebuild, policy proof rerun, N1 duration transfer, production-ready, canonical V5/predecessor or own24h credit is granted here.
