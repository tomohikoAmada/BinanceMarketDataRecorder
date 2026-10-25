# R2 recoverable readiness classifier repair

Current: **LOCAL CANDIDATE /39 PURE LOCAL CASES PASS / EXACT CODE REVIEW ACTIVE / NOT DEPLOYED**.

- [x] Actual capture failed at own85410.086844564s, before86400, on product.failure=planned_rotation in the private write-safety predicate. Original failed publication and full input read retained; no completed24h or saved-frame credit.
- [x] Source semantics distinguish statusFAILED supervisor termination from CONNECTING readiness/resync reasons. Five exact source tags are planned_rotation/unexpected_disconnect/server_shutdown/ingress_backpressure/sequence_gap. Service and write/trust failures still require pause.
- [x] Candidate621063b0 only changes private check_write_safety. Explicit FAILED always pauses, even a known/null reason; malformed/unknown status/readiness or reason/writer-style errors pause. Only trusted RUNNING+CONNECTING/readyfalse and exact known reasons remain logged quality faults. Core monitor BLOCK,2880/995 accounting and all other algorithms remain unchanged.
- [x]39 pure LOCAL fixtures reproduce the original rejected real metadata and exercise positive/negative/mixed-product/disk guards plus complete synthetic86400/2880 fault accounting. No real Linux, elapsed time, source CI, Raw or deployment credit.
- [x] [Independent actual-failure/source-semantics/restoration conclusion](../reviews/2026-10-25-r2-instrumented-n2-failure-policy-review.md) ACCEPT1f1630b9/result243a408b/manifest0faf33df; all23 parent members/current input SHA/bytes PASS.
- [ ] Separate exact implementation code review ACTIVE for final self-contained8-member manifest2f6dfe12; affected genuine Linux qualification remains pending.
- [ ] Fresh restored handoff/canonical identity and exact failed-partial custody or independently verified zero-partial inventory before startup; new scope/config identity binding/current measured admission/new finite window and own86400 T0. No old closed guard/time reuse or blind unchanged recapture.

Production source20e57/wheeldd2c/currentidentity9b8/config5b73 remain unchanged. No ping/queues/routes/retry/StopSec or threshold changes. All Raw/archive/failed/derived/venvs retained; no accepted N2/N4 or hardware maximum claim. Current automatic restoration already completed; do not repeat it. See [candidate design](R2-readiness-classifier-repair-20261025/DESIGN.md), [local results](R2-readiness-classifier-repair-20261025/parent-tests.json) and [exact manifest](R2-readiness-classifier-repair-20261025/DESIGN_MANIFEST.json).
