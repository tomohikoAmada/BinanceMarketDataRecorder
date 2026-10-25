# Bounded warm readiness ordering correction

Current: **LOCAL CANDIDATE BLOCKED / SUCCESSOR LOCAL CODE IMPLEMENTED / INDEPENDENT CODE REVIEW ACTIVE / NOT DEPLOYED**.

Accepted actual P3 diagnosis469cb72b/result0ffaa9d0/manifest095323da establishes that11 strict warm observations span301.625s, then the canonical CLI session takes115.543440s and the existing total1200s guard fails before T0. No accepted24h or current capture exists; original automatic restoration completed before guard cancellation.

Candidate controllerd8c0c6ed retains the single unchanged canonical readiness CLI on the first strict PASS, during the already required five-minute observation interval. It preserves1200s,300s endpoints, minimum sample count, full995/2880 and own86400/audit/restore semantics. The CLI is capped at min(330,remaining existing warm budget), its exact result/partialJSON/timeout bytes/timing and before-after boot/PID/inv/config/source are retained. Earlier canonical output explicitly does not supply current T0 freshness: fresh strict end/start/heartbeat/capacity/resource checks and same boot/service instance remain mandatory. Deadline assertions also guard a would-be break and strict warm completion.

24 isolated LOCAL cases cover timing boundaries, invalid byte/JSON/timeouts, canonical identity/runtime mismatches and evidence-write failures. All other controller functions/constants are AST-identical. These are synthetic-clock cases, not genuine Linux, actual duration, deployment or capacity qualification. Initial LOCAL prototypeec7581 remains preserved separately. The failed P3 runtime and production20e57 are unchanged.

Independent exact code review882fbd11/result5c37eb63/manifest9a1ca201 BLOCKED this candidate; all23 parent members checked. Two additional full-flow counterexamples show missing fresh capacity/publication-identity predicates after early READY. A small source-aligned current completion check over all cheap readiness predicates is required; successor independent design ACCEPT41199a43/result7907eb7d/manifest16243a84 has all9 parent SHA/bytes PASS. It creates no implementation and executes none of47 future flow faults; substantial implementation waits for adequate quota on a following hourly wake. After full review/parent artifact binding, genuine affected Linux qualification, new reader export of warm-readiness-check.json, fresh failed-partial custody/stopped handoff/current admission and a new scope/finite window precede any new capture/T0. Old guard194e is CLOSED forever; no extension, resume or inherited time. Shared KVM all-module/language optimization remains deferred.

See [design](R2-warm-readiness-ordering-repair-20261025/DESIGN.md), [candidate manifest](R2-warm-readiness-ordering-repair-20261025/DESIGN_MANIFEST.json), [local cases](R2-warm-readiness-ordering-repair-20261025/parent-tests.json) and [actual failure review](../reviews/2026-10-25-r2-readiness-warm-start-failure-review.md).

See the [independent BLOCK review](../reviews/2026-10-25-r2-warm-ordering-code-review.md). Original7 inputs remain immutable; this blocked candidate is never copied or launched.

See the [successor design review](../reviews/2026-10-25-r2-warm-source-successor-design-review.md). Earlier draft remains BLOCKED; no VPS or new T0 exists.

The design successor is now a separate [LOCAL code candidate](R2-warm-source-successor-code-20261026.md),77 explicit LOCAL cases PASS and independent exact review ACTIVE. No Linux/deployment/new T0 exists; earlier d8 draft remains BLOCKED.
