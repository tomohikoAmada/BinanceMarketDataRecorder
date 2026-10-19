# R3 offline independent review

2026-10-19 — **APPROVED**, no unresolved findings.

The owner requested a new review subagent for code branches. A fresh independent
subagent reviewed the uncommitted `codex/recorder-r3-offline` change against
`9643c96d2509`, ran local checks and examined the result/assumption documents.
Neither agent accessed the VPS or changed production source/configuration,
services, frozen qualification gates, capture data or automations.

Findings closed before acceptance:

- Include a whole timer cycle in peak no-service backlog, since a worker can
  exit before its runtime limit; also cap service by the real max-files limit.
- Charge old tiny-chunk backlog conservatively for its file-service cost.
- Budget external archive manifests, not only compressed Raw on the target.
- Require complete `verify_all()` success before reporting benchmark completion.
- Remove a stale 14-product expected set from the new 20-product emergency test.

The larger local fixture exposed a tool-only assumption about natural phased
rotation: the final frame may already seal a writer, so round close need not
produce another manifest. The corrected tool retains real chunk counts and has
a forced-every-frame rotation regression; the failed fixture remains preserved.
The all-feature runtime fixture's wait now accommodates the real Spot weight
pacer; no production limiter, rotation, queue or durability settings changed.

Final parent validation: **214 affected offline tests PASS**, full **Ruff PASS**,
full **mypy PASS (311 files)**, `git diff --check` PASS, candidate TOML parses as
10+10 with all14 switches enabled. The reviewer also independently ran the
runtime/backpressure, budget, archive and delivery cases and recomputed all four
saved budget scenarios. This is local validation, not a claimed Linux/VPS run.

[Storage/retention runbook](../runbooks/R3-offline-capacity.md),
[original local result records](../milestone_evidence/r3-offline-storage-20261019.json),
[budget inputs/results](../milestone_evidence/r3-disk-budget-examples-20261019.json)
and [joint-IP inventory](../runbooks/R3-shared-ip-budget.md) correctly separate
synthetic evidence from production admission. R3-b/G3-b's combined request and
connection windows remain OPEN; the 6220>6000 Spot scenario is a disclosed
negative budget, not an observed production ban. Final symbols/metadata,
representative real archive samples and same-VPS resources/recovery/BMD-4 remain
unqualified. The active R2 four-product 24h and R-078 authority are unchanged.
