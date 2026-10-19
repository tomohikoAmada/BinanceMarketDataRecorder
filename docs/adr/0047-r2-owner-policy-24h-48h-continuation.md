# ADR-0047: R2 owner-policy 24h and 48h continuation

Date: 2026-10-19. Status: owner-authorized; execution gates pending.

The owner requested completion of Recorder R2 after the accepted repaired
release's own12h, including the remaining24h/48h under the approved rules.
This supersedes earlier stop-before24h instructions for this request. It does
not authorize R3/20-product deployment or concurrent Gateway VPS load tests.

The owner-policy chain is the same frozen source
`ca760c09a5423b0b1c68778e0e3700c2758df897`, wheel
`c2eeb21996d1f1fe57f94ef316b4ce3fffeeef37332ca4ba1898fd93c75fbfb6`,
deployment `7276c9fa69b5a7a6073ec412fb84e319a2b86f8a69c94fac414eea26bf43f31d`,
config `5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`,
all14 enabled features and four Spot/USD-M BTCUSDT/ETHUSDT products:

1. Accepted own2h receipt `f45fbbc6ba0cf8bc418052ec32e2eaaba42bcdda1627400eb507b40abac64385`.
2. Accepted own12h receipt `aff063c3f617b06250bbe91165fcddf74208c50e6a8a5316b56aa6723b50209b`.
3. A separate fresh own86400s run, complete selected-data and restored closeout,
   then independent acceptance and an owner24h receipt.
4. Only after step3, a separate fresh own172800s run with the same complete
   gates, independent acceptance and an owner48h receipt.

These receipts are owner-policy predecessor anchors under ADR-0041/0043, not
canonical V5 finals. No canonical eligibility flag is rewritten. The old
49b4a2d Q7/Q8 and failed attempts contribute no duration to this repaired-source
chain. Reconcile four accepted minimum targets totaling309600s (86h) only after
all four receipts exist; label that result `OWNER_POLICY_QUALIFICATION_86H`.
Do not emit the older canonical four-final certificate.

Each stage requires fresh canonical deployment verification, package/reboot
checks, retained-corpus census, measured growth/capacity and full two-pass audit
plus verification/review/restoration budget, a separately bound finite quiet
window, actual all-enabled warm-up, strict26/core readiness and its own native
CLOCK_BOOTTIME T0. Keep existing10GiB reserves, memory/owned-swap/identity/clock
gates,30s nominal monitoring,60s per observed core episode and ADR-0046's strict
actual finish with original episode deadline and absolute minimum+60s bound.
Monitoring and finish probes remain separate from saved-frame correctness.

At each target, stop/disable, naturally drain archive, freeze only this attempt's
new saved data (including warm-up and orderly stop tail), run both released LIVE
passes and completed immutable-control verification, then independently reconstruct
the complete selected controls. Require saved-frame contract conformance strictly
above99.9%; preserve all gap/incomplete/zero-frame flags. No every-exchange-event
completeness claim is possible. Collect actual configured-product connection
rotation/recovery evidence around the23h50m policy and throughout48h; elapsed
alone does not establish that evidence. Do not inject destructive production faults.

Restore original OS/archive owners before cancelling the stage's guard. A fresh
same-identity stopped-disabled handoff must include natural archive success and
a finite future monotonic timer deadline. Closed windows are never reused,
rearmed or extended. Failure retains its evidence; repair the affected closeout
without recapture when the accepted duration can be preserved under these rules.

R-078 and broader Production Ready remain separate. Resolve and document actual
capacity/audit practicality from these stages;86h alone does not close R-078.
Preserve all previous data, receipts, failures, software and unrelated edits.
