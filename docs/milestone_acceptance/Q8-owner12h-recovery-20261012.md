# Q8 owner-policy twelve-hour recovery correction

Status: **PREPARED; new finite window/T0 not yet created.**

The owner requests diagnosis, research and a minimal reliable solution to the
failed twelve-hour test. [ADR-0042](../adr/0042-per-episode-core-recovery-in-owner-tests.md)
and the [independent implementation review](../reviews/2026-10-12-q8-recovery-policy-review.md)
record the corrected private recovery controller. The old2b406 retry failed
before43200s because recovered episodes exhausted a coarse cumulative budget;
its failure and closed window remain immutable with zero accepted12h.

Production source49b4a2d/wheelc7d354be/identity7072575b/config5b73 stay unchanged.
All14 functions/four Spot+USD-M BTCUSDT/ETHUSDT products/growing corpus/unique
Raw/archive/old failures/venvs remain preserved. No language/server change or
unrelated service intervention is required. Correctness strictly>99.9% requires
the complete actual saved-frame denominator, not availability samples.

Unique new scope:
`/srv/recorder-data/recorder-archive/evidence/Q8-owner12h-recovery-20261012-im8wwpyk`.
Private controls: `/private/var/tmp/bmdr-q8-recovery-retry-20261012-im8wwpyk`.
This preparation creates only directories; it does not reuse a closed window or
inherit old time. New actual129600s deadline will be immutable after arm.

Fresh stopped census49571chunks/82719886frames/4418752589storedB/
63199170403decodedB/max35520800B, both backlogs0. Projected writer11739087765B
and archive reserve exceed10GiB. Full dualLIVE+verify forecast59955.611s;
125% allowance plus1200s warm/43200s capture/stop-drain/review/restore requires
124924.514s of129600s, leaving4675.486s before actual arm. All actual admissions
recompute current remaining time/capacity. Audit temp files remain on the2TB
archive disk, distinct from the61GB writer filesystem.

- [x] Exact old failure replay and independent diagnosis.
- [x] Minimal helper/controller/timing correction independently ACCEPT.
- [x] Repo focused28 tests/Ruff/mypy; final VPS mock23+15 PASS.
- [x] Fresh retained-corpus census/new scope/finite forecast prepared.
- [ ] Exact new scope/census/native argv independent DESIGN acceptance.
- [ ] Actual copy/readback/temp binding/canonicalVERIFY/finite arm/admission.
- [ ] Actual all-enabled warm/current incarnation resources/strict own43200 T0.
- [ ] Complete strict target/full-period monitoring evidence, separate metric.
- [ ] Natural freeze/two fullLIVE/completedverify/full saved-frame denominator.
- [ ] Independent control-only reconstruction/historical manifests preservation.
- [ ] Actual OS restoration before guard cancel/normal archive/fresh VERIFY/handoff.
- [ ] Owner receipt/plan-state-handoff main push/hourly task pause.

No accepted12h or canonicalV5 final/predecessor is claimed. Stop before24h/48h.
