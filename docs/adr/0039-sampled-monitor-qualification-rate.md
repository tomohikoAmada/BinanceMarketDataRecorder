# ADR-0039 — Owner-approved sampled monitor pass rate

Status: accepted owner policy, October 4, 2026; implementation/admission review
and actual full terminal acceptance remain separate gates.

The owner explicitly authorizes monitoring pass rate **at least 99.9%** for
qualification and continuation through optimized-artifact Q7 / Formal2h. This
replaces the supplementary rule that any observed ordinary REST failure rejects
an entire stage. It does not redefine captured-data integrity or market coverage.

The numerator is recorded auxiliary monitoring checks with result PASS. The
denominator includes **all** recorded checks whose resource-row UTC timestamp is
within the native stage T0 and target, including original BLOCK checks. Compare
`passed * 1000 >= total * 999` exactly, without rounded displayed percentages.
Pre/post-target samples cannot dilute the denominator. Retain failed checks,
their reasons, original bytes and recovery evidence; never change them to PASS.

The fixed four-product / 26-context sampler has a nominal 30-second cadence.
The calculation rejects empty/malformed, unordered/duplicate, configuration-
mismatched or internally inconsistent evidence and gaps exceeding two cadences,
including period edges. This permits measured sampling overhead without treating
sparse observations as whole-period monitoring. Bind the complete immutable
JSONL with one streamed SHA-256 while calculating its rate. The digest identifies
the exact decision input; individual failed-line digests bind retained exceptions.
It is not an event hash chain or a replacement for any Raw readback/audit hash.

Strict pre-start and target endpoints, stable identity, core acceptance findings,
Raw CRC/hash/count/sequence/gap checks, archive readback/deletion authorization,
both independent complete LIVE passes, completed verification and independent
restored closeout remain required. Monitoring rate is not per-stream data
completeness, lossless capture, continuous uptime or a PRODUCTION_READY claim.

Q5's original 1435 PASS / 1436 in-period rows give 99.93036211699165%, satisfying
this new monitoring policy. The original BLOCK and independent rejection under
the earlier policy remain valid historical evidence. The same immutable Q5 run
can undergo the still-pending full audits under a separately recorded new-policy
decision; no new T0/time transfer or source-wheel substitution is involved.
Successful monitoring alone does not accept Q5 or start Q6.

`tools/qualification_monitor_rate.py` implements the streamed offline decision.
Boundary/failure tests and independent policy/admission review are recorded in
the current Q5 acceptance record. Old acceptance readers and published Raw,
archive and consumer contracts are unchanged.
