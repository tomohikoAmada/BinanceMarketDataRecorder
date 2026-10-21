# Owner change: sampled monitoring threshold99.5%

Date: October21,2026. The owner explicitly requested: “我愿意把通过门槛改为99.5%”,
referring to the sampled monitor result99.861%. Apply **at least99.5% (995/1000)**
to the current N1 supplemental reassessment and subsequent VPS capacity cohorts.
This is a newly authorized acceptance policy; the original observations and
original999/1000 result remain immutable.

| Criterion | Current requirement | Change |
| --- | --- | --- |
| Sampled monitor PASS checks / all nominal checks | >=99.5% | Owner approved |
| Complete saved-frame contract conformance | Strictly>99.9% | Unchanged |
| Own86400s and strict configured start/finish/core | Required | Unchanged |
| Identity/clock/resources/owned current+peak swap0/60s recovery | Required | Unchanged |
| Complete dual LIVE, immutable verify, independent final and actual restored handoff/receipt | Required | Unchanged |

## Plain explanation of the N1 result

A check occurs every30s,2880 times in24h. Four checks recorded core_not_ready;
their next separate probes around5s later were strict PASS under the same
Recorder incarnation, without restart. Thus2876/2880=99.861111%. The old99.9%
threshold allows at most two failed checks;99.5% allows at most14. The four
failed samples remain failures. The new monitor criterion is PASS.

This is a proportion of sampled ready states, not measured uptime/downtime or
saved-frame correctness. Current projected maps identify the core-readiness
symptom, not the specific stream or initiating transport/report/hardware cause.
Heartbeat was fresh at all four fault samples. The exact Recorder journal and
original diagnostic state bytes still need the already planned metadata read;
the preceding attempt timed out before SSH banner exchange. No new VPS read,
restart, threshold change to recovery or duplicate capture is needed here.

## Immutable supplemental reassessment

- Original current read4babd497 and original monitor resultae0a7700 remain unchanged.
  Original nominal JSONL hashf34abc73,2876PASS/fourfailed/2880total and failed
  line hashes are preserved; warm-up and finish probes do not enter the denominator.
- Owner policy SHAfe1e989cf6ac5f63f7dd4055b7d654b561c879f873645a4b36f0c60b09e08fe6.
- New supplemental monitor result SHAc5a0c117ecfce21414a84cdf43e02d9536ad7444db1f16956d1bad4f70d741a4,
  resultPASS at995/1000; it explicitly grants no final owner24h or N2 credit.
- The future private monitor template46c5bbd88b851e6b1a5fbba1b3c86c260fe20e0748e4984df9126bbd89708325
  changes only three policy/threshold literals from the accepted config-aware
  N1 monitor. It retains complete expected-context, cadence, config, ordering
  and evidence checks. Fifteen local boundary/invalid-input/dilution cases PASS,
  including exact99.5% and2880-check14-failurePASS/15-failureBLOCK boundaries.
- Private policy/template/reassessment: `/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/owner-monitor995-20261021`.
  Independent narrow review ACCEPT78fb3d73/result1315961f/manifestae2a7f11:
  all26 members/290 independent checks,15 supplied cases+10 extra faults,
  exact current supplement/old immutable evidence and minimal AST diff verified.
  See [frozen independent review](../reviews/2026-10-21-r2-monitor995-independent-review.md).
  The future new-stage controller must
  also change its separate diagnostic comparison from strictly>999/1000 to
  >=995/1000; exact one-line patch68eb6b0e is retained in that private directory.
  Apply it only before freezing a fresh stage, never to current armed helpers.

No running13-member control bundle, original report, source/wheel/config/identity
or service was modified. N1 final receipt must bind the original monitor result
and this separately labelled owner policy/supplement with independent acceptance.
New N2+ scopes must use the995 template when freezing their fresh controls; do
not rewrite an armed stage's frozen helper or fabricate a new original result.
N1 complete saved-data/independent final/restored owner receipt remains pending.
