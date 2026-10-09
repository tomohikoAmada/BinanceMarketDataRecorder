# Q7 2h acceptance under the October9 saved-data criterion

**COMPLETE_OWNER_ADJUSTED — October9,2026.** The existing corrected VPS run completed7200.002002669 seconds. The owner's latest criterion is saved-data correctness strictly above99.9%, with the existing data fully checked instead of blindly repeating2h for metadata lag. The exhaustive supplementary audit and independent restored closeout satisfy that criterion. No new2h run is required.

## Accepted result and scope

- Same run `d0be1407ef8f441dbeb4e0ab3f093a1d`, stage `2h-4c303fb942ec49d6a5c040edae20bf84`; T0 October8 23:56:09.954789776UTC and target October9 01:56:09.956795UTC.
- All24625 chunks /37312872 saved frames, including the historical corpus, fully covered by two complete LIVE reads and completed verification. Zero observed saved-frame contract failures:37312872/37312872=100%, strictly>99.9% by integer comparison.
- Complete418862 metadata records /881 chained shards reconstructed independently from a26541-member control-only bundle. Metadata rows and239/239 monitoring PASS checks are separate denominators.
- All21126 accepted predecessor manifest documents, hashes and immutable mappings remain unchanged. All24625 chunks/archive transactions are LOCAL_DELETED after verified natural archive completion; no partials/backlog or protected data removed during closeout.
- Historical4852 gap/incomplete manifests and176 zero-frame manifests remain disclosed. The claim is observed saved-frame format/CRC/hash/statistics/archive/lifecycle contract conformance, not capture of every Binance event or independent exchange payload truth.

The original target remains **INCOMPLETE/target_delta_pending**, with998 lifecycle rows and621 causal references pending at its endpoint. Supplementary frozen high-water archive123125/chunk197000/operational237 covers original122870/196630/236. The target and all original observations remain unchanged. This separately labelled owner-adjusted acceptance does not impersonate a canonical V5 stage-final, transfer source/T0/duration, or grant a next-stage predecessor. Q8/12h,24h,48h remain unstarted; production readiness remains NO and R-078 remains OPEN.

## Actual completion and independent review

The supplementary native invocation90900303865241b9800cb1b89111b447 finished successfully October9 05:45:28.103894UTC. Full audit and completed verification are COMPLETE/PASS_CANDIDATE with zero blocking findings. Wall11853.858464s/CPU10983.863954s/cgroup peak1572556800B/swap0 describe this full audit, not Recorder capture CPU.

Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE reconstruction checked every bundle member and all418862 records, ordered shard/hash chains, Catalog integrity/FK, complete24625 manifest/Raw-location/archive controls, original25-sample chain/7200s, monitor denominator and retained historical authorities. Reconstruction54.674677s; total input/chain validation65.812271s. Raw/archive payload opens, network/production opens and nonprivate writes were zero. See the [complete independent audit review](../reviews/2026-10-09-q7-posttarget-completed-independent-review.md).

Actual controlled OS restoration finished06:36:43.986576UTC, before guard cancellation06:38:56.877013UTC and the unchanged09:17:35UTC expiry. All five approved units are unmasked and the original three owners enabled-active. Normal archive timer recurrence produced naturally successful owned workers, with a finite future deadline. Recorder remains inactive AND disabled/PID0/NRestarts0, same boot/config/deployment. Fresh canonical deployment VERIFY is VERIFIED (131 installed RECORD files/28 protected exact dependencies). Short restoration CPU accounting was unavailable; it is not represented as zero. The [final independent restored review](../reviews/2026-10-09-q7-posttarget-restored-independent-review.md) ACCEPTS the complete actual handoff.

## Immutable evidence

Runtime source `49b4a2db39b47cef5350c3791e56fa0f19bd209c`; wheel `c7d354be3ce264d95046ba8e026f9970e825170910c60bfdd37a77c6979339d3`; deployment identity `7072575b248d1c0b3fccaf984189a50342fd3479e9508a0d328ca27f1527133e`.

Private VPS evidence is under `/srv/recorder-data/recorder-archive/evidence/Q7-optimized2h-causal-retry-20261008-prlintz2/BASELINE-WINDOW2-pvBxZSlJ/POSTTARGET-DATA-AUDIT-20261009`. No Raw/archive payloads or private configuration are published in this repository.

| Evidence | SHA256 |
| --- | --- |
| Separate `owner-adjusted-acceptance.json`, PASS_OWNER_ADJUSTED, published06:47:13UTC | `4ed481af9016f54efafcde4da2ffa43c022b1a218769d84e1d922714f11dabaa` |
| Original unchanged INCOMPLETE target | `e7880aa5b0580c34984234eedd9f549a6f30855de502f4885d7696e631fdd20b` |
| Supplementary complete audit root | `5b4474b04b2d3cd9fc1a13f3f62ea91196610a6f259df49634411847314e662e` |
| Completed verification | `d8f985b487bed1d89a2412572f6d999683c90ae184f85c9b16486d997026d36a` |
| Complete control-only bundle,26541 members | `6c92e52d194db31b52549f24e5e04336ebaa6ab02b0caddcb3020323bef63291` |
| Independent full audit report / identity | `33b8bfa0004ed2a728c8fca500edb917388c1fdcdeb16ae5d2bbe9b5db7a82e5` / `ed6daa0cfbbde8a25bd0820deb7269a5c32ced27e7fe9d64c6ab38be07ef2fd7` |
| Actual OS restoration | `6dbc3ff46666078d016cfdc3cc156f0074cf90deb23be253c6cb3d79b369d7b5` |
| Actual restored handoff | `e26f98a26367a96ca7fd0c8442908d75634c3179f15699130508ad73af841c7a` |
| Fresh canonical deployment VERIFY | `944c76920bf069808b1111544b267c2faa4b9c8c501fc63edf6be6d5c89613e7` |
| Final restored9-member control-only bundle | `92b880e3b8d52f2e1f32e87b53a37e8d7646c5c3945333e51a50d60d472f0d7e` |
| Independent restored report / identity | `2aba6b490372a922eae77a31bfb640476b97ce3782561bb9a7fb3716ae47a7e0` / `a5b1b4cad8d0b6803a1b022c8675e7661290a5f54da27e6ee2f9dccd94091d45` |

No production code, wheel or lock changed in this supplemental closeout. The existing exact dual CI/cloud release qualification is retained; documentation-only validation checks links, receipt bindings and whitespace. Mark Q7 complete under the owner's criterion, push main and pause hourly continuation. Stop here; do not start another stage.
