# Q4 — Corrected Spot idle source: reviewed VPS Formal2h

Q4 PASS, October 4, 2026 (Shanghai). The corrected artifact completed its own
7200.001928737-second target, both full LIVE terminal passes, completed
verification, independent GPT-6.1 Sol xhigh LOCAL/OFFLINE eligibility review and
actual restored-authority handoff. Q0–Q4 are complete; corrected-artifact credit
is 2h/38h. Q5/12h and Q6/24h are NOT_STARTED; `PRODUCTION_READY=NO`.
The original cf3909e9 PASS remains historical and contributes no replacement
identity or duration credit. No runtime/config change or corpus reset occurred
during this stage.

## Frozen artifact and lineage

Host greencloud-tokyo-01, Ubuntu 24.04 x86_64, four logical CPUs/about 6GB RAM,
boot59bb1735-ac48-408d-b3ec-bda79fc49b41. Source
23ea5c40541b93563f5bda414c6691ba44af5b04, tree
8e038727b4260cacb7aa3fa151d6143f624ed515; local1998 and exact-source
CI37130186751 both platforms PASS, affected cloud233/clean locked wheel PASS.
Independent source/deployer/helper reviews ACCEPT. All14 auxiliary flags and
Spot/USD-M BTCUSDT/ETHUSDT four products remain enabled.

| Binding | SHA-256 |
|---|---|
| Wheel |bb7e357edfb4a20b09bcda64f5df18bcf59f096bdd290b7ba8010bb4645e52a0|
| Dependency lock |44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335|
| Config |5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419|
| Deployment identity |0ea93b2c15e35e949e3bd5dc4cb1c8cee96aa5544f5d51e6ace33ba3d965fbee|
| Accepted post-warm-up baseline |a31e957190380a911e78e97e623011abf59a6f7d247c93cbaf7d9722b69c18ce|

The growing corpus and all old Raw/archive/failure/custody evidence were retained.
Engineering and authoritative post-warm-up baselines, actual all-enabled
normal/missed/catch-up warm-up, resource admission and finite quiet arm were
independently accepted before T0; see the [correction record](Q4-spot-idle-correction.md).

Stage `Formal2h/2h-e77218f9d1fa4c78a8a48ce8bafdaf27`, run
1eec7d8fc0f24f959ec973933c103ff0. T0 is October3,18:34:56.795853724UTC;
BOOTTIME117390976389278ns. Target20:34:56.797775863UTC, elapsed
7200001928737ns. Recorder invocation42ba1999d8a74b01ba40a37a37e7c960/PID68629,
observercee0971f77774169b083b99fa4297a48; stable boot/invocation/NRestarts0.
Fresh strict core4/aux26 and endpoint PASS. Endpoint20:35:18.531684593UTC,
automatic stop/disable20:35:33.660415171UTC. No restart or duplicate T0.

## Full terminal integrity and actual cost

After natural archive drain, its timer was paused. Recorder remained inactive
AND disabled. Native terminal unit
`binance-recorder-q4-spot-idle-formal2h-terminal.service`, invocation
e03d18ea4bf444a9a7cea0b142f0eccd, ran21:04:36.081402–21:54:41.597133UTC.
Typeoneshot/TimeoutStartinfinity and the unchanged900s no-progress authority were
used. Finalize performed producer plus independent LIVE reconstruction before
publishing the final. Completed CLI verification then reconstructed frozen
controls/proofs; it was not a third Raw scan.

Actual SOURCE wall3005.515731s (50m05.516s), CPU2899.088622s, cgroup peak
1239740416B including cache, swap0. Completed verify took190.516999s.
Both native wrappers report PASS_CANDIDATE/eligible_for_next_stage=true/blockers[].
Terminal COMPLETE:114303 ordered unique records/241 shards,6718 manifests/chunks/
locations/transactions;53744 chunk transitions/33590 archive events/97 operational
records. All6718 chunks and transactions LOCAL_DELETED,10151154 frames,
552193809 stored bytes,7833293106 uncompressed bytes. Raw local bytes verified0
reflect retired local sources; all552193809 archived bytes received full native
framed-data verification. Audit roots earn zero duration credit.

Whole-corpus flags remain visible:1269 gap/incomplete and64 zero-frame manifests;
eight completed discontinuity pairs are historical, with no new current-run pair.
These counts include the retained growing corpus and do not label all its
intervals complete. Catalog integrity/FK checks pass; no partials/backlog.

## Coverage and resource limits

All25 online samples and239 in-period auxiliary checks reconstruct with no
blockers. Thirty-second sampler span7155.693792741s, maximum spacing30.113477s;
strict endpoints PASS. Three in-period typed-empty retries and two premium-index
NetworkError missed snapshots are retained. BTC recovered in5.049598227s and
ETH in5.037137524s. No snapshot backfill, suppressed error, generic retry exception,
or continuous/every-event completeness claim is granted. No current-run WS gap
appears in the verified controls. See the [online review](../reviews/2026-10-04-spot-idle-formal2h-online-independent-review.md).

Actual sampled host execution15.78%, steal2.07%, minimum available RAM
4760264704B, Recorder RSS maximum284667904B. Whole-host swap274432B with zero
swap-in/out pages over the sampled span; audit cgroup swap0. Sampled I/O pressure
and co-resident work remain visible. These measurements support this2h scope;
they do not certify later peak capacity or justify a speculative language/server
change. R-078 remains OPEN. Before later stages, reforecast actual reserve,
temporary/staging/backlog/retained controls and full-audit windows using this
larger measured scope; the prior38h peak reserve failure remains unresolved.

## Restored normal authority

Reviewed controlled helper returned RESTORED22:00:16.698841510UTC. Only then was
the unchanged absolute23:31:19UTC expiry cancelled at22:00:53.894653775UTC;
inactive readback22:00:53.905688603UTC. No extension. All five approved runtime
masks removed; original apt timers/unattended-upgrades enabled and active.

Archive timer resumed with an actual22:00:53UTC trigger. Its journal proves four
natural BACKLOG_EMPTY/success workers before the handoff, without direct worker
start/stop. Handoff22:04:51.880189757UTC has timer enabled/active and finite future
deadline32.872494s. Recorder inactive AND disabled/MainPID0/NRestarts0, same
verified deployment/config/boot, Catalogok/0partials/backlog/all6718 archived.
Writer free29359800320B/archive free2100969160704B. Earlier operator-only readback
assertions about a prior worker invocation and an active normal recurrence were
not published as acceptance receipts; no production or evidence contract changed.

## Immutable evidence and independent acceptance

VPS root `/srv/recorder-data/recorder-archive/evidence/Q4-spot-idle-20261003-Xo1tH7WN`;
local cache `/Users/amada/Library/Caches/BinanceMarketDataRecorder/spot-idle-fix-20261003`.

| Item | SHA-256 |
|---|---|
| Stage start |71a8fe11b3b6ae38430a31f5aeb7a86dd81c24e8c9f2ba46de29fc3264bebf06|
| Stage target |30fd63c456420de7e027feab5e623c88f2416797641068a82965d891eda573f9|
| Terminal audit root |d7b460fb92c47c8fc58e0127725bdceb442b9fbd4935f9b7f0f377b040eb5574|
| Stage final |648891891da2659b7d5e84c86692bdc5e4cb1f97bb2d815920bc9c0fac0b00c2|
| Native finalize wrapper |5599c8ddb1f0eb4ccb2f0fb8de273e93a70c14dc61a9f57ccbf8dd7bcc388e6c|
| Native completed verification |07b36ab3af22cda153c876a31bf5bfe5e225508001fad4546caaaeda51025e5a|
| Completed controls bundle,74065928B |b1789251f4264839c1c1dc27ab7eb7a63468d96a6ba46e158a9f138e64481824|
| Handoff controls bundle,8189B |bfba258f781d3ae944fa8c3dbceaa3baced65fb57bd059fa3acba4ad2c705e8d|
| Controlled OS restoration |7deefd76d396db73280757608b96ad89778185f572412c548d5fcc5054ce18b0|
| Expiry cancellation/archive resume |0a1826a378db4a27c8520da28fbe8e0f4460edfe2d91fca2975c6c3738678082|
| Restored handoff |f0012863c04876cc03d2e296867cdeb0e8c10b7b7aa7b824d00444ad1f78ba80|

Independent original `verify_completed_v5_stage(expected_stage='2h',
require_eligible=True)` PASS36.722126s, including exact predecessor reconstruction.
All4814 accepted online input files remain byte-identical. Guarded Raw/network/
production opens/outside-private writes/reconstruction processes0; no third
production scan. Native eligibility and actual restored handoff independently
ACCEPT; [final report](../reviews/2026-10-04-spot-idle-formal2h-independent-review.md),
SHAe876c551f28e134d98db89116250cb8f0e251c7ec766cb7e761d0d538494c8f1;
review identity019b49d4f10939987388f7ce2da8f3da51ef58fc270a3b3994e0985f884ad73b.

This record completes the authorized continuation. Pause hourly `vps-2h` after
the closeout is pushed to main; historical tasks remain paused. Stop before12h.
