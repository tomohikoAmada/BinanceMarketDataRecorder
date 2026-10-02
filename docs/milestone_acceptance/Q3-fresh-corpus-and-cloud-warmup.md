# Q3 — Fresh corpus, exact deployment and nonformal warm-up

October 2, 2026. **PARTIAL_NOT_ACCEPTED.** Predecessor Q2 is complete;
this record does not close Q3 or authorize Formal T0. The owner authorized
continuation through eligible Q4 2h, stopping before 12h. The cloud warm-up
failed its actual delta-capacity gate; Recorder is now stopped and disabled.

## Completed operations

- PR #79 merged at `8c9310554a559369c8708a30e6a79fdcb379d8e2` after passing
  exact-head CI. Only local/remote `main` remains; no open PR. The merged PR
  retains its review history. Ancestor-only local backup/WIP branches were
  deleted; the original stash, six untracked review bundles and detached
  historical worktrees remain. Documentation descendants are not runtime wheels.
- Installed the reviewed pending Ubuntu packages, with no removals, and
  performed the required reboot while Recorder was inactive/disabled. New
  kernel is `6.8.0-146-generic`; boot ID
  `11b990f3-68bb-43fa-ac82-03e94ea6962a`. The post-reboot gate had zero pending
  upgrades, clean dpkg and no reboot requirement. Normal update authority was
  restored. Repeat these checks before any later identity-sensitive run.
- Verified the old 143,362 chunks/transactions were `LOCAL_DELETED`, with no
  active partials or unarchived source. Inventoried 143,415 metadata files
  (1,075,649,969 bytes), then atomically retained the entire old writer at
  `/var/lib/binance-market-data-recorder-custody-q3-20261002-ccf48ee63f5d`.
  Full metadata hash/owner/mode readback PASS in 44.08s; inventory SHA-256
  `929f0596a795d0f6d1cd824a72eba9bcc22366993eeb7afe04f651a0be5e226e`.
  Old archive and evidence remain at their original locations. No old Raw
  audit PASS is claimed, and no physical deletion occurred.
- Initialized a fresh Catalog/layout at the same canonical writer root and
  installed the **one frozen Q2 wheel** into a newly created canonical venv.
  Old relocated venv is inert custody, not an executable rollback.

| Authority | Value |
|---|---|
| Installed source | `d0f455c1a417cc1a184c47b6ff766a60f3dc0159` |
| Tree | `cf3854d7fc032de79096dced8e002a6cd55e65c3` |
| Wheel SHA-256 | `b286923d3dc777bf2e3d63ea661effd7cf389137aaf81883519c3a071d449921` |
| Lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Config SHA-256 | `4dbbb6bf415b209635857df6cf537478b03f262b2c3a88553ac77bb9c1c0d781` |
| Deployment identity | `6ab685ed3bc51c9787a52e461607aafe99c35312d2f6e3a489ac1ae4ab79f40e` |
| Python | 3.12.3, noneditable wheel, 28/28 locked runtime distributions |
| New archive directory | `/srv/recorder-data/recorder-archive-qualification-q3-d0f455c-lg427ebp` |
| Storage ID | `57512e1d-59cc-4aff-bf3b-49ae84a7a44e` |
| Volume UUID | `95A2CE20-BF7A-4FAE-98E8-D208517AE318` |
| Marker nonce | `c2f7a682f0f349af8280185dba147ada` |

Config bytes are unchanged: four ProductKeys, twelve core WebSocket contexts,
public depth snapshots, all auxiliary kinds disabled. Exact stopped deployment
verification passed before warm-up and again at closeout. Storage probing used
the existing registered-folder fsync/readback/rename protocol.

## Actual warm-up result

The separate engineering-only empty baseline and its completed verification
passed, then all four products reached READY. This baseline is **not** the
authoritative post-warm-up predecessor. All warm-up data belongs to the new
growing corpus and earns zero Formal credit.

The first operator helper omitted its multiprocessing main guard. Spawned
children reentered the script and failed on its exclusive log creation. That
failed helper/log remains preserved. A corrected guarded helper used a separate
attempt-2 root and the actual V5 observer plus independent online replay.

| Complete observation | Producer + independent replay | Document bytes | Pending references |
|---|---:|---:|---:|
| Engineering start | 48.04s | 512,523 | 0 |
| Immediate | 12.48s | 102,128 | 0 |
| Normal 300s interval | 54.39s | 7,491,191 | 343 |
| Missed cadence, 550s interval | 87.35s | 7,601,340 | 685 |

The next catch-up observation failed **before publication** with
`AcceptanceError: pending causal reference cap exceeded`; observer exit 1.
No target or final exists. Previous documents had no blockers and were within
the byte/time bounds, but their growing pending work does **not** pass the
steady/missed-cadence recovery gate. Recorder PID 3106, InvocationID
`5cf32b32b93341d09ccc98f783c98b47` stayed stable with `NRestarts=0` until
the controlled operator stop.

The archive timer initially reported active with no future deadline after the
long stopped/rebind interval. The first two observations consequently do not
qualify concurrent archive-load performance. Explicitly starting the existing
verified worker once processed 110 files/25,271,673 bytes in 11.26s, with zero
failures/backlog. Subsequent automatic timer firings were verified; the normal
and missed-cadence observations included archive activity. Before any next T0,
verify a finite future timer deadline and actual automatic recurrence; prime
the existing worker if needed. No timer scheduling root cause is asserted.
The retained [systemd v255 timer source](https://github.com/systemd/systemd/blob/v255/man/systemd.timer.xml)
describes `OnUnitActiveSec` relative to service activation; it is corroboration,
not an interleaving trace. Source SHA-256:
`d00dd4ed9d5dd84cddb3943c014f41273bd754277f765e0e423748f9076a7c93`.

## Performance diagnosis and remaining gates

155 resource samples show peak Recorder RSS 228,929,536 bytes, combined
observer/child RSS 399,953,920 bytes, and combined sampled process RSS
742,944,768 bytes. Available host memory never fell below 4,991,655,936 bytes;
swap remained unused. Shutdown-flushed stream metrics show maximum queue depth
255 of configured 8192. These are bounded warm-up measurements, not long-run
leak or server certification. Market/stream metric groups aggregate products;
manifest/Catalog totals below retain ProductKey identity.

After verified drain, 448 chunks contain 2,178,806 frames, 1,632,730,464
uncompressed bytes and 105,244,226 stored bytes. These are metadata totals,
not a completed full Raw audit. New data, including failed warm-up capture,
must be retained and included in subsequent baseline/audit forecasts.

Code and actual document inspection show family-ordered byte admission spends
most of the 7 MiB budget on chunk entries, leaving fewer archive entries.
The same complete archive lifecycle bundle is repeated across related entries;
pending cross-cursor references grow. The cap also applies during replay of
one family before later families can discharge references. The failed
unpublished observation cannot establish its final reference count.

A read-only serialization diagnostic on the two actual documents estimates
that sharing identical transaction bundles **within one document** would reduce
entry/table bytes from about 7.33 MB to 1.91 MB / 2.25 MB. This is an optimization
hypothesis, not an implemented codec or throughput/correctness PASS. First
reproduce both byte starvation and intermediate causal-cap behavior offline;
evaluate a small finite representation/admission correction, preserve exact
rows/proofs and old-policy replay/resume, then review/freeze a changed release
and repeat real cloud gates. Do not merely widen caps, relax cadence, switch
language or upgrade the host. There is no measured hardware-exhaustion cause.

**Unrun/not passed:** sustained and missed-cadence drain, size-triggered/burst
qualification, cumulative 2h/14h/38h complete-audit/capacity forecasts,
authoritative stopped baseline/verification, bounded maintenance quiet window,
fresh post-baseline readiness, Formal Q4 2h, Q5 12h and Q6 24h. The historical
143,362-chunk R-078 remains open. Q2 checks still describe their exact artifact
and do not substitute for this failed cloud gate. No production code changed
in this Q3 operational record; no source test rerun is claimed.

## Controlled closeout and evidence

Recorder is inactive/dead **and disabled**, `MainPID=0`, `Result=success`,
`NRestarts=0`. New Catalog integrity is `ok`; all 448 chunks/transactions are
`LOCAL_DELETED`; active partials and archive backlog/pending/failure counts are
zero. Archive timer is enabled/active with a future deadline. Both apt timers
and unattended upgrades are enabled/active; no runtime masks remain. Diagnostic
resource sampler is stopped; the failed observer units are retained. Old
continuation automation remains paused.

Evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q3-fresh-d0f455c-20261002-YgstkmRP`.
Immutable attempt-2 `closeout.json` SHA-256:
`b6bb1fac567403044f7c38f74c565fded374efb139fa11a1f8ff389796f56eb9`.
[Compact readback](../reviews/2026-10-02-q3-warmup-closeout.json) binds retained
logs/resources, actual documents, storage identity and stopped state.
`deployment-verify-closeout.json` is additive evidence following that snapshot.

```text
Q3=PARTIAL_NOT_ACCEPTED
FORMAL_V5_STARTED=NO
FORMAL_V5_CREDIT_SECONDS=0
12H_STARTED=NO
PRODUCTION_READY=NO
NEXT=Q3_OBSERVER_BYTE_AND_CAUSAL_CAP_OPTIMIZATION_REVIEW
```
