# Current Production State

The [production plan](production_milestone_plan.md) governs future development.
P0 aligns the development base with accepted remote main `01d7421`, after preserving
user changes as `d81e7d2`; the merge base for new work is `113cdb3`.
PR #86 merged as `004c1a6`, PR #87 as `43aab9f`. The [review](milestones/P0/review-20261026.md)
accepts R1/R3/R4 and the [P0 measurements](milestones/P0/README.md); the remaining
R2 classifier defect is corrected with 26 focused tests and an appended v4
interpretation of the same boundaries. P0 corrections and [P1 local capture-path development](milestones/P1/README.md) are complete;
P2–P6 have not started. P1 uses the existing main without branch cleanup.
P1 did not access the VPS, install a candidate, change service state, or touch
production data. Its macOS fixture measurements do not update the installed
source observation below or establish production qualification.

The installed-source observation at 2026-10-26T08:41:57Z is in the
[P0 environment evidence](milestones/P0/baseline/environment-final.json): Recorder
inactive, disabled, PID 0, NRestarts 0; installed source `20e57f3` still matches all
124 Python files, distinct from the selected development source. A new isolated
v3 fixture and separate profile completed without deployment or service changes.
The [2026-10-26T15:06:33Z observation](milestones/P0/corrections-20261026/benchmark/completion.json)
again records inactive, disabled, PID 0, NRestarts 0; it is not a repeated installed-
source audit. The [v4 classification](milestones/P0/corrections-20261026/r2-reclassification.json)
remains STEADY for unprofiled and NON_STEADY for profile, with additional backlog
reasons; producer results remain untouched. This R2 fix did not access the VPS or
change production code/service state.
The P0 benchmark uses an isolated source directory and fresh temporary data only.
It does not deploy or start the production Recorder service.

Historical N1/N2 durations, saved-data results and failures remain under their original
criteria. They do not qualify BTC/ETH's expanded Spot/USD-M/COIN-M scope. Production
readiness remains **NO**. R-078 stays open until the final configured 72h and data
verification requirements pass.

Old live checkpoints and pending private-helper reviews are preserved in
[pre-P0 history](history/pre-P0-CURRENT_PRODUCTION_STATE.md) and
[earlier history](CURRENT_PRODUCTION_STATE_HISTORY.md). Those pending private
helpers are not dependencies of the production plan.
