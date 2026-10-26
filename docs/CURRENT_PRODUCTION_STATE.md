# Current Production State

The [production plan](production_milestone_plan.md) governs future development.
P0 aligns the development base with accepted remote main `01d7421`, after preserving
user changes as `d81e7d2`; the merge base for new work is `113cdb3`.
PR #86 merged as `004c1a6`. See the [P0 result](milestones/P0/README.md) for delivered
evidence and the [review](milestones/P0/review-20261026.md) for completed R1–R4
corrections. P0 is complete; P1 and P4–P6 qualification have not started.

The installed-source observation at 2026-10-26T08:41:57Z is in the
[P0 environment evidence](milestones/P0/baseline/environment-final.json): Recorder
inactive, disabled, PID 0, NRestarts 0; installed source `20e57f3` still matches all
124 Python files, distinct from the selected development source. A new isolated
v3 fixture and separate profile completed without deployment or service changes.
The [2026-10-26T15:06:33Z observation](milestones/P0/corrections-20261026/benchmark/completion.json)
again records inactive, disabled, PID 0, NRestarts 0; it is not a repeated installed-
source audit. The unprofiled run is STEADY; the profile is NON_STEADY and excluded
from performance comparison.
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
