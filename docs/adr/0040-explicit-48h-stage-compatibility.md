# ADR-0040: Explicit 48h predecessor with historical stage compatibility

Date: 2026-10-05. Status: Accepted for source/compatibility, October8 independent review.
Exact release/live qualification remains pending; Q10 has not started.

The optimized artifact's planned chain is2h ->12h ->24h ->48h. Historical
artifacts used2h ->12h ->24h ->72h ->168h. Inserting48h into a positional stage
list would silently change historical72h eligibility and resumed verification.

Define predecessors explicitly:12h<-2h,24h<-12h,48h<-24h,72h<-24h,168h<-72h.
2h still requires an authoritative baseline.48h target is172800 seconds;
all historical targets and proof schemas remain unchanged. STAGE_NAMES remains
a membership/CLI validation set in its preserved legacy order with48h appended,
not a source of predecessor authority. V1–V5 predecessor resolution uses the
same explicit mapping. Native helpers derive deadlines from the selected
stage duration, never infer a stage from accumulated elapsed time.

No duration/source/wheel/deployment identity is inherited across artifacts.
Completed proof verification, exact predecessor identity, strict readiness,
sampled monitoring policy, two independent full LIVE passes and immutable
completed proof replay retain their existing authority. Existing72h/168h
readers/resume remain supported; a48h final cannot replace their predecessors.

Accelerated offline tests cover all predecessor branches, premature48h finalize,
exact target, resumed T0/start identity, completed replay and closed-stage resume
refusal. Actual48h production certification remains Q10 and is not earned by
offline clocks or by24h plus an unverified additional day.
