# ADR-0043: Audit only a new owner-policy capture

October21 correction: a post-boundary discontinuity completion may depend on a
START recorded before the capture boundary. Retain only that exact paired START
as causal metadata authority with its original row values and sequence ID. This
does not add a prior chunk, Raw frame, predecessor qualification or duration.
Missing/invalid pairs still fail the unchanged authority validator. A previously
frozen selection can be repaired only in a new private child using hash-bound
original frozen metadata, preserving original failed controls and the exact Raw
membership. Sequence triggers/counters/schema and all unrelated rows stay intact.
See [N1 repair](../milestone_acceptance/R2-n1-causal-audit-repair-20261021.md).


Status: accepted design/implementation/copy qualification, October12,2026. Actual capture/target/data/restoration qualification remains separate.

The owner requested a fresh12h test without analyzing historical data. The Recorder deployment and data directory remain unchanged. Existing data is retained; the private test controller defines a new capture boundary before enabling Recorder.

The boundary contains the three Catalog cursors and a hash-bound, read-only metadata backup. Terminal membership must equal both the start/end chunk-ID set difference and the post-boundary initial creation transitions. Missing initial rows or missing prior identities reject qualification. A private Catalog copy retains original selected chunk/lifecycle/archive/operational row IDs and values, schema and storage authority. Old metadata is copied only to prove selection; old Raw and manifest contents are not traversed. Every selected chunk, including zero-frame and gap/incomplete chunks, remains disclosed. Total selected saved frames must be positive.

All private SQLite snapshots use DELETE journaling and close before fsync/hash or immutable reads. Consistent terminal snapshots and stopped-mutator quiescence surround the selected manifest freeze. Production Catalog is not modified by this adapter.

The released run_audit performs producer LIVE and independent LIVE qualification on the exact selected manifests/Raw. Its complete PASS_CANDIDATE with no findings is followed by hash-bound immutable-control verification, matching the released completed-baseline CLI semantics. That final verification does not perform an unnecessary third Raw traversal. The owner receipt labels two LIVE passes and its verification mode explicitly.

Saved-frame contract conformance uses the complete selected frame denominator (including this attempt's warm-up and12h capture); monitoring availability has a separate denominator. No claim is made about every exchange event, pre-start stream context, canonical V5 online reconnect enforcement, or next-stage predecessor eligibility. Full final independent control reconstruction and actual restored handoff are still required. This scope does not change the production transport, configuration, wheel or language.

Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE review ACCEPT: report SHA `dd376fa04f93da9da95acfb73e77493950fe758141ef50da31ad4f304204837e`, LOCAL69 and saved Linux41 cases PASS. Exact adapter SHA `936e614aefe7e3f0e128dd4f0317a955afcaff61240410a8683b8e5e5ba12588`; scoped binding SHA `4e3f4b3ba5e0e78141c31175dc2f4f3a7b7c97d9b7a9ed19db70903b2f98544d`. See [review](../reviews/2026-10-12-q8-new-capture-scope-review.md).


The October12 actual target reached its own43200.136582231s. Its private terminal entrypoint lacked a main guard; real spawn re-import repeated preflight publication before the Raw worker began. Guard all runtime entrypoint operations with main()/__main__. The prior mocked Raw fixtures did not cover this entrypoint and are not evidence of spawn safety.

An affected audit-only continuation preserves the original failed controls and completed capture. Copy only the verified immutable selected Catalog, preflight, scope receipt, exact mapping shards and referenced selected objects into an exclusive child. Restoration changes the real systemd quiescence certificate, so the child records a fresh genuine certificate and explicit provenance to the unchanged original start/target/corpus. Never overwrite the old certificate, fake the probe, reselect membership or create another capture boundary. The unchanged released auditor performs both complete LIVE passes and completed immutable-control reconstruction. New protection has separately reviewed bindings and current physical/time admission; the original closed window is never reused or extended.

The continuation helper and22 focused tests pass LOCAL Python3.12/Ruff/mypy. Exact private entrypoint Linux3.12.3 four-case real nested-spawn regression PASS (synthetic endpoints, helper bytes bound); actual helper/default executor synthetic proof is independently tested separately. These are repair qualification, not actual saved-frame acceptance. Actual native copy/arm/dualLIVE/final verification/independent restored receipt remain separate pending gates.

Actual affected-closeout dispatch on October12 17:26UTC exposed a second private entrypoint packaging defect: `operator.py` shadowed the Python standard-library module under the installed interpreter. Imports failed before selected preparation or Raw reads; automatic audit-exit restoration completed before cancellation. Rename this private arm script to `arm_audit.py`, preserve the failed child, and use a separately bound exclusive continuation child. A fresh installed-interpreter import probe must expose the entire exact directory, without module stubs, isolated-path flags or preloading the standard-library module. This naming correction changes no production source, selection, capture duration or audit algorithm.

## Actual owner-policy acceptance

October13 Q8 COMPLETE_OWNER_NEW_DATA_ONLY: sole run9e048 own43200.136582231s and current-attempt selected dualLIVE/completed immutable verification plus independent restored closeout ACCEPT33723537/33723537 saved frames100%. Preserve original failures and2645 gap/incomplete plus16 zero-frame manifests. Scope includes original stop/drain tail without extra duration credit; monitor1434/1440 remains separate. No production source/wheel/config change, canonicalV5/predecessor/every-event/24h qualification. See the current Q8 qualification and final independent review.
