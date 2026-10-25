from pathlib import Path
import hashlib,json,os
R=Path(__file__).resolve().parent
H='d103983ac1af2f14064617e708860bcb1b9ea317d8899811c100128946219365';M='f8e10a42807ae57cb2c80c29efdfede84b4f07359356d15b77d0921a04eec40c'
def digest(p):return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
manifest=R/'inputs-initial-unexecuted/preflight-design-manifest.json';assert digest(manifest)['sha256']==M
for n,v in json.loads(manifest.read_text())['members'].items():assert digest(R/'inputs-initial-unexecuted'/n)==v
C=Path('/Users/amada/.codex/worktrees/r2-close-diagnostics/BinanceMarketDataRecorder')
refs=[]
for relative in ['src/binance_market_data_recorder/cli.py','src/binance_market_data_recorder/service/lock.py']:
 p=C/relative
 refs.append({'path':str(p),**digest(p)})
(R/'source-static-references.json').write_text(json.dumps({'scope':'STATIC definition-only deployment verify and lock compatibility inspection, no source QA rerun','references':refs,'candidate_module_imported':False,'source_executed':False},indent=2)+'\n')
findings=[
 {'id':'F1','priority':1,'line':97,'title':'Incomplete active-tree inventory can be accepted as zero partials','detail':'os.walk has no onerror callback and silently skips scan failures; a local PermissionError fixture yields zero files and zero partials. Only the active leaf is checked against symlinks, so a symlinked data ancestor is accepted. A complete fail-closed census and trusted ancestor/entry checks are required before startup_custody_gate may be true.'},
 {'id':'F2','priority':1,'line':83,'title':'The advertised 180-second worker bound is not enforced across I/O','detail':'The deadline is checked only at loop entry, while each state/journal subprocess permits 330 seconds. A state read completing at 181 seconds is accepted without a post-I/O deadline check. Pass strictly positive remaining time into each bounded I/O and check again afterward.'},
 {'id':'F3','priority':1,'line':102,'title':'Worker proof is not coherent with later inventory, owners and timer observations','detail':'After worker2 is observed inactive the helper reads inventory/owners/timer without a final same-invocation worker check. A synthetic timer race publishes an old inactive archive_worker while current owners reports a new active worker. Use a bounded entire-attempt bracket and retain each raced/failed attempt.'},
 {'id':'F4','priority':3,'line':3,'title':'The module contract incorrectly denies deployment verification','detail':'The design actually invokes canonical definition-only deployment verify; update the docstring to match that authorized read-only verification.'}]
result={'decision':'BLOCK','scope':'ORIGINAL_UNEXECUTED_PREFLIGHT_DESIGN_ONLY','helper_sha256':H,'design_manifest_sha256':M,'proposed_scope':'/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt','findings':findings,'isolated_local_case_count':5,'all_expected_original_behaviors_reproduced':True,'pin_argv_definition_only':True,'service_lock_compatible_with_deployment_verify':True,'remote_executed':False,'production_action_performed':False,'raw_or_manifest_or_partial_payload_opened':False,'new_scope_created':False,'formal_credit_seconds':0,'linux_qualification_credit':False,'current_admission_or_handoff_accepted':False,'corrected_successor_not_assessed_in_this_report':True}
(R/'RESULT.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
(R/'REPORT.md').write_text('''# Original unexecuted preflight design — BLOCK

This review binds only helper `d103983ac1af2f14064617e708860bcb1b9ea317d8899811c100128946219365` and design manifest `f8e10a42807ae57cb2c80c29efdfede84b4f07359356d15b77d0921a04eec40c` for proposed scope `R2-capacity-n2-readiness-retry-20261025-1s0kswlt`. The original design was not remotely executed. Its immutable originals are retained separately from any correction.

The design is blocked by three concrete control defects. First, the active-tree walk suppresses scan errors and only rejects a symlink at the leaf, so it can report zero partials without a complete trusted census. Second, the 180-second natural-worker bound is checked only before I/O; each command can take 330 seconds and a late successful result is still accepted. Third, a timer race after the last worker check can make the published inactive-worker proof contradict the later current owner state. The docstring also inaccurately states that no deployment verification occurs.

Five isolated LOCAL fixtures reproduce the ordinary empty synthetic-partial census, silent scan failure, symlink-ancestor acceptance, late successful worker read, and contradictory old-worker/current-owner publication. The fixtures execute only AST-extracted inventory/wait/handoff components with synthetic files, clocks, service state and D-Bus objects. No module main or whole remote program was executed; no network, real process/service, production data or payload was accessed.

The canonical pin argv is definition-only: LoadUnit/RefUnit/UnrefUnit surround canonical `deployment verify` under the installed environment as bmdr, without starting Recorder. Static production source inspection confirms deployment verification does not acquire the runtime service lock, so the outer exclusive lock does not introduce a lock deadlock.

Before execution, require fail-closed traversal with ancestor/entry checks; remaining-time command bounds and post-I/O deadline checks; and one bounded worker/journal/inventory/owner/timer/final-worker coherence attempt that retains failed or raced observations. A corrected successor requires its own exact-hash review. Nothing here accepts an actual current handoff, Linux qualification, startup, duration, saved-data correctness or capacity. All failed-run data, duration and closed guards remain unchanged.
''')
# This freezer itself is retained as a reproducibility artifact.
items={str(p.relative_to(R)):digest(p) for p in sorted(R.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'}
(R/'MANIFEST.json').write_text(json.dumps({'kind':'INDEPENDENT_ORIGINAL_UNEXECUTED_PREFLIGHT_DESIGN_REVIEW','decision':'BLOCK','members':items},indent=2,sort_keys=True)+'\n')
for n,v in items.items():assert digest(R/n)==v
for p in R.rglob('*'):
 if p.is_file():p.chmod(0o444)
for p in sorted([x for x in R.rglob('*') if x.is_dir()],reverse=True):p.chmod(0o555)
R.chmod(0o555)
for n,v in items.items():assert digest(R/n)==v
print(json.dumps({'directory':str(R),'member_count':len(items),'REPORT':digest(R/'REPORT.md'),'RESULT':digest(R/'RESULT.json'),'MANIFEST':digest(R/'MANIFEST.json'),'all_members_readback':'PASS'},sort_keys=True))
