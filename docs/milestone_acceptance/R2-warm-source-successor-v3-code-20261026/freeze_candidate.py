from pathlib import Path
import json,hashlib,difflib
p=Path(__file__).resolve().parent;v2=Path('/private/var/tmp/bmdr-r2-warm-successor-v2-20261026-8af9t1z7')
subjects={n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['runtime/controller.py','runtime/warm_readiness.py','runtime/warm_completion.py','runtime/owned_command.py']}
for r in json.loads((p/'PARENT_LOCAL_EXECUTIONS.json').read_text()):assert r['returncode']==0 and r['subjects_SHA256']==subjects
counts=json.loads((p/'CURRENT_LOCAL_COUNTS.json').read_text());assert counts['total']==105 and counts['subjects']==subjects
review=Path('/private/var/tmp/bmdr-r2-warm-successor-v2-review-20261026-ztq0xae1');rb=(review/'MANIFEST.json').read_bytes();assert hashlib.sha256(rb).hexdigest()=='d57f165fb38b59cc9ae6e347b1dae7b21e35cb8c3c70732d406f0f641d49c9d4';rm=json.loads(rb)
for n,a in rm['members'].items():
 b=(review/n).read_bytes();assert len(b)==a['bytes'] and hashlib.sha256(b).hexdigest()==a['sha256'],n
inputs=json.loads((v2/'INPUTS.json').read_text());inputs.update(previous_V2_manifest='4f73a416b00522c9afd99fb62b34a0ad981678ac6d7a7234161fa4b0081c6309',previous_V2_BLOCK_directory=str(review),previous_V2_BLOCK_manifest=hashlib.sha256(rb).hexdigest(),all120_V2_review_parent_SHA_bytes_PASS=True)
(p/'INPUTS.json').write_text(json.dumps(inputs,indent=2)+'\n')
delta=''
for n in ['warm_readiness.py','owned_command.py']:
 delta+=''.join(difflib.unified_diff((v2/'runtime'/n).read_text().splitlines(True),(p/'runtime'/n).read_text().splitlines(True),fromfile='BLOCKEDv2/'+n,tofile='v3/'+n))
(p/'V3_DELTA.diff').write_text(delta)
(p/'CANDIDATE_V3.md').write_text('''# V3 narrow correction of frozen V2 BLOCK

V2 manifest4f73 remains independently BLOCKc3187d5f/resultabe9419e/manifestd57f165f, all120 complete members parent SHA/bytes PASS. Its two findings were an accepted abnormal keeper death and lost inventory originals. Earlier2f75 BLOCK617d/all100 and all prior code/reports remain preserved. V3 changes only command success validation in warm_readiness and bounded ps evidence custody in owned_command; controller eb8f321a, read-only child f8c7fc85, source20e57 and formal monitor/target/stop/audit code remain unchanged.

Successful command eligibility now requires exact integer inner/keeper returncode0, typed positive process IDs, explicit normal lease release, no group KILL, no live residuals/cleanup error and coherent original control PID/returncode. The actual inner0/control/release then keeper SIGKILL case is now rejected. False/bool/mismatched fields also reject. This does not relabel a clean failed command as successful.

Each bounded ps attempt retains exact original stdout/stderr or timeout prefixes with base64/size/SHA, explicit availability/partial flags, returncode/exception/parse error, clocks and matching parsed members. Failed/malformed inventory still fails closed. The final command receipt contains all attempts; the existing bound of128 receipts and the same timeout/cleanup/warm budgets remain. The three invalidUTF8/timeout/nonzero injected responses are retained in complete owned-command failure receipts rather than discarded. Actual lease groups are cleaned in those fixtures, without signalling unrelated groups.

105 parent LOCAL cases are bound to exact subjects:63 complete warm-prefix/end/start cases using the real source evaluator with explicit simulated OS/static/clock providers,20 boundaries,8 real macOS POSIX lease cases,11 keeper success/abnormal-exit/protocol cases and3 ps-custody fault cases. This is not Linux, exact runuser, real BOOTTIME, formal duration or every47 operational specification qualification. Guardian guarantee remains limited to its retained session/group, with exact Linux runuser/helper retention a future mandatory gate. Earlier static installed/Catalog subproof scope and all fresh source dynamic/pair/commit checks remain unchanged.

This is a separate LOCAL component candidate with failedP3 common constants retained only as OFFLINE references; forbidden for copy/ARM/launch. Independent narrow delta/protocol/raw-custody review is pending. Any accepted component still needs new reader/all warm metadata and commit exports, genuine affected Linux/import/origin/latency/group tests, fresh stopped custody/handoff/current budget/admission and a new exclusive finite scope/window before newown86400. No production source/wheel/configuration/service/Raw action occurred and no new T0/end time/data/capacity/stability or hardware maximum credit exists. All995/2880/own86400/min+60/full new-data audits/restored receipt/main requirements remain.
''')
names=['INPUTS.json','CANDIDATE_V3.md','V3_DELTA.diff','ORIGINAL_CONTROLLER.py','source-module-hashes.json',*subjects,'test_full_flow.py','test_boundaries.py','test_owned_regressions.py','test_keeper_commit.py','test_inventory_evidence.py','LOCAL_FULL_FLOW_RESULTS.json','LOCAL_BOUNDARY_RESULTS.json','LOCAL_OWNED_GROUP_REGRESSIONS.json','LOCAL_KEEPER_SUCCESS_REGRESSIONS.json','LOCAL_INVENTORY_EVIDENCE_RESULTS.json','PARENT_LOCAL_EXECUTIONS.json','CURRENT_LOCAL_COUNTS.json','ABNORMAL_KEEPER_RECEIPT.json','freeze_candidate.py']
m={'kind':'V3_NARROW_COMPONENT_PROTOCOL_AND_INVENTORY_CUSTODY_CORRECTION_PENDING_REVIEW','members':{n:{'bytes':(p/n).stat().st_size,'sha256':hashlib.sha256((p/n).read_bytes()).hexdigest()} for n in names},'LOCAL_cases':105,'Linux_runuser_copy_ARM_launch_qualified':False,'formal_T0':False,'previous_V2_BLOCK_retained':True}
(p/'CODE_MANIFEST.json').write_text(json.dumps(m,indent=2)+'\n')
print(json.dumps({'candidate':str(p),'manifest_SHA256':hashlib.sha256((p/'CODE_MANIFEST.json').read_bytes()).hexdigest(),'members':len(names),'subjects':subjects,'LOCAL_cases':105,'review':'PENDING'}))
