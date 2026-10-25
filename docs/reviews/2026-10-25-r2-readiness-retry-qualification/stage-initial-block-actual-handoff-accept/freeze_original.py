from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parent;I=R/'inputs'
def d(p):return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
actual=json.loads((R/'ACTUAL_RECONSTRUCTION.json').read_text());checks=json.loads((R/'ORIGINAL_STAGE_CHECKS.json').read_text());m=json.loads((I/'stage-controls-manifest.json').read_text())
result={'decision':'BLOCK','scope':'ORIGINAL_UNEXECUTED_STAGE_DESIGN_WITH_DISTINCT_ACTUAL_PREFLIGHT_ACCEPTANCE','accepted_preflight_sha256':actual['accepted_preflight_sha256'],'actual_preflight_decision':'ACCEPT','bindings':{'operator_sha256':checks['operator_sha256'],'controls_manifest_sha256':checks['controls_manifest_sha256'],'runtime_members':m['members'],'scope':m['scope'],'source':m['source'],'identity_sha256':m['actual_identity_sha256']},'design_manifest_sha256':'ef4d5e20b05f09d856a48539870dfa229fdb26fdbdf9addaa82f69fbd8c3f07c','findings':checks['findings'],'stage_component_cases':9,'actual_metadata_rows':25639,'raw_pack_occurrences_verified':38,'current_Linux_qualification':'PENDING_NOT_RUN','remote_actions_by_review':False,'formal_credit_seconds':0,'corrected_successor_not_accepted_here':True}
(R/'RESULT.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
(R/'REPORT.md').write_text('''# Original new-stage binding design — BLOCK; actual stopped preflight — ACCEPT

This original unexecuted design binds operator `b4c7e06084c8cb72a2ab0c021bd266f372b80720d62029b7cc4c9c1ebd647b51`, controls manifest `d3ed70e54b3e2fe1131f0fdffe7eaf1b7637aaafc673a188681dc38ad905b54f` and 22-member design manifest `ef4d5e20b05f09d856a48539870dfa229fdb26fdbdf9addaa82f69fbd8c3f07c`. No copy, ARM or launch was executed. The original version remains BLOCK for two design defects, independently of the accepted real stopped preflight.

The new generated copy/ARM/launch programs use a glob-only partial check, which silently accepts scan errors and does not validate ancestors or all entries. This reintroduces incomplete fresh custody and contradicts the promised fail-closed current check. Nine isolated code-generation/binding checks reproduce UNBOUND refusal, four compile-only programs and silent scan-error acceptance. Replace those fresh gates with one fail-closed metadata-only inventory and preserve observations. The read program also omits newly persisted `linux-classifier-qa.json` and `parent-tests.json`; failure before software-copy publication leaves the preserved QA/partial-result evidence inaccessible through this reader. Export those exact controls losslessly.

Other reviewed bindings hold: exactly 11 historical runtime members are unchanged; common.py function ASTs match the prior instrumented controller; only common/policy bindings and previously accepted classifier621 differ. The new Linux39 subprocess is bounded and its original stdout/stderr/partial JSON evidence is published before PASS assertions; it remains unrun. Four generated programs compile, native ARM arguments retain the finite one-shot protection/pin/restoration bindings, and UNBOUND refuses mutation. This does not accept an actual Linux or physical copy result.

Separately, actual preflight `e7dfc0f5aea159eb3fbe8b0d19b3a9c2b6f4fcb149c836eca72578ecf9401bd9` is independently accepted for its current stopped metadata handoff/planning only. The 6,951,497-byte original stdout equals the preserved actual JSON, return code0 and exact accepted abe667 helper/REMOTE hashes bind execution, stderr is empty. All38 raw byte packs and19 command occurrences verify. The one coherent attempt spans0.180774710s; same source20e57/wheeldd2c/identity9b8/config5b73/boot59bb canonical VERIFY and Recorder inactive-disabled/PID0/NRestarts0 hold, with production Stop90 unchanged. The embedded original restoration/cancellation objects match retained independently accepted originals, restore before cancellation. Five owned natural archive records bind worker07dc success/backlogs0; one directory and zero active files/partials supply current custody. The finite future CLOCK_MONOTONIC deadline is25.932545s at collection, not a persistent no-worker guarantee.

All25,639 failed-attempt metadata rows and1,431 creation-minute bins reconstruct the current rate peaks, componentwise historical/current forecast, required221626.19209639446s and newly derived230400s window. Projected writer20,075,891,662B/archive1,957,747,952,848B are planning outputs, not live admission or a universal upper bound. Creation time is not seal time. No Raw/manifest/partial payload is opened and metadata supplies no saved-frame/24h/capacity correctness credit. Current Linux qualification, physical copy, new ARM/window/admission and own86400 remain pending; failed data, old duration and closed guards remain immutable. A corrected design requires a separate exact-hash decision.
''')
items={str(p.relative_to(R)):d(p) for p in sorted(R.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'}
(R/'MANIFEST.json').write_text(json.dumps({'kind':'INDEPENDENT_ORIGINAL_STAGE_DESIGN_BLOCK_AND_ACTUAL_PREFLIGHT_REVIEW','decision':'BLOCK','members':items},indent=2,sort_keys=True)+'\n')
for n,v in items.items():assert d(R/n)==v
for p in R.rglob('*'):
 if p.is_file():p.chmod(0o444)
for p in sorted([x for x in R.rglob('*') if x.is_dir()],reverse=True):p.chmod(0o555)
R.chmod(0o555)
for n,v in items.items():assert d(R/n)==v
print(json.dumps({'directory':str(R),'member_count':len(items),'REPORT':d(R/'REPORT.md'),'RESULT':d(R/'RESULT.json'),'MANIFEST':d(R/'MANIFEST.json'),'readback':'PASS'},sort_keys=True))
