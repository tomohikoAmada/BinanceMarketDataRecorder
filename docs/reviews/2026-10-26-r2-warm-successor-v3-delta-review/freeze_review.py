from pathlib import Path
import base64,datetime,hashlib,json,os,subprocess
R=Path(__file__).resolve().parent;C=Path('/private/var/tmp/bmdr-r2-warm-successor-v3-20261026-x7yfquh1')
EXPECTED='b020d893bb4acb01aaa46b3272d4a15b043a483cddfba234715f9736ff3fb164'
def rec(p):
 b=p.read_bytes();return {'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}
def save(n,v):(R/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
assert rec(C/'CODE_MANIFEST.json')['sha256']==EXPECTED
m=json.loads((C/'CODE_MANIFEST.json').read_bytes())
for n,w in m['members'].items():assert rec(C/n)==w==rec(R/'inputs'/n)
for n,num in [('LOCAL_FULL_FLOW_RESULTS.json',63),('LOCAL_KEEPER_SUCCESS_REGRESSIONS.json',11),('LOCAL_INVENTORY_EVIDENCE_RESULTS.json',3)]:
 x=json.loads((R/'run'/n).read_bytes());assert x['case_count']==num and x['all_PASS']
limits=json.loads((R/'INDEPENDENT_DELTA_LIMITS.json').read_bytes());assert limits['case_count']==27 and limits['all_PASS']
full=json.loads((R/'run/INDEPENDENT_FULL_INVENTORY_FAILURE_RECEIPTS.json').read_bytes());assert full['case_count']==3 and full['all_PASS']
count=[0]
def packs(v):
 if isinstance(v,dict):
  if {'base64','bytes','sha256'}<=set(v):
   b=base64.b64decode(v['base64'],validate=True);assert len(b)==v['bytes'] and hashlib.sha256(b).hexdigest()==v['sha256'];count[0]+=1
  for z in v.values():packs(z)
 elif isinstance(v,list):
  for z in v:packs(z)
for file in [R/'run/ABNORMAL_KEEPER_RECEIPT.json',R/'run/INDEPENDENT_FULL_INVENTORY_FAILURE_RECEIPTS.json',R/'INDEPENDENT_DELTA_LIMITS.json']:packs(json.loads(file.read_bytes()))
groups={json.loads((R/'run/ABNORMAL_KEEPER_RECEIPT.json').read_bytes())['owned_pgid']}
for case in full['cases']:groups.add(case['full_owned_receipt']['owned_pgid'])
p=subprocess.run(['/bin/ps','-axo','pid=,pgid=,stat='],capture_output=True,timeout=1,check=True);rows=[]
for line in p.stdout.decode('ascii').splitlines():
 f=line.split()
 if len(f)==3 and int(f[1]) in groups:rows.append({'pid':int(f[0]),'pgid':int(f[1]),'state':f[2],'live':not f[2].startswith('Z')})
assert not any(v['live'] for v in rows)
save('FINAL_RECONSTRUCTION.json',{'decision':'ACCEPT_LOCAL_DELTA_ONLY','bytepacks_full_readback_count':count[0],'all_bytepack_SHA_bytes_PASS':True,'known_fixture_group_ids':sorted(groups),'final_matching_rows':rows,'no_matching_live_members':True,'zombies_not_claimed_reaped':True,'read_only_observation_no_signals':True,'candidate23unchanged':True,'scope':'Evidence readback and LOCAL fixture cleanup only; not operational qualification'})
report='''# Independent exact V3 narrow delta review

Decision: **ACCEPT for exact code/LOCAL component delta only**. No new blocking finding was established in the changed success protocol or inventory-custody paths. This accepts CODE_MANIFEST `b020d893bb4acb01aaa46b3272d4a15b043a483cddfba234715f9736ff3fb164` and its23 bound members for this limited scope. It does not authorize copy, ARM, launch or a new24h clock.

Candidate: `/private/var/tmp/bmdr-r2-warm-successor-v3-20261026-x7yfquh1`. Reviewed warm_readiness is `9305c447d8263a3a7a57a540fd129f1f9a974805c46766745f3e5855b9a79988`; owned_command is `ef2f8882cabbd6becca136b41075e117a28fb9f3c3ed7b8c721d60f283e757bd`. Controller `eb8f321a8c1433987f72e42d2dea1afc2ddc217a2e4e016ac4d5d3a2a9367c4f` and read-only warm_completion `f8c7fc8569d6a0b32cca4cb0e24a5b975e376be2a90f5b97872ce8c4e1d3287d` retain exact V2 bytes.

## Scope and provenance

All23 V3 inputs and12 referenced runtime files were read by full bytes and SHA/size checked. The six exact source maps still match source20e57. Prior V2 BLOCKd57f165f/reportc318/resultabe9 and its accepted parent all120 custody are retained as references; the old review, historical QA, source deployment and actual failed runs were not repeated.

AST comparison confirms warm_readiness changes only command_ok. Keeper main is AST-identical to V2. After removing only the inventory list initialization, extra _members evidence argument and returned inventory list field, run_owned is AST-identical to V2. The remaining owned change is the new raw-byte packing and bounded _members evidence function. Thus prior V2 positive conclusions about static/source pairing, current-publication rejection, original warm bounds and unchanged formal/stop/audit/restore code are reused, not newly expanded.

## V2 abnormal-keeper finding is corrected

command_ok now requires exact integer inner and keeper returncodes0, timed_out is False, lease_released is True, group_KILL_used is False, an explicit empty residual list, no cleanup error, the no-live cleanup state and a successful parse. It decodes the original SHA/size-bound control bytes and requires positive nonboolean integer PIDs and matching keeper/PGID/inner identities and control returncode0.

The provided11-case protocol regression independently reproduced PASS. This includes a real LOCAL inner success followed by actual release flush and SIGKILL of only its still-owned keeper. The returned receipt still truthfully records inner0, keeper-9 and no live group members, but V3 now rejects it. The unrelated sentinel remains unaffected. ABNORMAL_KEEPER_RECEIPT retains the exact full receipt/control bytes. Normal keeper release remains accepted as command execution only; bool-as-int fields, false release, intentional KILL and mismatched control identities reject.

The independent27-case delta probes additionally reject missing required protocol fields, integer substitutes for boolean flags, boolean/float PID fields, absent residual-list type, unresolved cleanup state, malformed/invalidUTF8/empty control structures and a changed control digest. Missing or invalid proof halts eligibility rather than supplying T0 credit. These are local predicate/fault-provider cases, not Linux process facts or arbitrary malicious-root resistance.

## V2 inventory evidence finding is corrected

_members now appends a proof record before actual I/O. Each record carries command, timeout, before/after monotonic timestamps, returncode or exception, explicit stdout/stderr availability, partial-by-timeout marker, lossless base64/length/SHA streams, parse_error and successful matching-member observations. All bounded attempts are returned in inventory_receipts on the owned command receipt, including the original failing attempt and later cleanup scans. Nonzero status is recorded before rejection; invalid encoding is not replacement-decoded. Timeout absence of a stream remains distinguishable from available empty bytes.

The provided three complete-owned failure scenarios independently reproduced PASS for invalidUTF8, timeout-prefix bytes and nonzero ps exit. Complete returned receipts were separately reconstructed and preserved under run/INDEPENDENT_FULL_INVENTORY_FAILURE_RECEIPTS.json; this extra reconstruction adds no new coverage count. The original failure stays visible while cleanup observations remain separate. Every encoded bytepack in these complete receipts, the abnormal keeper receipt and independent probe results was independently decoded and rehashed, with all lengths/SHA matching.

The independent bound probes establish that record128 can retain its full response and zombie distinction, while a129th attempt is refused before any new I/O and the list remains bounded. An exhausted nonpositive inventory budget likewise performs no I/O and retains a failure record. The source parser/cleanup success policy remains unchanged: malformed or failed observations are not interpreted as an empty group. A timeout with unavailable stdout and available empty stderr preserves both availability facts. No actual host encoding/ps-timeout fault is claimed; these responses are explicit providers around real local owned-command cleanup.

## Executed validation and limits

In the exclusive clone,63 full actual warm-prefix cases,11 keeper success/abnormal protocol cases and3 inventory evidence cases independently passed:77 provided V3 cases. The full-prefix cases execute the exact warm/start controller prefix through commit and exact source evaluator with explicit OS/static/clock providers; they are necessary compatibility coverage for changed command_ok. The27 independent delta probes also passed. This is104 LOCAL case executions in the current narrow review, not the parent's whole105-case suite and not all47 future design specifications. The unchanged20 boundary and8 broader owned cases are prior reviewed/reference coverage, not re-executed here. The extra three full-receipt reconstructions repeat evidence custody only.

Real subprocess tests are on macOS POSIX with an explicitly simulated BOOTTIME provider. Current scope remains processes retaining the newly leased session/group; arbitrary escaped descendants are not controlled or claimed. Exact Linux runuser/helper group retention, actual Linux process controls/clocks/imports and realistic scan/receipt latency remain mandatory future gates. A final read-only check of the known keeper/failure fixture groups found no live matching rows; it sent no signals and does not confer future ownership authority.

The105 parent LOCALPASS label was read as input provenance only. No new claim about a24h run, saved frames, CPU/network root cause, production source stability, N1 qualification, capacity boundary or N4 eligibility is made. Earlier V2 BLOCK and original failure evidence remain immutable.

## Required operational gates remain pending

This is code/LOCAL component acceptance only. The retained common constants point at failed P3 solely as OFFLINE references; scope/guard194e is CLOSED and forbidden for reuse or extension. No production/source/P3/runtime/W/repo-plan bytes were edited; no SSH/network, Raw/manifest payload, service action, copy/ARM/launch or newT0 occurred.

Before any operational capture, a new exact reader/control manifest must export and bind the budget, binding, first/full attestation, every end/start attempt/witness, owner-start and commit plus new owned-command proof. Actual Linux exact-source/helper/runuser retention/import/latency qualification and fresh stopped custody/current identity-config/resource admission/unique finite scope remain required. The future47/F47 gate is not discharged by component review. Only then can a new own86400 measurement begin under unchanged995/2880/minimum+60, full new saved-data dualLIVE/verify/independent acceptance and actual restored-before-cancel/handoff/receipt requirements. This result supplies none of that operational or duration credit.
'''
(R/'REPORT.md').write_text(report)
subjects={n:rec(R/'inputs'/n) for n in ['runtime/controller.py','runtime/warm_readiness.py','runtime/warm_completion.py','runtime/owned_command.py']}
save('RESULT.json',{'kind':'INDEPENDENT_EXACT_V3_NARROW_CODE_LOCAL_DELTA_REVIEW','decision':'ACCEPT','scope':'EXACT_CODE_LOCAL_COMPONENT_DELTA_ONLY_NOT_OPERATIONAL_QUALIFICATION','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':str(C),'candidate_code_manifest_sha256':EXPECTED,'bindings':{'subjects':subjects,'source_git_sha':'20e57f30682e88b4090cc2f0da34aa26fca4de2e','prior_V2_BLOCK_manifest_sha256':'d57f165fb38b59cc9ae6e347b1dae7b21e35cb8c3c70732d406f0f641d49c9d4'},'input_readback':{'manifest_members':23,'runtime_references':12,'source_maps':6,'all_SHA_bytes_PASS':True},'changes_verified':{'warm_readiness_only_command_ok_changed':True,'owned_keeper_main_AST_unchanged':True,'owned_control_flow_unchanged_except_inventory_evidence_wiring':True,'controller_and_completion_exact_bytes_unchanged':True,'V2_abnormal_keeper_after_release_rejected':True,'V2_failed_inventory_original_bytes_retained':True},'validation':{'provided_current_V3_executed':{'full_warm_prefix':63,'keeper_protocol':11,'inventory_evidence':3,'all_PASS':True},'independent_delta_probes':{'cases':27,'all_PASS':True},'full_inventory_receipt_reconstruction':{'cases':3,'additional_coverage_credit':0,'all_bytepacks_SHA_size_PASS':True},'not_reexecuted':'Prior20 boundary and8 broader group cases, old actual failures and historical QA','parent105_is_provenance_not_current_review_execution_count':True},'blocking_findings':[],'report':rec(R/'REPORT.md'),'credits':{'component_code_LOCAL_delta_accepted':True,'Linux_or_exact_runuser_qualified':False,'reader_or_fresh_bindings_qualified':False,'copy_ARM_launch_eligible':False,'production_changed_or_deployed':False,'new_T0':False,'own86400_accepted':False,'saved_data_or_capacity_receipt':False,'N4_eligible':False,'root_cause_or_stability_fixed':False},'required_next':'Future exact reader/control manifests; actual Linux runuser/group retention/import/latency; fresh stopped custody/current admission/new finite scope before any capture. F47 remains a required later gate.','preservation':{'writes':'THIS_EXCLUSIVE_REVIEW_DIRECTORY_ONLY','old_candidate_and_reports':'UNCHANGED','old_P3_and_guard194e':'CLOSED_NEVER_REUSE_OR_EXTEND','SSH_network_Raw_service_actions':False}})
files={str(p.relative_to(R)):rec(p) for p in sorted(R.rglob('*')) if p.is_file() and p!=R/'MANIFEST.json'}
save('MANIFEST.json',{'kind':'INDEPENDENT_V3_NARROW_DELTA_REVIEW_ARTIFACTS','decision':'ACCEPT_CODE_LOCAL_DELTA_ONLY','candidate_code_manifest_sha256':EXPECTED,'member_count':len(files),'members':files,'report':rec(R/'REPORT.md'),'result':rec(R/'RESULT.json'),'note':'MANIFEST alone excludes itself. Prior reference manifest, all inputs/results/original console streams and complete receipts are bound; no Raw/remote/service evidence.'})
for n,w in files.items():assert rec(R/n)==w
for n,w in m['members'].items():assert rec(C/n)==w
for p in R.rglob('*'):
 if p.is_file():os.chmod(p,0o444)
for p in sorted((p for p in R.rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True):os.chmod(p,0o555)
os.chmod(R,0o555)
print(json.dumps({'directory':str(R),'decision':'ACCEPT_CODE_LOCAL_DELTA_ONLY','members':len(files),'REPORT':rec(R/'REPORT.md'),'RESULT':rec(R/'RESULT.json'),'MANIFEST':rec(R/'MANIFEST.json'),'all_members_readback_PASS':True,'candidate_all23_unchanged':True},indent=2))
