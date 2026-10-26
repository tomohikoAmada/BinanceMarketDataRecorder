from pathlib import Path
import ast,base64,datetime,hashlib,json,os
R=Path(__file__).resolve().parent;C=Path('/private/var/tmp/bmdr-r2-control-files-v2-20261026-k9ma9t7a');EXPECTED='b42e377f2d501f26fb9496eb4a963ad24e09d07afaa3a9ea1bda96a64b99d7c4'
def rec(p):
 b=p.read_bytes();return {'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}
def save(n,v):(R/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
assert rec(C/'COLLECTION_MANIFEST.json')['sha256']==EXPECTED
m=json.loads((C/'COLLECTION_MANIFEST.json').read_bytes())
for n,w in m['members'].items():assert rec(C/n)==w==rec(R/'inputs'/n)
for n,num in [('LOCAL_FILE_READER_CASES.json',16),('LOCAL_COLLECTION_REGRESSIONS.json',6)]:
 x=json.loads((R/'run'/n).read_bytes());assert x['case_count']==num and x['all_PASS']
a=json.loads((R/'INDEPENDENT_DELTA_CASES.json').read_bytes());assert a['case_count']==17 and a['all_PASS']
checks=[0]
def packs(v):
 if isinstance(v,dict):
  if {'base64','bytes','sha256'}<=set(v):
   raw=base64.b64decode(v['base64'],validate=True);assert len(raw)==v['bytes'] and hashlib.sha256(raw).hexdigest()==v['sha256'];checks[0]+=1
  for z in v.values():packs(z)
 elif isinstance(v,list):
  for z in v:packs(z)
for n in ['LOCAL_FILE_READER_CASES.json','LOCAL_COLLECTION_REGRESSIONS.json']:packs(json.loads((R/'run'/n).read_bytes()))
for case in a['cases']:
 if 'result_path' in case:
  p=R/case['result_path'];raw=p.read_bytes();assert len(raw)==case['result_bytes'] and hashlib.sha256(raw).hexdigest()==case['result_sha256'];value=json.loads(raw);packs(value);json.dumps(value,allow_nan=False)
old=ast.parse((R/'prior-BLOCK/inputs/control_files.py').read_text());new=ast.parse((R/'inputs/control_files.py').read_text());func=lambda t:{n.name:n for n in t.body if isinstance(n,ast.FunctionDef)};fo,fn=func(old),func(new);dump=lambda n:ast.dump(n,include_attributes=False)
unchanged={n:dump(fo[n])==dump(fn[n]) for n in ['_stat','_identity','_error']};assert all(unchanged.values())
to=next(n for n in fo['collect_file'].body if isinstance(n,ast.Try));tn=next(n for n in fn['collect_file'].body if isinstance(n,ast.Try));assert [dump(n) for n in to.finalbody]==[dump(n) for n in tn.finalbody]
wo=next(n for n in to.body if isinstance(n,ast.While));wn=next(n for n in tn.body if isinstance(n,ast.While));assert dump(wo)==dump(wn)
save('FINAL_RECONSTRUCTION.json',{'all10candidate_members_unchanged':True,'bytepack_count':checks[0],'all_bytepack_SHA_size_PASS':True,'all12_additional_returned_results_strict_JSON_serializable':True,'unchanged_AST':{'stat_identity_error_helpers':unchanged,'original_cooperative_read_loop':True,'original_raw_capture_and_fd_close_finally':True},'scope':'Current collection asset delta only; no whole-reader, source QA, Linux or operational action'})
report='''# Independent collection V2 narrow delta review

Decision: **ACCEPT, collection-primitive code/LOCAL delta only** for COLLECTION_MANIFEST `b42e377f2d501f26fb9496eb4a963ad24e09d07afaa3a9ea1bda96a64b99d7c4` and exact control_files.py `d13a30e0d007666c26d152f66d4ff5d70d6854014f3893a2c6cb87ae8b4ee263`. No new blocking finding was established in the repaired ancestor custody or parse-retention paths. This is not an accepted whole reader or operational integration.

The existing exclusive partial review directory was resumed. Its full10-member input snapshot and already completed16+6 current V2 execution artifacts were retained; those completed tests were not duplicated after the resumed parent task. No prior V3/source/design/historical QA or actual capture failure was rerun. All10 current candidate members remained unchanged.

## Executed scope

The16 original temporary-file/provider cases and six current repair regressions independently reproduced PASS, with original stdout/stderr/execution metadata retained. Seventeen additional bounded probes independently passed: three after-open ancestor/root/relative-directory redirections; ordinary ancestor replacement between stat/open; final entry replacement between stat/open; six exact parsed-depth boundaries; deep JSONL retaining valid rows before and after the bad line; and five invalid depth parameters. Every additional returned result was strictly JSON serialized with allow_nan=False, saved independently and hashed. All recorded raw bytepacks from supplied and independent results were decoded and SHA/length checked.

The tests use disposable LOCAL metadata files and explicit I/O scheduling hooks. They grant neither actual Linux filesystem/process qualification nor whole-reader, invocation, commit, duration or production facts.

## Ancestor repair

V2 opens / as the stable directory anchor and traverses every absolute root component through retained directory fds using O_DIRECTORY|O_NOFOLLOW. Each link has before/open identity evidence and participates in post-read identity checks. The prior resolve call is only a canonical precheck and is no longer the authority for an absolute-path open. Below-root relative directories use the same mechanism. Stat/identity/error helper functions and the original raw-capture/close finally path remain AST-identical; the cooperative read loop is unchanged.

The exact old after-resolve ancestor symlink counterexample now fails before reading file data. New probes redirect an absolute ancestor, root entry or relative directory after the trusted file fd is pinned. They retain only the original TRUSTED bytes, identify custody change and withhold read_complete. They do not read the FOREIGN replacement. A non-symlink ordinary ancestor substituted between its stat and open fails identity validation before data; a final ordinary file substituted at that point likewise fails before contributing FOREIGN bytes.

These observations establish the exercised path/FD race behavior. They do not turn path custody into owner trust or security against arbitrary root. The caller still supplies a reviewed root/allowlist and must separately validate expected ownership, known scope and full evidence contract. An exception may be represented by primary_error rather than custody_race; read_complete remains false on such failed paths. No unsafe completeness inference is required from a single flag.

## Parser repair and bounded view

JSON/JSONL parsing now catches ordinary parser exceptions including RecursionError after raw capture. The original depth20000 JSON and JSONL resource faults return structured parse_errors and retain exact originals. Valid earlier JSONL records survive, and the independent case also verifies a later valid row is retained with line3 provenance after the line2 error. The error's byte_offset is tied to the same captured bytes.

The non-authoritative parsed view has default depth64 and a strict integer1..256 option. Iterative inspection rejects views exceeding the declared depth and non-finite float values while preserving raw. Independent boundary probes verify depth1/64/256 acceptance at the exact configured bound, rejection just beyond each, and rejection of True/0/257/float/None depth parameters before collection. Supplied NaN and1e999 cases remain structured errors. All returned test results tested for serialization can be embedded in a strict JSON export without deep/non-finite parsed values escaping.

This is a diagnostic-view bound, not mutation/deletion of source bytes or a change to producer policy/schema/availability thresholds. A complete stable raw read can still have parse_errors. A bounded overflow prefix may parse without being complete. Neither parsed success nor read_complete establishes owner trust, valid warm proof or T0. The later full reader must enforce those distinctions.

## Timing and remaining qualifications

The unchanged timeout semantics are cooperative checks before regular-file reads. They do not preempt a blocked kernel syscall, provide a hard global walk/parse/SSH deadline, or promise survival under arbitrary OOM. Caller byte/time bounds and a separately reviewed outer reader/command envelope remain required. No stronger deadline guarantee was inferred or executed here.

Prior BLOCK8a2091f7/all25 remains immutable. Its two reproduced defects are corrected in the tested V2 paths, but this acceptance covers only the definition-only collection asset. The whole generated lossless command collector, invocation selector, owner-start/commit/source-pair/postcommit-entry validator,17-runtime/15-safe-import builder, actual Linux/runuser/group-retention/import/latency proof, fresh stopped preflight/physical scope/admission/window and copy/ARM/launch gates are still unimplemented or unqualified. The24 integration-design specifications are not these22+17 collection tests. No V3 runtime subjects, source/W/P3/plans/candidate files were modified; no SSH/network/Raw/service action occurred.

The parent may reuse this primitive acceptance in a later distinct exact integration, retaining this subject hash and limits. It grants no automatic whole-reader acceptance, remote operation, new scope/window, T0,own86400,saved-data correctness,capacity receipt or stability/root-cause credit.
'''
(R/'REPORT.md').write_text(report)
save('RESULT.json',{'kind':'INDEPENDENT_COLLECTION_V2_EXACT_NARROW_DELTA_REVIEW','decision':'ACCEPT','scope':'DEFINITION_ONLY_COLLECTION_PRIMITIVE_CODE_LOCAL_DELTA','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':str(C),'collection_manifest_sha256':EXPECTED,'subject_sha256':'d13a30e0d007666c26d152f66d4ff5d70d6854014f3893a2c6cb87ae8b4ee263','input_members':10,'all_input_SHA_bytes_PASS':True,'validation':{'current_V2_provided_cases':{'base':16,'repair_regressions':6,'all_PASS':True},'independent_delta_cases':{'cases':17,'all_PASS':True},'all_recorded_bytepacks_SHA_size_PASS':True,'all_additional_returned_results_strict_JSON_serializable':True,'partial_review_directory_continued_not_restarted':True},'blocking_findings':[],'old_block_manifest_sha256':'8a2091f718908b7ed7f34330117c15d1bd857821583cfd312b1ecd98bf18deea','report':rec(R/'REPORT.md'),'limits':{'read_complete':'Stable full original byte capture only, not parse/owner/T0 validity','parsed_view':'Non-authoritative resource-limited view; source bytes retained','deadline':'Cooperative before-read checks, not kernel-I/O preemption or whole-reader timeout'},'credits':{'collection_component_delta_accepted':True,'whole_reader_invselector_or_commit_consumer_accepted':False,'Linux_or_newscope_qualified':False,'copy_ARM_launch_eligible':False,'new_T0_or_24h':False,'data_capacity_or_stability_credit':False},'preservation':{'candidate_W_source_P3_runtime_plans':'UNCHANGED','SSH_network_Raw_service_actions':False,'writes':'THIS_EXCLUSIVE_REVIEW_DIRECTORY_ONLY'}})
files={str(p.relative_to(R)):rec(p) for p in sorted(R.rglob('*')) if p.is_file() and p!=R/'MANIFEST.json'}
save('MANIFEST.json',{'kind':'INDEPENDENT_COLLECTION_V2_DELTA_REVIEW_ARTIFACTS','decision':'ACCEPT_COLLECTION_PRIMITIVE_ONLY','collection_manifest_sha256':EXPECTED,'member_count':len(files),'members':files,'report':rec(R/'REPORT.md'),'result':rec(R/'RESULT.json')})
for n,w in files.items():assert rec(R/n)==w
for n,w in m['members'].items():assert rec(C/n)==w
for p in R.rglob('*'):
 if p.is_file():os.chmod(p,0o444)
for p in sorted((p for p in R.rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True):os.chmod(p,0o555)
os.chmod(R,0o555)
print(json.dumps({'directory':str(R),'decision':'ACCEPT_COLLECTION_PRIMITIVE_ONLY','members':len(files),'REPORT':rec(R/'REPORT.md'),'RESULT':rec(R/'RESULT.json'),'MANIFEST':rec(R/'MANIFEST.json'),'all_members_readback_PASS':True,'candidate_all10_unchanged':True},indent=2))
