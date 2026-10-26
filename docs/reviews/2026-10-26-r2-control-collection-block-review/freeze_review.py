from pathlib import Path
import base64,datetime,hashlib,json,os
R=Path(__file__).resolve().parent;C=Path('/private/var/tmp/bmdr-r2-warm-reader-integration-20261026-mnolxttl');EXPECTED='892f6e5e38ab0f26091a88f46848ff8e1eb202010716692aad7805add16f89bf'
def rec(p):
 b=p.read_bytes();return {'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}
def save(n,v):(R/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
assert rec(C/'COLLECTION_MANIFEST.json')['sha256']==EXPECTED
m=json.loads((C/'COLLECTION_MANIFEST.json').read_bytes())
for n,w in m['members'].items():assert rec(C/n)==w==rec(R/'inputs'/n)
t=json.loads((R/'run/LOCAL_FILE_READER_CASES.json').read_bytes());assert t['case_count']==16 and t['all_PASS']
a=json.loads((R/'ADVERSARIAL_COLLECTION_RESULTS.json').read_bytes());assert a['case_count']==3 and a['all_faults_reproduced']
assert a['cases'][0]['result']['parsed']=={'origin':'FOREIGN'} and a['cases'][0]['result']['read_complete'] and not a['cases'][0]['result']['custody_race']
assert all(not c['result_returned'] for c in a['cases'][1:])
def packs(v):
 if isinstance(v,dict):
  if {'base64','bytes','sha256'}<=set(v):
   b=base64.b64decode(v['base64'],validate=True);assert len(b)==v['bytes'] and hashlib.sha256(b).hexdigest()==v['sha256']
  for z in v.values():packs(z)
 elif isinstance(v,list):
  for z in v:packs(z)
packs(t);packs(a)
findings=[{'id':'COLLECTION-ROOT-ANCESTOR','priority':'P1','file':'control_files.py','lines':[55,66],'title':'Root canonicality can race before unanchored absolute-path open','reproduced':True,'evidence':'ADVERSARIAL_COLLECTION_RESULTS.json case1','mechanism':'root.resolve is checked before root.lstat/open. O_NOFOLLOW protects only the final root entry; higher ancestors are traversed by absolute lstat/open and are not pinned or rechecked. Swapping one ancestor to a symlink after resolve makes before/open/after all describe another root, yielding foreign allowed-name bytes as read_complete true and custody_race false.','minimal_remedy':'Open root by an O_DIRECTORY/O_NOFOLLOW dir-fd walk from a trusted stable anchor, recording/checking every ancestor link, or accept a separately trusted pre-opened root fd with bound identity. Do not rely on resolve followed by an absolute open; reject/mark ancestor changes and retain any observations. Extend below-root link/race discipline to the root path itself.'},{'id':'COLLECTION-PARSE-RESOURCE','priority':'P2','file':'control_files.py','lines':[122,139],'title':'Parser resource error escapes after raw capture and discards result','reproduced':True,'evidence':'ADVERSARIAL_COLLECTION_RESULTS.json cases2-3','mechanism':'JSON/JSONL parse runs outside collection try/finally and catches only ValueError/UnicodeError. A bounded40001-byte depth20000 JSON triggers RecursionError; JSONL with an earlier valid line similarly escapes without returning raw or earlier parsed rows.','minimal_remedy':'Retain parser resource exceptions such as RecursionError as structured parse_errors and return already captured raw/previous row evidence. Keep parsing/serialization resource bounds explicit; do not change global recursion limits or let later parsing discard earlier bytes. Whole-reader outer runtime/serialization safety remains a separate pending gate.'}]
save('FINDINGS.json',{'decision':'BLOCK','findings':findings,'distinct_executed_counterexamples':3,'whole_reader_or_Linux_inference':False})
report='''# Independent collection primitive review

Decision: **BLOCK** for COLLECTION_MANIFEST `892f6e5e38ab0f26091a88f46848ff8e1eb202010716692aad7805add16f89bf` (six exact members), subject control_files.py `9b6107b0a65c0805c1f32e0c743c731388ba5f1bfdfc1103637d67f8894e67ce`. This is only the first definition-only metadata collection asset. It is not a complete reader, proof consumer, runtime/new-scope binding or Linux gate.

All six inputs were independently read in full and SHA/bytes checked. In a new exclusive clone, the16 supplied temporary-metadata/provider cases reproduced PASS. Original stdout/stderr/execution metadata and the resulting complete test JSON are retained. Two code defects are independently reproduced in three further counterexamples, using only disposable LOCAL metadata directories and explicit scheduling hooks. No candidate/W/source/P3/runtime/plan bytes were changed; no SSH/network/Raw/service action occurred.

## COLLECTION-ROOT-ANCESTOR (P1)

At control_files.py:55-66, canonical root is checked using resolve, then root.lstat and os.open use the absolute path. O_NOFOLLOW applies to the final root entry only. The higher root ancestors are not walked from retained directory fds or linked to the initial canonical observation. All below-root directory-fd checks therefore start after an unprotected ancestor traversal.

The independent fixture creates two ordinary temporary roots. The allowed evidence.json in the requested root contains TRUSTED; the same allowed basename under a separate root contains FOREIGN. A Path.resolve scheduling hook first calls the real resolve on the original canonical root, then actually renames its ancestor and replaces that ancestor with a symlink to the separate temporary tree before returning the original resolved path. Subsequent lstat/open/fstat/after observations all refer to the foreign root. The exact collector returns the FOREIGN bytes and parsed value with read_complete=true, custody_race=false and no primary error. Complete result and raw bytes are in ADVERSARIAL_COLLECTION_RESULTS case1.

This is a path-scope/race defect, not a claim that stable read_complete alone promises owner trust or T0. The requested root path passed canonical validation, yet an above-root link changed and the current collection result cannot distinguish the redirection. The test does not attack a production path or claim resistance against arbitrary malicious root.

Minimal correction: anchor the root itself through a checked O_DIRECTORY/O_NOFOLLOW dir-fd walk from a trusted stable anchor, recording/opening/rechecking its ancestor links, or consume a separately trusted already-open root fd with an explicit bound identity. Do not resolve once then open via a subsequently traversed absolute path. Preserve before/open/after observations and raw bytes if a later race is discovered, but never label a changed chain stable. The existing below-root discipline is useful and can be extended rather than replaced with a framework.

## COLLECTION-PARSE-RESOURCE (P2)

The raw bytes are captured in finally, but JSON/JSONL parsing afterwards only catches ValueError and UnicodeError. A RecursionError escapes the function, so the caller receives neither the structured raw result nor parse diagnostics. This breaks the asset's promised retention of already collected evidence after a later error.

A finite40001-byte JSON consisting of20000 nested arrays and a scalar triggers the local json.loads RecursionError under the explicit100000-byte read limit. A JSONL file beginning with a valid row, followed by the same nested JSON, likewise raises and returns no result; even its earlier row/raw evidence is unavailable to the caller. Both original bytepacks and exception facts are retained in ADVERSARIAL_COLLECTION_RESULTS cases2-3. The correct behavior is a parse error while preserving the full captured raw and any preceding row evidence, never silently ignoring the failure or treating it as eligibility.

The first adversarial harness used depth1500 and incorrectly expected RecursionError there; this local decoder accepted it. The harness assertion failed, and that assumption failure is preserved in INITIAL_HARNESS_OBSERVATION plus the original harness. It is not a candidate PASS or an executed defect. The final bounded depth20000 harness independently completed and reproduced both parser escapes. No enormous parsed tree or synthetic byte dump is claimed as a real production control.

Minimal correction: contain parser resource errors such as RecursionError and return structured parse_errors tied to the same raw/line offsets. Keep any earlier JSONL rows. Do not increase global recursion limits to hide the failure. The later whole reader must also bound serialization/aggregate parsing; this component review does not qualify that unimplemented layer or promise survival after arbitrary process OOM/kernel blocking.

## Positive scope and limits

The supplied16 tests establish same-fd original byte capture/parse for normal JSON and JSONL; missing-vs-empty; invalidUTF8/partial evidence; final and below-root symlink rejection; traversal/FIFO rejection; bounded overflow prefix; cooperative deadline preservation; atomic final-entry replacement and growth detection; finite nonboolean argument checks; and retained raw after secondary close errors. The raw bytepacks in supplied and adversarial results were all independently decoded and SHA/length verified. Those positives remain, but do not cover the above-root race or all parser exceptions.

read_complete is correctly documented as a stable full byte read rather than valid JSON, trusted ownership or T0. Invalid/empty JSON can be read_complete while parse_errors are present, and an overflow prefix can parse without being complete. The deadline is a cooperative before-read mechanism; it does not preempt a blocked kernel syscall, bound later parsing or constitute a full reader/SSH timeout. A future outer reader must preserve those distinctions and add its own complete evidence/eligibility/resource contract. No stricter hard-timeout claim is inferred here.

The exact subject remains blocked. Correct these two affected paths in a distinct frozen successor, retain the counterexamples, and independently review the narrow delta. The17-runtime/15-safe-import reader integration, invocation selector, owner-start/commit/postcommit validator, Linux/runuser/group qualification, current stopped preflight/physical scope/forecast/admission/window and operational single-launch gates are still pending. The24 design fixture specifications were not executed by these16 collection tests. All four accepted V3 runtime subjects and old closed scopes/guards retain their prior scope; this result grants no copy/ARM/launch, newT0,24h/data/capacity/stability credit.
'''
(R/'REPORT.md').write_text(report)
save('RESULT.json',{'kind':'INDEPENDENT_EXACT_LOSSLESS_METADATA_COLLECTION_COMPONENT_REVIEW','decision':'BLOCK','scope':'DEFINITION_ONLY_FILE_COLLECTION_PRIMITIVE_NOT_WHOLE_READER','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':str(C),'collection_manifest_sha256':EXPECTED,'subject_sha256':'9b6107b0a65c0805c1f32e0c743c731388ba5f1bfdfc1103637d67f8894e67ce','input_members':6,'all_input_SHA_bytes_PASS':True,'validation':{'provided_LOCAL_cases':16,'provided_PASS':16,'independent_reproduced_counterexamples':3,'distinct_findings':2,'initial_harness_depth_assumption_failure_retained':True,'all_recorded_bytepacks_SHA_size_PASS':True},'blocking_findings':findings,'report':rec(R/'REPORT.md'),'credits':{'collection_accepted':False,'whole_reader_implemented_or_accepted':False,'Linux_or_newscope_qualified':False,'copy_ARM_launch_eligible':False,'new_T0_or_24h':False,'data_capacity_or_stability_credit':False},'preservation':{'candidate_W_source_P3_runtime_plans':'UNCHANGED','writes':'THIS_EXCLUSIVE_REVIEW_DIRECTORY_ONLY','SSH_network_Raw_service_actions':False},'required_next':'Distinct exact correction of root ancestor anchoring and parser error custody, then narrow independent review. Whole integration remains separately pending.'})
files={str(p.relative_to(R)):rec(p) for p in sorted(R.rglob('*')) if p.is_file() and p!=R/'MANIFEST.json'}
save('MANIFEST.json',{'kind':'INDEPENDENT_COLLECTION_COMPONENT_REVIEW_ARTIFACTS','decision':'BLOCK','collection_manifest_sha256':EXPECTED,'member_count':len(files),'members':files,'report':rec(R/'REPORT.md'),'result':rec(R/'RESULT.json')})
for n,w in files.items():assert rec(R/n)==w
for n,w in m['members'].items():assert rec(C/n)==w
for p in R.rglob('*'):
 if p.is_file():os.chmod(p,0o444)
for p in sorted((p for p in R.rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True):os.chmod(p,0o555)
os.chmod(R,0o555)
print(json.dumps({'directory':str(R),'decision':'BLOCK','members':len(files),'REPORT':rec(R/'REPORT.md'),'RESULT':rec(R/'RESULT.json'),'MANIFEST':rec(R/'MANIFEST.json'),'all_members_readback_PASS':True,'candidate_all6_unchanged':True},indent=2))
