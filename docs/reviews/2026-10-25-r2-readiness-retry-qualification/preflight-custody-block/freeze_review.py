from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parent
def digest(p):return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
H='4f4c710c37d17e99976473387257ac3dbd89d861820b0102e3fab7a44728fda3';M='294785cf4f36b5bfb49568b9234a7b8425b11e427c9756e54d9f2c5d42c7b71f'
assert digest(R/'inputs/read_current_capacity.py')['sha256']==H
assert digest(R/'inputs/preflight-design-manifest.json')['sha256']==M
for n,v in json.loads((R/'inputs/preflight-design-manifest.json').read_text())['members'].items():assert digest(R/'inputs'/n)==v
x=json.loads((R/'correction-fixture-results.json').read_text());assert x['case_count']==21 and all(c['result']=='PASS' for c in x['cases'])
findings=[{'id':'C1','priority':2,'line':92,'title':'A successful over-deadline I/O observation is discarded','detail':'timed_state and timed_run check remaining() before returning the response to the attempt accumulator. The 181-second synthetic successful state is correctly rejected, but failed_attempts retains only the start time, losing the observed response. Retain an outcome with argv/timeout/timing and its response before the post-I/O deadline assertion, without accepting the late result.'},{'id':'C2','priority':2,'line':133,'title':'Timeout byte custody is lossy','detail':'The exception serializer decodes byte stdout/stderr with errors=replace. Exact synthetic bytes ff657861637400 and fe657272 become replacement characters; the original bytes cannot be reconstructed from the retained JSON or outer SSH output. Add lossless base64 plus byte length/SHA for byte outcomes; keep readable text as a convenience only.'}]
result={'decision':'BLOCK','scope':'CORRECTED_PREFLIGHT_DESIGN_ONLY_NOT_EXECUTED','helper_sha256':H,'design_manifest_sha256':M,'proposed_scope':'/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-readiness-retry-20261025-1s0kswlt','original_three_acceptance_blockers_corrected':True,'remaining_findings':findings,'isolated_local_cases':21,'case_outcomes':'PASS_EXPECTED_COMPONENT_BEHAVIOR','remote_executed':False,'helper_main_or_whole_remote_executed':False,'raw_or_manifest_or_partial_payload_opened':False,'real_subprocess_network_service_called':False,'formal_credit_seconds':0,'linux_qualification_credit':False,'current_handoff_or_admission_accepted':False}
(R/'RESULT.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
(R/'REPORT.md').write_text('''# Corrected preflight 4f4c — BLOCK for response custody

This separate review binds only helper `4f4c710c37d17e99976473387257ac3dbd89d861820b0102e3fab7a44728fda3` and manifest `294785cf4f36b5bfb49568b9234a7b8425b11e427c9756e54d9f2c5d42c7b71f`. The original d103 findings remain immutable in the preceding report. Neither version was remotely executed.

The three original acceptance defects are corrected: traversal fails closed on scan errors and rejects untrusted ancestors/entries; remaining-time I/O and post-I/O checks prevent acceptance past the 180-second bound; and the complete inventory/owners/timer observation is bracketed by the same inactive successful worker invocation, with natural timer/worker races retried. The docstring now accurately describes definition-only canonical verification.

Twenty-one isolated LOCAL component cases check coherent success, retained-partial startup rejection, scan/symlink/FIFO failures, bounded busy/late reads, worker and timer races, Recorder changes, masked owners, disabled timer, failed/invalid worker identity, journal failures and expired inventory time. All expected outcomes are reproduced. These fixtures execute only an extracted handoff component under synthetic clocks, service replies and D-Bus objects; no helper main, whole remote program, real subprocess/network/service or production payload is used. This supplies no Linux credit.

Two evidence-preservation defects remain. A successful state/journal response arriving after the deadline is correctly rejected but discarded before attachment to its attempt, so the observation that triggered the rejection is not retained. Timeout stdout/stderr bytes are decoded using replacement characters, losing their exact original contents. The fixtures reproduce both losses. Preserve the full command outcome before the post-I/O assertion and retain original timeout bytes using base64, length and SHA; readable decoding may accompany those lossless fields. Acceptance checks must remain unchanged.

The result is BLOCK for these narrow custody corrections. It accepts no actual current handoff, capacity admission, Linux qualification, new capture, elapsed time, saved-data denominator or stability repair. The production source/wheel/identity, failed attempt, data and closed guards remain unchanged. Any lossless successor requires its own exact-hash correction review.
''')
items={str(p.relative_to(R)):digest(p) for p in sorted(R.rglob('*')) if p.is_file() and p.name!='MANIFEST.json'}
(R/'MANIFEST.json').write_text(json.dumps({'kind':'INDEPENDENT_PREFLIGHT_CORRECTION_REVIEW','decision':'BLOCK','members':items},sort_keys=True,indent=2)+'\n')
for n,v in items.items():assert digest(R/n)==v
for p in R.rglob('*'):
 if p.is_file():p.chmod(0o444)
for p in sorted([p for p in R.rglob('*') if p.is_dir()],reverse=True):p.chmod(0o555)
R.chmod(0o555)
for n,v in items.items():assert digest(R/n)==v
print(json.dumps({'directory':str(R),'member_count':len(items),'REPORT':digest(R/'REPORT.md'),'RESULT':digest(R/'RESULT.json'),'MANIFEST':digest(R/'MANIFEST.json'),'all_member_readback':'PASS'},sort_keys=True))
