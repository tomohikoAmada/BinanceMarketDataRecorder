"""Independent exact-V2 local POSIX and explicit inventory faults; no Linux credit."""
from pathlib import Path
import base64,hashlib,json,os,signal,subprocess,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'provided-run/runtime'))
import owned_command as own
import warm_readiness as wr
original_run=own.subprocess.run;original_members=own._members
CASES=[]
def packed(receipt):
 return {k:({'base64':base64.b64encode(v).decode(),'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} if isinstance(v,bytes) else v) for k,v in receipt.items()}
def rejected(receipt):
 try:wr.command_ok(dict(receipt,parse_error=None))
 except RuntimeError:return True
 return False
# One actual malformed inventory response. Subsequent real scans allow safe fixture cleanup.
calls=0;bad_stdout=b'\xfforiginal-ps-bytes\x00\n';bad_stderr=b'\xfeoriginal-ps-stderr'
def malformed(argv,*a,**kw):
 global calls
 if argv and argv[0]=='/bin/ps':
  calls+=1
  if calls==1:return subprocess.CompletedProcess(argv,0,bad_stdout,bad_stderr)
 return original_run(argv,*a,**kw)
with patch.object(own.subprocess,'run',malformed):
 begin=time.monotonic();x=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=.5,cleanup_seconds=.5);elapsed=time.monotonic()-begin
assert rejected(x) and x['cleanup_error'] and not any(m['live'] for m in original_members(x['owned_pgid'],.3))
rawretained=any(isinstance(v,bytes) and (bad_stdout in v or bad_stderr in v) for v in x.values())
assert not rawretained
CASES.append({'case':'malformed_inventory_original_bytes_not_retained','outcome':'ELIGIBILITY_FAIL_CLOSED_BUT_EVIDENCE_GAP_REPRODUCED','receipt':packed(x),'elapsed':elapsed,'fixture_ps_stdout':packed({'raw':bad_stdout})['raw'],'fixture_ps_stderr':packed({'raw':bad_stderr})['raw'],'original_inventory_bytes_present_in_receipt':rawretained})
# Simulate a kernel-live/unresolved inventory, not a claim that an actual D task was killed.
def unresolved(group,timeout):return [{'pid':group,'pgid':group,'state':'D','live':True}]
with patch.object(own,'_members',unresolved):
 begin=time.monotonic();x=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=.2,cleanup_seconds=.2);elapsed=time.monotonic()-begin
assert elapsed<.8 and rejected(x) and x['cleanup']=='UNRESOLVED_OWNED_GROUP_CLEANUP'
assert not any(m['live'] for m in original_members(x['owned_pgid'],.3))
CASES.append({'case':'unresolved_live_inventory_is_bounded_and_ineligible','outcome':'PASS','receipt':packed(x),'elapsed':elapsed,'inventory_scope':'Explicit synthetic D state; actual fixture process normal and checked gone'})
begin=time.monotonic();x=own.run_owned([sys.executable,'-c','import time;time.sleep(1)'],python_executable=sys.executable,timeout=.001,cleanup_seconds=.2);elapsed=time.monotonic()-begin
assert elapsed<.8 and rejected(x) and x['timed_out']
assert not any(m['live'] for m in original_members(x['owned_pgid'],.3))
CASES.append({'case':'near_exhausted_command_budget_is_not_success','outcome':'PASS','receipt':packed(x),'elapsed':elapsed})
x=own.run_owned([sys.executable,'-c','import os,signal;os.kill(os.getppid(),signal.SIGKILL);print("{}",flush=True)'],python_executable=sys.executable,timeout=.5,cleanup_seconds=.5)
assert rejected(x) and x['cleanup_error'] and not any(m['live'] for m in original_members(x['owned_pgid'],.3))
CASES.append({'case':'keeper_death_before_inner_control_is_rejected','outcome':'PASS','receipt':packed(x)})
(R/'OWNED_FAULT_BOUNDARIES.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'local_POSIX_plus_explicit_inventory_faults':True,'real_Linux_or_runuser_credit':False},indent=2,sort_keys=True)+'\n')
print(json.dumps({'cases':len(CASES),'bounded_fail_closed_cases':3,'inventory_raw_custody_gap_reproduced':True}))
