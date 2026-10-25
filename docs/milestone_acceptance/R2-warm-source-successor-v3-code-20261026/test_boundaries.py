"""LOCAL guards plus real macOS POSIX owned-tree timeout, no Linux credit."""
from pathlib import Path
import ast, base64, contextlib, copy, hashlib, json, os, signal, subprocess, sys, tempfile, time, types
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'runtime'))
import warm_readiness as w
CASES=[]
def expect_failure(name,call):
 try:call()
 except Exception as e:CASES.append({'case':name,'PASS':True,'error':type(e).__name__+':'+str(e)});return
 raise AssertionError('Accepted invalid guard: '+name)
B={'kind':'WARM_FIXED_1200S_BUDGET_V1','begin':{'utc_ns':10**18,'boottime_ns':10**12,'monotonic_ns':0},'boot_id':'fixture','deadline_monotonic_ns':1200*10**9,'deadline_boottime_ns':10**12+1200*10**9}
with patch.object(w,'boot',lambda:'fixture'):
 for name,now in [('deadline',{'utc_ns':10**18+1200*10**9,'boottime_ns':10**12+1200*10**9,'monotonic_ns':1200*10**9}),('UTCregress',{'utc_ns':10**18-1,'boottime_ns':10**12,'monotonic_ns':0}),('BTregress',{'utc_ns':10**18,'boottime_ns':10**12-1,'monotonic_ns':0}),('MONOregress',{'utc_ns':10**18,'boottime_ns':10**12,'monotonic_ns':-1}),('UTCjump',{'utc_ns':10**18+3*10**9,'boottime_ns':10**12,'monotonic_ns':0}),('BTMONOjump',{'utc_ns':10**18,'boottime_ns':10**12,'monotonic_ns':3*10**9})]:
  with patch.object(w,'clocks',lambda:now):expect_failure(name,lambda:w.clock_guard(B))
 with patch.object(w,'clocks',lambda:dict(B['begin'])):
  for k in ['deadline_monotonic_ns','deadline_boottime_ns']:
   q=copy.deepcopy(B);q[k]+=10**9;expect_failure('fixed_budget_extension:'+k,lambda:w.clock_guard(q))
  q=copy.deepcopy(B);q['kind']='forged';expect_failure('budget_kind',lambda:w.clock_guard(q))
with patch.object(w,'boot',lambda:'other'),patch.object(w,'clocks',lambda:dict(B['begin'])):expect_failure('boot_changed',lambda:w.clock_guard(B))
for name,record in [('missingbytes',{'base64':base64.b64encode(b'original').decode(),'bytes':1,'sha256':hashlib.sha256(b'original').hexdigest()}),('wronghash',{'base64':base64.b64encode(b'original').decode(),'bytes':8,'sha256':'other'}),('invalidbase64',{'base64':'$notbase64','bytes':1,'sha256':'other'})]:expect_failure(name,lambda:w.decoded(record))
with tempfile.TemporaryDirectory(prefix='warm-local-stat-',dir='/private/var/tmp') as td:
 q=Path(td);f=q/'record.json';f.write_text('{"original":true}');sy=q/'symlink.json';sy.symlink_to(f)
 expect_failure('symlink_file',lambda:w.file_identity(sy))
 expect_failure('directory_as_file',lambda:w.file_identity(q))
 before=w.file_identity(f);f.rename(q/'old');f.write_text('new');after=w.file_identity(f)
 assert before!=after;CASES.append({'case':'replacement_inode_detected','PASS':True})
 with f.open('a') as out:out.write(' ordinary growth')
 assert after==w.file_identity(f);CASES.append({'case':'same_inode_growth_not_fake_integrity','PASS':True})
# Exact function prefix/tail scope: all source functions other than capture unchanged.
old=ast.parse((R/'ORIGINAL_CONTROLLER.py').read_text());new=ast.parse((R/'runtime/controller.py').read_text())
func=lambda tree:{x.name:ast.dump(x,include_attributes=False) for x in tree.body if isinstance(x,(ast.FunctionDef,ast.ClassDef))}
a=func(old);b=func(new);assert [n for n in a if a[n]!=b[n]]==['capture']
def formal(tree):
 f=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='capture')
 i=next(i for i,x in enumerate(f.body) if isinstance(x,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='next_bt' for t in x.targets))
 return ast.dump(ast.Module(body=f.body[i:],type_ignores=[]),include_attributes=False)
assert formal(old)==formal(new);CASES.append({'case':'all_postT0_capture_and_other_controller_functions_AST_unchanged','PASS':True})
# Real LOCAL POSIX child + nested descendant, actual timeout/pipe collection/group signal.
# Only BOOTTIME/provider is simulated with MONOTONIC because this host is macOS.
child_code="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)"
wrapper_code='''import json,os,signal,subprocess,sys,time
child=subprocess.Popen([sys.executable,'-c',sys.argv[1]])
def stop(sig,frame):
 child.kill(); child.wait(); print('owned wrapper reaped own descendant',file=sys.stderr,flush=True); sys.exit(0)
signal.signal(signal.SIGTERM,stop)
print(json.dumps({'descendant_pid':child.pid}),flush=True)
time.sleep(30)
'''
sentinel=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],start_new_session=True)
try:
 with patch.object(w,'bt',time.monotonic_ns),patch.object(w,'boot',lambda:'LOCAL_POSIX_FIXTURE'),patch.object(w,'PYTHON',sys.executable):
  begin=w.clocks();budget={'kind':B['kind'],'begin':begin,'boot_id':'LOCAL_POSIX_FIXTURE','deadline_monotonic_ns':begin['monotonic_ns']+1200*10**9,'deadline_boottime_ns':begin['boottime_ns']+1200*10**9}
  receipt=w.owned_command([sys.executable,'-c',wrapper_code,child_code],budget,3.0)
 (R/'LAST_BOUNDARY_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n')
 assert receipt['timed_out'] and receipt['cleanup']=='LEASED_GROUP_NO_LIVE_MEMBERS',receipt
 desc=receipt['parsed_stdout']['descendant_pid']
 try:os.kill(desc,0)
 except ProcessLookupError:pass
 else:raise AssertionError('owned descendant still exists after reap')
 assert sentinel.poll() is None
 assert w.decoded(receipt['stdout'])==w.decoded(receipt['timeout_prefix_stdout'])
 assert b'owned wrapper reaped own descendant' in w.decoded(receipt['stderr'])
 expect_failure('timeout_zero_exit_is_never_success',lambda:w.command_ok(receipt))
 CASES.append({'case':'real_LOCAL_POSIX_nested_timeout_owned_group_and_unrelated_sentinel','PASS':True,'receipt':receipt,'real_Linux_credit':False,'CLOCK_BOOTTIME':'explicit MONOTONIC fixture'})
finally:
 sentinel.terminate();sentinel.wait(timeout=5)
(R/'LOCAL_BOUNDARY_RESULTS.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'real_Linux_or_capture_credit':False},indent=2)+'\n')
print(json.dumps({'LOCAL_boundary_cases':len(CASES),'all_PASS':True,'real_Linux_credit':False}))
