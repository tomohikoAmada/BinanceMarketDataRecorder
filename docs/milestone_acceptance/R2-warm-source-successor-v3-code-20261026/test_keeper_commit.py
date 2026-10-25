"""LOCAL real abnormal keeper exit and success-protocol rejection regressions."""
from pathlib import Path
import copy,json,os,signal,subprocess,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'runtime'))
import owned_command as own
import warm_readiness as w
CASES=[]
def budget():
 b=w.clocks();return {'kind':'WARM_FIXED_1200S_BUDGET_V1','begin':b,'boot_id':'LOCAL_KEEPER_FIXTURE','deadline_monotonic_ns':b['monotonic_ns']+1200*10**9,'deadline_boottime_ns':b['boottime_ns']+1200*10**9}
def fail(name,receipt):
 try:w.command_ok(receipt)
 except RuntimeError as exc:CASES.append({'case':name,'PASS':True,'error':str(exc)});return
 raise AssertionError(name+' wrongly accepted')
with patch.object(w,'bt',time.monotonic_ns),patch.object(w,'boot',lambda:'LOCAL_KEEPER_FIXTURE'),patch.object(w,'PYTHON',sys.executable):
 try:w.owned_command([sys.executable,'-c','print(1)'],budget(),2)
 except RuntimeError:CASES.append({'case':'no command without its cleanup reserve','PASS':True})
 else:raise AssertionError('cleanup reserve was waived')
 good=w.owned_command([sys.executable,'-c','print("{}",flush=True)'],budget(),3)
 w.command_ok(good);CASES.append({'case':'normal keeper release and exact protocol qualifies command only','PASS':True})
 original=own.subprocess.Popen
 class FlushKill:
  def __init__(self,stream,proc):self.stream=stream;self.proc=proc
  def __getattr__(self,n):return getattr(self.stream,n)
  def flush(self):
   self.stream.flush()
   self.proc.kill()  # Still unreaped owned keeper only; no PGID/unrelated signalling.
 def factory(args,*a,**kw):
  p=original(args,*a,**kw)
  if isinstance(args,list) and '--keeper' in args:p.stdin=FlushKill(p.stdin,p)
  return p
 sentinel=original([sys.executable,'-c','import time;time.sleep(10)'],start_new_session=True)
 try:
  with patch.object(own.subprocess,'Popen',factory):bad=w.owned_command([sys.executable,'-c','print("{}",flush=True)'],budget(),3)
  assert bad['returncode']==0 and bad['keeper_returncode']==-signal.SIGKILL
  assert bad['lease_released'] and bad['cleanup_error'] is None
  assert not bad['unexpected_live_descendants'] and not any(x['live'] for x in bad['final_group_members'])
  assert sentinel.poll() is None
  fail('actual inner0 plus release then abnormal keeper death never qualifies',bad)
  (R/'ABNORMAL_KEEPER_RECEIPT.json').write_text(json.dumps(bad,indent=2)+'\n')
 finally:
  sentinel.terminate();sentinel.wait(timeout=2)
 for key,value in [('keeper_returncode',True),('lease_released',False),('group_KILL_used',True),('returncode',False),('child_pid',True),('timed_out',0)]:
  changed=copy.deepcopy(good);changed[key]=value;fail('success_protocol:'+key,changed)
 control=json.loads(w.decoded(good['keeper_control']))
 changed=copy.deepcopy(good);control['returncode']=True;changed['keeper_control']=w.encoded(json.dumps(control).encode());fail('control returncode nonboolean integer required',changed)
 changed=copy.deepcopy(good);control['returncode']=0;control['keeper_pid']+=1;changed['keeper_control']=w.encoded(json.dumps(control).encode());fail('control and leased keeper identity must match',changed)
(R/'LOCAL_KEEPER_SUCCESS_REGRESSIONS.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'real_LOCAL_POSIX_abnormal_keeper_counterexample_now_rejected':True,'Linux_or_T0_credit':False},indent=2)+'\n')
print(json.dumps({'LOCAL_keeper_success_cases':len(CASES),'all_PASS':True,'Linux_credit':False}))
