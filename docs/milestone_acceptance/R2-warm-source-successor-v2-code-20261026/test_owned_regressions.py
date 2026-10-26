"""Real LOCAL POSIX leased-group regressions; no Linux or runuser credit."""
from pathlib import Path
import json,os,signal,subprocess,sys,tempfile,time,types
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'runtime'))
import owned_command as own
CASES=[]
def run(name,code,*,timeout=0.5,normal=False,expect_residual=False):
 sentinel=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'],start_new_session=True)
 try:
  begin=time.monotonic();receipt=own.run_owned([sys.executable,'-c',code],python_executable=sys.executable,timeout=timeout,cleanup_seconds=2)
  elapsed=time.monotonic()-begin
  assert elapsed<timeout+2.2,(name,elapsed)
  assert receipt['cleanup_error'] is None,(name,receipt)
  assert receipt['cleanup']=='LEASED_GROUP_NO_LIVE_MEMBERS' and not any(x['live'] for x in receipt['final_group_members'])
  assert sentinel.poll() is None
  if normal:
   assert receipt['returncode']==0 and not receipt['timed_out'] and receipt['lease_released']
  if expect_residual:
   assert receipt['unexpected_live_descendants'] and receipt['group_KILL_used'] and not receipt['lease_released']
  if not normal and not expect_residual:assert receipt['timed_out']
  converted={k:v for k,v in receipt.items() if not isinstance(v,bytes)}
  for k,v in receipt.items():
   if isinstance(v,bytes):converted[k+'_text']=v.decode('utf-8',errors='backslashreplace')
  CASES.append({'case':name,'PASS':True,'elapsed':elapsed,'receipt':converted,'Linux_credit':False})
  return receipt
 finally:
  sentinel.terminate();sentinel.wait(timeout=5)

r=run('normal JSON and no control/lease FD inheritance',"import json,os,signal\nfds=[]\nfor fd in range(3,128):\n try:os.fstat(fd);fds.append(fd)\n except OSError:pass\nprint(json.dumps({'fds':fds,'stdin':os.read(0,1).decode(),'TERM_default':signal.getsignal(signal.SIGTERM)==signal.SIG_DFL}),flush=True)",normal=True)
body=json.loads(r['stdout_raw']);assert body=={'fds':[],'stdin':'','TERM_default':True}
r=run('default TERM is preserved across keeper exec',"import json,signal,time; print(json.dumps({'TERM_default':signal.getsignal(signal.SIGTERM)==signal.SIG_DFL}),flush=True);time.sleep(20)")
assert json.loads(r['stdout_raw'])['TERM_default'] is True
late=json.loads(r['control_raw']);assert late['returncode']==-signal.SIGTERM
run('noncooperative inner requires KILL',"import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(20)")
child="import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);os.close(0);os.close(1);os.close(2);time.sleep(20)"
spawn="import subprocess,sys,json,time;p=subprocess.Popen([sys.executable,'-c',"+repr(child)+"]);print(json.dumps({'owned_descendant':p.pid}),flush=True);time.sleep(.15);"
run('normal wrapper exit with closed-pipe noncooperative descendant',spawn,timeout=1,expect_residual=True)
run('timeout wrapper TERM exit with closed-pipe noncooperative descendant',spawn+'time.sleep(20)',timeout=.5)
run('nonzero wrapper also drains remaining owned group',spawn+'sys.exit(7)',timeout=1,expect_residual=True)
# Malformed inventory is a failed, bounded eligibility result, never assumed empty.
original=own._members
calls=0
def bad_inventory(*a,**kw):
 global calls
 calls+=1
 if calls==1:raise RuntimeError('fixture malformed ps')
 return original(*a,**kw)
with patch.object(own,'_members',bad_inventory):
 receipt=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=1,cleanup_seconds=2)
 assert receipt['cleanup_error'] and 'malformed ps' in receipt['cleanup_error']
 assert not any(x['live'] for x in receipt['final_group_members'])
 CASES.append({'case':'malformed inventory fails closed and cleanup is finite','PASS':True,'cleanup_error':receipt['cleanup_error']})
# Parent death: stdin lease closes; keeper self-kills only its own new group.
with tempfile.TemporaryDirectory(prefix='owned-lease-parent-loss-',dir='/private/var/tmp') as td:
 witness=Path(td)/'pids.json'
 inner="import os,json,time;open("+repr(str(witness))+",'w').write(json.dumps({'keeper':os.getppid(),'inner':os.getpid()}));time.sleep(20)"
 driver="import sys;sys.path.insert(0,"+repr(str(R/'runtime'))+");from owned_command import run_owned;run_owned([sys.executable,'-c',"+repr(inner)+"],python_executable=sys.executable,timeout=5,cleanup_seconds=2)"
 controller=subprocess.Popen([sys.executable,'-c',driver],start_new_session=True)
 sentinel=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'],start_new_session=True)
 try:
  due=time.monotonic()+3
  while not witness.exists() and time.monotonic()<due:time.sleep(.02)
  assert witness.exists();pids=json.loads(witness.read_text())
  controller.kill();controller.wait(timeout=2)
  due=time.monotonic()+3;members=[]
  while time.monotonic()<due:
   members=original(pids['keeper'],.3)
   if not any(x['live'] for x in members):break
   time.sleep(.02)
  assert not any(x['live'] for x in members),members
  assert sentinel.poll() is None
  CASES.append({'case':'controller death closes lease and kills only owned new group','PASS':True,'owned_pids':pids,'final_members':members,'Linux_credit':False})
 finally:
  if controller.poll() is None:controller.kill();controller.wait(timeout=2)
  sentinel.terminate();sentinel.wait(timeout=2)
(R/'LOCAL_OWNED_GROUP_REGRESSIONS.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'real_LOCAL_POSIX':True,'Linux_or_runuser_credit':False,'escape_scope':'only processes retaining the leased session/group; exact Linux runuser group retention remains a pre-execution gate'},indent=2)+'\n')
print(json.dumps({'real_LOCAL_POSIX_cases':len(CASES),'all_PASS':True,'Linux_credit':False}))
