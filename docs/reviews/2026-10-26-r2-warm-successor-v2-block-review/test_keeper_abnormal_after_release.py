"""Exact V2 local owned-command protocol fault: SIGKILL keeper after real release flush.
Only fixture keeper PID is signalled; inner completed first. No service/Linux credit.
"""
from pathlib import Path
import base64,hashlib,json,os,signal,subprocess,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent
sys.path.insert(0,str(R/'provided-run/runtime'))
import owned_command as own
import warm_readiness as wr
original=own.subprocess.Popen
created=[]
class FlushKill:
 def __init__(self,stream,pid): self.stream=stream;self.pid=pid;self.done=False
 def write(self,data):return self.stream.write(data)
 def flush(self):
  result=self.stream.flush()
  if not self.done:
   self.done=True
   os.kill(self.pid,signal.SIGKILL)
  return result
 def close(self):return self.stream.close()
 def __getattr__(self,n):return getattr(self.stream,n)
def launch(argv,*a,**kw):
 p=original(argv,*a,**kw)
 if '--keeper' in argv:
  p.stdin=FlushKill(p.stdin,p.pid);created.append(p.pid)
 return p
sentinel=original([sys.executable,'-c','import time;time.sleep(10)'],start_new_session=True)
try:
 with patch.object(own.subprocess,'Popen',launch):
  receipt=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=1,cleanup_seconds=2)
 assert receipt['keeper_returncode']==-signal.SIGKILL,receipt
 assert receipt['returncode']==0 and receipt['lease_released'] and not receipt['timed_out']
 assert receipt['cleanup_error'] is None and receipt['cleanup']=='LEASED_GROUP_NO_LIVE_MEMBERS'
 assert sentinel.poll() is None
 candidate_receipt=dict(receipt,parse_error=None)
 wr.command_ok(candidate_receipt)
 packed={}
 for k,v in receipt.items():
  packed[k]={'base64':base64.b64encode(v).decode(),'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} if isinstance(v,bytes) else v
 result={'case':'unexpected_keeper_SIGKILL_after_real_R_flush','result':'BUG_REPRODUCED','keeper_pids':created,'receipt':packed,'command_ok_accepts':True,'unrelated_sentinel_unaffected':True,'scope':'Actual local POSIX signal of exact owned keeper only, after inner has completed; no Linux/runuser/service actions'}
 (R/'ADVERSARIAL_KEEPER_EXIT.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
 print(json.dumps({k:v for k,v in result.items() if k!='receipt'}))
finally:
 sentinel.terminate();sentinel.wait(timeout=3)
