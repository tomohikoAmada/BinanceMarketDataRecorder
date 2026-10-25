from pathlib import Path
import json,os,signal,subprocess,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'provided-run/runtime'));import warm_readiness as w
child="import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); os.close(1); os.close(2); time.sleep(30)"
wrapper="import json,subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]]); print(json.dumps({'descendant_pid':p.pid}),flush=True)"
sentinel=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],start_new_session=True);group=None
try:
 with patch.object(w,'bt',time.monotonic_ns),patch.object(w,'boot',lambda:'LOCAL_NORMAL_EXIT_FIXTURE'):
  begin=w.clocks();budget={'kind':'WARM_FIXED_1200S_BUDGET_V1','begin':begin,'boot_id':'LOCAL_NORMAL_EXIT_FIXTURE','deadline_monotonic_ns':begin['monotonic_ns']+1200*10**9,'deadline_boottime_ns':begin['boottime_ns']+1200*10**9}
  receipt=w.owned_command([sys.executable,'-c',wrapper,child],budget,3.0)
 group=receipt['owned_pgid'];desc=receipt['parsed_stdout']['descendant_pid'];state=subprocess.check_output(['/bin/ps','-o','stat=','-p',str(desc)],text=True).strip();assert state and not state.startswith('Z')
 w.command_ok(receipt);assert not receipt['timed_out'] and receipt['cleanup']=='normal_exit' and sentinel.poll() is None
 result={'case':'normal_wrapper_exit_with_live_closed_pipe_descendant','result':'BUG_REPRODUCED','receipt':receipt,'descendant_pid':desc,'descendant_state':state,'command_ok_accepts':True,'unrelated_sentinel_unaffected':True,'macOS_POSIX_only':True,'Linux_credit':False}
 (R/'OWNED_GROUP_NORMAL_EXIT_RESULT.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='receipt'}))
finally:
 if group is not None:
  try:os.killpg(group,signal.SIGKILL)
  except ProcessLookupError:pass
 if sentinel.poll() is None:sentinel.terminate()
 sentinel.wait(timeout=5)
