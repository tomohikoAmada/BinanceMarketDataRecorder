from pathlib import Path
import json,os,signal,subprocess,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'provided-run/runtime'));import warm_readiness as w
child="import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); os.close(1); os.close(2); time.sleep(30)"
wrapper="import json,subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]]); print(json.dumps({'descendant_pid':p.pid}),flush=True); time.sleep(30)"
sentinel=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],start_new_session=True)
receipt=None;desc=None;group=None
try:
 with patch.object(w,'bt',time.monotonic_ns),patch.object(w,'boot',lambda:'LOCAL_MAC_POSIX_ADVERSARIAL_FIXTURE'):
  begin=w.clocks();budget={'kind':'WARM_FIXED_1200S_BUDGET_V1','begin':begin,'boot_id':'LOCAL_MAC_POSIX_ADVERSARIAL_FIXTURE','deadline_monotonic_ns':begin['monotonic_ns']+1200*10**9,'deadline_boottime_ns':begin['boottime_ns']+1200*10**9}
  receipt=w.owned_command([sys.executable,'-c',wrapper,child],budget,3.0)
 desc=receipt['parsed_stdout']['descendant_pid'];group=receipt['owned_pgid']
 os.kill(desc,0)
 state=subprocess.check_output(['/bin/ps','-o','stat=','-p',str(desc)],text=True).strip()
 alive_running=bool(state) and not state.startswith('Z')
 assert receipt['timed_out'] and receipt['cleanup']=='owned_group_TERM_reaped' and alive_running
 assert sentinel.poll() is None
 out={'case':'noncooperative_descendant_closes_pipes_and_ignores_TERM','result':'BUG_REPRODUCED','receipt':receipt,'descendant_pid':desc,'descendant_ps_state_after_claimed_cleanup':state,'descendant_still_running':alive_running,'unrelated_sentinel_unaffected':True,'macOS_POSIX_only':True,'CLOCK_BOOTTIME':'explicit MONOTONIC fixture','Linux_credit':False,'production_or_VPS_actions':False}
 (R/'OWNED_GROUP_ADVERSARIAL_RESULT.json').write_text(json.dumps(out,indent=2,sort_keys=True)+'\n')
 print(json.dumps({k:v for k,v in out.items() if k!='receipt'}))
finally:
 if group is not None:
  try:os.killpg(group,signal.SIGKILL)
  except ProcessLookupError:pass
 if sentinel.poll() is None:sentinel.terminate()
 sentinel.wait(timeout=5)
