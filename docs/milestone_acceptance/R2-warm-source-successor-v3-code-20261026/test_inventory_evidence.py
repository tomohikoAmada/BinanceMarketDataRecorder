"""Complete owned-command failure receipts retain exact malformed/timeout ps bytes."""
from pathlib import Path
import base64,json,subprocess,sys,types
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'runtime'))
import owned_command as own
CASES=[]
original=own.subprocess.run
for mode in ['invalid_UTF8','timeout_prefix','nonzero']:
 count=[0]
 raw=b'\xffpartial original ps\n';err=b'\x80original stderr'
 def fake(*a,**kw):
  count[0]+=1
  if count[0]>1:return original(*a,**kw)
  if mode=='timeout_prefix':raise subprocess.TimeoutExpired(a[0],kw['timeout'],output=raw,stderr=err)
  return types.SimpleNamespace(returncode=0 if mode=='invalid_UTF8' else 7,stdout=raw,stderr=err)
 with patch.object(own.subprocess,'run',fake):
  receipt=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=1,cleanup_seconds=2)
 assert receipt['cleanup_error'] and receipt['inventory_receipts']
 first=receipt['inventory_receipts'][0]
 assert base64.b64decode(first['stdout']['base64'])==raw and base64.b64decode(first['stderr']['base64'])==err
 assert first['exception'] and first['after_monotonic_ns']>=first['before_monotonic_ns']
 assert not any(x['live'] for x in receipt['final_group_members'])
 CASES.append({'case':mode,'PASS':True,'failure':receipt['cleanup_error'],'exact_original_ps_evidence':first,'Linux_credit':False})
(R/'LOCAL_INVENTORY_EVIDENCE_RESULTS.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'real_lease_processes_but_PS_responses_explicit_fixtures':True,'Linux_or_T0_credit':False},indent=2)+'\n')
print(json.dumps({'LOCAL_inventory_failure_cases':len(CASES),'all_PASS':True,'Linux_credit':False}))
