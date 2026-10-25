"""Explicit late-I/O fault around exact owned helper; real owned subprocess, no Linux credit."""
from pathlib import Path
import base64,hashlib,json,sys,time
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'provided-run/runtime'))
import owned_command as own
import warm_readiness as wr
original=own._members
calls=[]
def delayed(*a,**kw):
 value=original(*a,**kw)
 calls.append({'args':a,'kwargs':kw,'observed':value,'at':time.monotonic()})
 if len(calls)==2:time.sleep(.4)
 return value
begin=time.monotonic()
with patch.object(own,'_members',delayed):
 receipt=own.run_owned([sys.executable,'-c','print("{}",flush=True)'],python_executable=sys.executable,timeout=.12,cleanup_seconds=.12)
elapsed=time.monotonic()-begin
try:wr.command_ok(dict(receipt,parse_error=None));accepted=True
except RuntimeError:accepted=False
result={'case':'late_successful_inventory_response_after_original_absolute_cap','accepted':accepted,'elapsed':elapsed,'total_requested_cap_seconds':.24,'late_I_O_fixture':'Second actual ps result delayed by .4s after capture, modelling returned I/O after scheduler pause; no claim of actual host CPU stall','inventory_calls':calls,'receipt':{k:({'base64':base64.b64encode(v).decode(),'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} if isinstance(v,bytes) else v) for k,v in receipt.items()},'real_Linux_or_runuser_credit':False}
(R/'LATE_INVENTORY_RESULT.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in {'receipt','inventory_calls'}}))
assert not any(x['live'] for x in original(receipt['owned_pgid'],.3))
