"""Narrow V3 independent success-protocol and inventory record-bound probes."""
from pathlib import Path
import base64,copy,hashlib,json,subprocess,sys,time,types
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'run/runtime'))
import owned_command as own
import warm_readiness as w
cases=[]
def fail(name,call):
 try:call()
 except Exception as e:cases.append({'case':name,'PASS':True,'failure':type(e).__name__+':'+str(e)});return
 raise AssertionError(name+' accepted')
with patch.object(w,'bt',time.monotonic_ns),patch.object(w,'boot',lambda:'INDEPENDENT_V3_DELTA'),patch.object(w,'PYTHON',sys.executable):
 begin=w.clocks();budget={'kind':'WARM_FIXED_1200S_BUDGET_V1','begin':begin,'boot_id':'INDEPENDENT_V3_DELTA','deadline_monotonic_ns':begin['monotonic_ns']+1200*10**9,'deadline_boottime_ns':begin['boottime_ns']+1200*10**9}
 good=w.owned_command([sys.executable,'-c','print("{}",flush=True)'],budget,3)
 w.command_ok(good)
for key in ['keeper_returncode','lease_released','group_KILL_used','keeper_control','child_pid','inner_pid','owned_pgid','unexpected_live_descendants','timed_out','returncode','parse_error','cleanup']:
 v=copy.deepcopy(good);del v[key];fail('missing required success protocol:'+key,lambda:w.command_ok(v))
for key,value in [('lease_released',1),('group_KILL_used',0),('inner_pid',True),('owned_pgid',1.0),('unexpected_live_descendants',None),('cleanup','UNRESOLVED_OWNED_GROUP_CLEANUP')]:
 v=copy.deepcopy(good);v[key]=value;fail('typed or contradictory success protocol:'+key,lambda:w.command_ok(v))
for raw in [b'\xff',b'{}',b'[]',b'{"keeper_pid":true}']:
 v=copy.deepcopy(good);v['keeper_control']=w.encoded(raw);fail('malformed lossless keeper control:'+raw.hex(),lambda:w.command_ok(v))
v=copy.deepcopy(good);v['keeper_control']['sha256']='0'*64;fail('keeper control raw digest mismatch',lambda:w.command_ok(v))
# The record bound prevents a 129th I/O rather than dropping a completed response.
records=[{'existing_record':i} for i in range(128)]
with patch.object(own.subprocess,'run',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('I/O forbidden after bound'))):
 fail('128 existing records refuse I/O before attempt129',lambda:own._members(17,.1,records))
assert len(records)==128
records=[{'existing_record':i} for i in range(127)]
with patch.object(own.subprocess,'run',lambda *a,**kw:types.SimpleNamespace(returncode=0,stdout=b'17 17 Z\n',stderr=b'raw-err')):
 assert own._members(17,.1,records)==[{'pid':17,'pgid':17,'state':'Z','live':False}]
assert len(records)==128 and records[-1]['matching_members'][0]['live'] is False
cases.append({'case':'record128 retains complete original bytes and zombie distinction','PASS':True,'proof':records[-1]})
records=[]
with patch.object(own.subprocess,'run',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('I/O forbidden with exhausted budget'))):
 fail('nonpositive inventory budget before I/O',lambda:own._members(17,0,records))
assert len(records)==1 and records[0]['stdout_available'] is False and records[0]['after_monotonic_ns']>=records[0]['before_monotonic_ns']
cases[-1]['proof']=records[0]
records=[]
with patch.object(own.subprocess,'run',lambda *a,**kw:(_ for _ in ()).throw(subprocess.TimeoutExpired(['ps'],.1,output=None,stderr=b''))):
 fail('timeout absent stdout distinct from available empty stderr',lambda:own._members(17,.1,records))
assert records[0]['stdout_available'] is False and records[0]['stderr_available'] is True and records[0]['partial_by_timeout'] is True
cases[-1]['proof']=records[0]
def packs(v):
 if isinstance(v,dict):
  if {'base64','bytes','sha256'}<=set(v):
   raw=base64.b64decode(v['base64'],validate=True);assert len(raw)==v['bytes'] and hashlib.sha256(raw).hexdigest()==v['sha256']
  for z in v.values():packs(z)
 elif isinstance(v,list):
  for z in v:packs(z)
for f in [R/'run/ABNORMAL_KEEPER_RECEIPT.json',R/'run/LOCAL_INVENTORY_EVIDENCE_RESULTS.json']:
 packs(json.loads(f.read_bytes()))
(R/'INDEPENDENT_DELTA_LIMITS.json').write_text(json.dumps({'case_count':len(cases),'cases':cases,'all_PASS':True,'all_recorded_bytepacks_SHA_size_PASS':True,'real_Linux_or_runuser_credit':False,'scope':'One actual normal LOCAL owned command plus isolated receipt/inventory-provider probes; source/OS predicates are not requalified'},indent=2,sort_keys=True)+'\n')
print(json.dumps({'independent_delta_cases':len(cases),'all_PASS':True,'recorded_bytepacks_valid':True}))
