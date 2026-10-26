from pathlib import Path
import ast, hashlib, json, os, re, stat, tempfile, types
from unittest.mock import patch
ROOT=Path(__file__).resolve().parent
helper=ROOT/'inputs-initial-unexecuted/read_current_capacity.py'
b=helper.read_bytes();assert hashlib.sha256(b).hexdigest()=='d103983ac1af2f14064617e708860bcb1b9ea317d8899811c100128946219365'
top=ast.parse(b); remote=next(ast.literal_eval(n.value) for n in top.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='REMOTE' for t in n.targets))
tree=ast.parse(remote)
def target(n,name):return isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets)
def component(start,end):
 a=next(i for i,n in enumerate(tree.body) if target(n,start));z=next(i for i,n in enumerate(tree.body) if target(n,end))
 return compile(ast.Module(body=tree.body[a:z],type_ignores=[]),'<isolated-original-component>','exec')
invcode=component('active','rec_after'); waitcode=component('bounded_end','active'); finalcode=component('rec_after','result')
results=[]
with tempfile.TemporaryDirectory(prefix='review-local-empty-metadata-') as td:
 d=Path(td)/'D';active=d/'data/active';active.mkdir(parents=True);(active/'sentinel.partial').touch()
 g={'Path':Path,'D':d,'os':os,'stat':stat};exec(invcode,g)
 assert len(g['partials'])==1 and g['scanned']==1
 results.append({'case':'baseline_empty_synthetic_partial_is_seen','result':'PASS','partial_count':1})
 with patch('os.scandir',side_effect=PermissionError('fixture scan denied')):
  g={'Path':Path,'D':d,'os':os,'stat':stat};exec(invcode,g)
 assert g['partials']==[] and g['scanned']==0
 results.append({'case':'walk_permission_error_silently_becomes_zero_inventory','result':'REPRODUCED','would_publish_startup_custody_gate':True,'scan_error_retained':False})
 d2=Path(td)/'D2';d2.mkdir();outside=Path(td)/'outside';(outside/'active').mkdir(parents=True);(d2/'data').symlink_to(outside,target_is_directory=True)
 g={'Path':Path,'D':d2,'os':os,'stat':stat};exec(invcode,g)
 assert g['partials']==[] and (d2/'data/active').resolve()!=d2/'data/active'
 results.append({'case':'symlink_ancestor_is_accepted','result':'REPRODUCED','leaf_is_symlink':False,'ancestor_is_symlink':True})
clock=types.SimpleNamespace(value=0.0)
clock.monotonic=lambda:clock.value;clock.sleep=lambda s:None
worker={'ActiveState':'inactive','MainPID':'0','Result':'success','InvocationID':'a'*32}
calls=[]
def state(unit):
 calls.append(unit)
 if len(calls)==1:clock.value=181.0
 return dict(worker)
def run(argv):return {'returncode':0,'stdout':json.dumps({'__REALTIME_TIMESTAMP':'2'}),'stderr':'','argv':argv}
g={'time':clock,'state':state,'run':run,'json':json,'re':re,'rest':{'ended_at_utc_ns':1000}};exec(waitcode,g)
assert clock.value>g['bounded_end'] and len(calls)==2
results.append({'case':'late_successful_worker_read_accepted_after_180_seconds','result':'REPRODUCED','elapsed_seconds':clock.value,'bound_seconds':180})
REC='binance-market-data-recorder.service';AS='binance-market-data-archive.service';AT='binance-market-data-archive.timer'
original={u:{} for u in ['apt-daily.timer','apt-daily-upgrade.timer','apt-daily.service','apt-daily-upgrade.service','unattended-upgrades.service',AT,AS]}
rec={'ActiveState':'inactive','UnitFileState':'disabled','MainPID':'0','NRestarts':'0'}
def current(unit):
 if unit==REC:return dict(rec)
 if unit==AS:return {'ActiveState':'active','UnitFileState':'static','MainPID':'999','InvocationID':'b'*32,'Result':'success'}
 return {'ActiveState':'active' if unit in ['apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service',AT] else 'inactive','UnitFileState':'enabled' if unit!=AS else 'static'}
class Manager:
 def GetUnit(self,u):return u
class Props:
 def Get(self,*a):return 100000001
class Bus:
 def get_object(self,*a):return object()
dbus=types.SimpleNamespace(SystemBus=lambda:Bus(),Interface=lambda obj,iface:Manager() if iface.endswith('.Manager') else Props())
tm=types.SimpleNamespace(CLOCK_MONOTONIC=1,clock_gettime_ns=lambda c:1000000,time_ns=lambda:3000)
g={'state':current,'rec':rec,'backlog':0,'tx':0,'original':original,'dbus':dbus,'time':tm,'rest':{},'cancel':{},'verify':{},'verified':{},'hashlib':hashlib,'PIN':'fixture','worker2':worker,'journal':{},'partials':[],'scanned':0}
exec(finalcode,g)
h=g['handoff'];assert h['archive_worker']['ActiveState']=='inactive' and h['owners'][AS]['ActiveState']=='active'
results.append({'case':'final_handoff_accepts_old_inactive_worker_but_current_active_owner','result':'REPRODUCED','published_archive_worker':h['archive_worker'],'published_current_archive_owner':h['owners'][AS]})
result={'scope':'ISOLATED_LOCAL_ORIGINAL_DESIGN_REPRODUCTIONS','helper_sha256':hashlib.sha256(b).hexdigest(),'cases':results,'case_count':len(results),'helper_main_executed':False,'whole_remote_executed':False,'network_or_subprocess_called':False,'production_files_or_payload_opened':False,'linux_qualification_credit':False,'formal_credit_seconds':0}
(ROOT/'original-fixture-results.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps({'case_count':len(results),'all_expected_behaviors_reproduced':True}))
