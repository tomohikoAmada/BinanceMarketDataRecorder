from pathlib import Path
import ast,contextlib,hashlib,io,json,os,re,stat,subprocess,tempfile,types
from unittest.mock import patch
R=Path(__file__).resolve().parent
b=(R/'inputs/read_current_capacity.py').read_bytes();H=hashlib.sha256(b).hexdigest();assert H=='4f4c710c37d17e99976473387257ac3dbd89d861820b0102e3fab7a44728fda3'
top=ast.parse(b);remote=next(ast.literal_eval(n.value) for n in top.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='REMOTE' for t in n.targets));tree=ast.parse(remote)
t=next(n for n in tree.body if isinstance(n,ast.Try));first=next(i for i,n in enumerate(t.body) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='bus' for x in n.targets));part=ast.Try(body=t.body[first:],handlers=t.handlers,orelse=[],finalbody=[]);code=compile(ast.fix_missing_locations(ast.Module(body=[part],type_ignores=[])),'<isolated-correction-handoff>','exec')
REC='binance-market-data-recorder.service';AS='binance-market-data-archive.service';AT='binance-market-data-archive.timer';A='a'*32;B='b'*32
rec={'ActiveState':'inactive','UnitFileState':'disabled','MainPID':'0','NRestarts':'0','InvocationID':'c'*32}
units=['apt-daily.timer','apt-daily-upgrade.timer','apt-daily.service','apt-daily-upgrade.service','unattended-upgrades.service',AT,AS]
def worker(inv=A,active=False):return {'ActiveState':'active' if active else 'inactive','UnitFileState':'static','MainPID':'999' if active else '0','Result':'success','InvocationID':inv}
class Clock:
 CLOCK_MONOTONIC=1
 def __init__(self):self.v=0.;self.sleeps=[]
 def monotonic(self):return self.v
 def time_ns(self):return int((1000+self.v)*1e9)
 def clock_gettime_ns(self,c):return int((1000+self.v)*1e9)
 def sleep(self,s):self.sleeps.append(s);self.v+=s
class Manager:
 def GetUnit(self,u):return u
class Bus:
 def get_object(self,*a):return object()
results=[]
def case(name,expect='accept',mode='',setup=None):
 with tempfile.TemporaryDirectory(prefix='review-local-synthetic-metadata-',dir='/private/var/tmp') as td:
  d=Path(td).resolve()/'D';(d/'data/active').mkdir(parents=True)
  if setup:setup(d)
  c=Clock();ops=[];ascalls=0;propscalls=0
  def state(unit,timeout=330):
   nonlocal ascalls
   ops.append({'kind':'state','unit':unit,'timeout':timeout});c.v+=.01
   if mode=='late_success' and len(ops)==1:c.v=181
   if mode=='timeout' and len(ops)==1:raise subprocess.TimeoutExpired('synthetic-state',timeout,output=b'\xffexact\x00',stderr=b'\xfeerr')
   if mode=='busy':c.v+=60;return worker(active=True)
   if unit==REC:return {**rec,'MainPID':'1'} if mode=='recorder_changed' else dict(rec)
   if unit==AS:
    ascalls+=1
    if mode=='worker_race':return [worker(),worker(B,True),worker(B,True),worker(B),worker(B),worker(B)][min(ascalls-1,5)]
    if mode=='worker_failure':return {**worker(),'Result':'exit-code'}
    if mode=='bad_inv':return {**worker(),'InvocationID':'invalid'}
    return worker()
   return {'ActiveState':'active' if unit in ['apt-daily.timer','apt-daily-upgrade.timer','unattended-upgrades.service',AT] else 'inactive','UnitFileState':'masked' if mode=='masked_owner' and unit=='apt-daily.timer' else ('disabled' if mode=='timer_disabled' and unit==AT else 'enabled')}
  def run(argv,timeout=330):
   ops.append({'kind':'journal','argv':argv,'timeout':timeout});c.v+=.01
   return {'argv':argv,'returncode':1 if mode=='journal_rc' else 0,'stdout':'bad-json' if mode=='journal_json' else json.dumps({'__REALTIME_TIMESTAMP':'0' if mode=='old_journal' else '2'}),'stderr':''}
  class Props:
   def Get(self,*a,**kw):
    nonlocal propscalls
    propscalls+=1;ops.append({'kind':'timer','timeout':kw['timeout']});c.v+=.01
    return 0 if mode=='past_timer' and propscalls==1 else int((1000+c.v+60)*1e6)
  dbus=types.SimpleNamespace(SystemBus=lambda:Bus(),Interface=lambda obj,iface:Manager() if iface.endswith('.Manager') else Props())
  g={'Path':Path,'D':d,'os':os,'stat':stat,'state':state,'run':run,'time':c,'dbus':dbus,'json':json,'re':re,'hashlib':hashlib,'rec':dict(rec),'backlog':0,'tx':0,'original':{u:{} for u in units},'rest':{'ended_at_utc_ns':1000},'cancel':{},'verify':{},'verified':{},'PIN':'synthetic'}
  output=io.StringIO();error=None
  try:
   with contextlib.redirect_stdout(output):
    if mode=='scan_error':
     with patch('os.scandir',side_effect=PermissionError('synthetic scan failure')):exec(code,g)
    elif mode=='inventory_timeout':
     realwalk=os.walk
     def walk(*a,**kw):
      for row in realwalk(*a,**kw):c.v=181;yield row
     with patch('os.walk',side_effect=walk):exec(code,g)
    else:exec(code,g)
  except BaseException as e:error=type(e).__name__
  fail=json.loads(output.getvalue()) if output.getvalue() else None
  if expect=='accept':assert error is None,(name,error,output.getvalue());assert g['handoff']['ended_within_bound']
  else:assert error is not None,(name,'unexpected acceptance')
  x={'case':name,'expected':expect,'result':'PASS','error':error,'elapsed_seconds':c.v,'attempt_count':len(g.get('attempts',[])),'operation_timeouts':[o['timeout'] for o in ops],'failure_output':fail}
  if error is None:x.update(partial_count=g['handoff']['partial_count'],startup_custody_gate=g['handoff']['startup_custody_gate'],attempt_results=[a.get('result') for a in g['attempts']])
  if mode=='late_success':
   assert fail['failed_attempts']==[{'began_at_utc_ns':1000000000000}]
   x['successful_late_response_lost']=True
  if mode=='timeout':
   assert fail['timeout_stdout']=='\ufffdexact\x00' and fail['timeout_stderr']=='\ufffderr'
   x['original_timeout_bytes_lost']=True
  results.append(x)
case('coherent_empty_inventory')
case('metadata_only_retained_partial_gates_startup_false',setup=lambda d:(d/'data/active/test.partial').touch())
assert results[-1]['partial_count']==1 and results[-1]['startup_custody_gate'] is False
case('scan_error_is_fail_closed','reject','scan_error')
def ancestor(d):
 outside=d.parent/'outside';(outside/'active').mkdir(parents=True);(d/'data/active').rmdir();(d/'data').rmdir();(d/'data').symlink_to(outside,target_is_directory=True)
case('symlink_ancestor_rejected','reject',setup=ancestor)
case('symlink_directory_entry_rejected','reject',setup=lambda d:(d/'data/active/link').symlink_to(d.parent,target_is_directory=True))
case('symlink_file_entry_rejected','reject',setup=lambda d:(d/'data/active/link').symlink_to(d.parent/'missing'))
case('fifo_entry_rejected','reject',setup=lambda d:os.mkfifo(d/'data/active/fifo'))
case('late_success_is_rejected_but_response_not_retained','reject','late_success')
case('timeout_is_rejected_but_original_bytes_replaced','reject','timeout')
case('busy_worker_exhausts_bounded_wait','reject','busy')
case('natural_worker_race_is_retried','accept','worker_race');assert results[-1]['attempt_count']==2
case('past_timer_race_is_retried','accept','past_timer');assert results[-1]['attempt_count']==2
for m in ['recorder_changed','worker_failure','bad_inv','masked_owner','timer_disabled','journal_rc','journal_json','old_journal','inventory_timeout']:case(m,'reject',m)
result={'helper_sha256':H,'scope':'ISOLATED_LOCAL_HANDOFF_COMPONENT_REVIEW_ONLY','case_count':len(results),'cases':results,'whole_remote_or_helper_main_executed':False,'real_network_subprocess_or_service_called':False,'production_payloads_opened':False,'linux_credit':False,'formal_credit_seconds':0}
(R/'correction-fixture-results.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
print(json.dumps({'case_count':len(results),'all_expected_component_behaviors':'PASS','remaining_custody_findings':2}))
