from pathlib import Path
import ast,base64,contextlib,copy,hashlib,inspect,io,json,os,subprocess,sys,tempfile,types,uuid
from unittest.mock import patch
R=Path(__file__).resolve().parent;source=(R/'inputs/controller.py').read_text();tree=ast.parse(source)
helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='canonical_readiness_during_warm');capture=copy.deepcopy(next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capture'))
def publishes(n,name):return isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='publish' and n.value.args and isinstance(n.value.args[0],ast.Constant) and n.value.args[0].value==name
end=next(i for i,n in enumerate(capture.body) if publishes(n,'owner-start.json'));capture.body=capture.body[:end+1]
functions=[copy.deepcopy(n) for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'write_sample','resource_gate','validate_warm'}]+[helper,capture]
code=compile(ast.fix_missing_locations(ast.Module(body=functions,type_ignores=[])),'<isolated-complete-warm-prefix>','exec')
INV='i'*32;PID='17';BOOT='boot';INSTANCE='service-instance';BASEUTC=10**18;BASEBT=10**12
runtime={'InvocationID':INV,'MainPID':PID,'ActiveState':'active','NRestarts':'0'}
CASES=[]
def scenario(name,*,ready_after=810,cli_duration=115.54344,mode='',expected_t0=False,expected_error=None):
 with tempfile.TemporaryDirectory(prefix='review-local-full-warm-prefix-',dir='/private/var/tmp') as td:
  q=Path(td);clock=types.SimpleNamespace(now=0.0);events=[];saved={};cli=[];lastpub={};sample_counts=collections_counter={};end_samples=[];failure=None
  def utc():return BASEUTC+int(clock.now*1e9)
  def bt():return BASEBT+int(clock.now*1e9)
  def boot():return BOOT
  def state(unit):return dict(runtime)
  def run(argv,timeout=30):clock.now+=1.0;events.append(('service-request',clock.now));return ''
  def publish(n,b):
   events.append(('publish:'+n,clock.now));saved[n]=copy.deepcopy(b)
   if mode=='slow_success_evidence_write' and n=='warm-readiness-check.json':clock.now=1201
   if mode=='fail_success_evidence_write' and n=='warm-readiness-check.json':raise OSError('synthetic durability fault')
  def sha(path):return {'config':'config','identity':'identity'}.get(str(path),'hash')
  def boundary(*a,**kw):
   p=q/'start-catalog.sqlite';p.touch();return {'start_catalog_path':str(p),'scope':'new-only'}
  def sample(config,inv,pid,strict=False):
   nonlocal lastpub
   frame=inspect.currentframe().f_back;role='start' if 'warmup-result.json' in saved else ('end' if frame.f_locals.get('out') is not None and frame.f_locals['out'].closed else 'warm')
   sample_counts[role]=sample_counts.get(role,0)+1
   ready=clock.now>=ready_after
   if mode=='recoverable_bad_middle' and 950<clock.now<1000 and role=='warm':ready=False
   if mode=='end_not_strict' and role=='end':ready=False
   if mode=='start_not_strict' and role=='start':ready=False
   instance='changed-instance' if mode=='instance_changed_after_early_check' and cli and role=='warm' else INSTANCE
   bid='changed-boot' if mode=='boot_changed_after_early_check' and cli and role=='warm' else BOOT
   res={'rss_bytes':100*1024**2,'host_available_bytes':4*1024**3,'cgroup_swap_bytes':0,'cgroup_swap_peak_bytes':0,'writer_free_bytes':25*1024**3,'archive_free_bytes':2*1024**4,'pswpin':0,'pswpout':0}
   if mode=='late_resource_failure' and role=='end':res['writer_free_bytes']=1
   pub={'status':'RUNNING' if ready else 'STARTING','pid':17,'failure':None,'shutdown_reason':None,'catalog_open':True,'startup_recovery_complete':ready,'deployment_identity':{'identity_sha256':'identity','source_git_sha':'source','config_sha256':'config'},'capacity_profile_id':'vps-production-v1','core_ready':ready,'capacity':{'actual_hard_reserve_reached':False,'observed_at_utc_ns':utc(),'capacity_profile':'vps-production-v1','free_bytes':25*1024**3,'total_bytes':60*1024**3,'capacity_state':'NORMAL'}}
   if mode=='stale_capacity_after_early_check' and role in {'end','start'}:pub['capacity']['observed_at_utc_ns']=utc()-3600*10**9
   if mode=='wrong_published_identity_after_early_check' and role in {'end','start'}:pub['deployment_identity']['identity_sha256']='untrusted-other-identity'
   lastpub=copy.deepcopy(pub)
   gate={'result':'PASS' if ready else 'BLOCK','reasons':[] if ready else ['core_not_ready']}
   row={'utc_ns':utc(),'boottime_ns':bt(),'boot_id':bid,'heartbeat_at_utc_ns':utc(),'resources':res,'core_ready':ready,'recorder_service_instance_id':instance,'auxiliary_gate':copy.deepcopy(gate),'strict_auxiliary_gate':copy.deepcopy(gate),'publication_complete':True,'write_safety_verified':True,'state_publication_base64':base64.b64encode(json.dumps(pub).encode()).decode()}
   if role in {'end','start'}:end_samples.append({'role':role,'publication':copy.deepcopy(pub)})
   return row
  def command(argv,capture_output,timeout):
   assert argv==['canonical-cli','deployment','readiness'] and capture_output and 0<timeout<=330 and timeout<=1200-clock.now
   cli.append({'began':clock.now,'timeout':timeout,'sampled_publication':copy.deepcopy(lastpub)});events.append(('canonical-cli',clock.now));clock.now+=cli_duration
   if mode=='canonical_timeout':raise subprocess.TimeoutExpired(argv,timeout,output=b'\xfforiginal',stderr=b'partial')
   body={'state':'READY','evidence':{'identity':{'identity_sha256':'identity','source_git_sha':'source','config_sha256':'config'},'systemd_runtime':{'main_pid':17,'active_state':'active'},'service_state':copy.deepcopy(lastpub)}}
   if mode=='canonical_not_ready':body['state']='NOT_READY'
   return types.SimpleNamespace(returncode=0,stdout=json.dumps(body).encode(),stderr=b'')
  configmod=types.ModuleType('binance_market_data_recorder.config');configmod.load_config=lambda **kw:types.SimpleNamespace(config=types.SimpleNamespace())
  ownermod=types.ModuleType('qualification_owner_995');ownermod.evaluate_fixed_slots=lambda *a,**kw:None
  tm=types.SimpleNamespace(monotonic=lambda:clock.now,time_ns=utc,sleep=lambda seconds:setattr(clock,'now',clock.now+seconds))
  env={'time':tm,'bt':bt,'boot':boot,'state':state,'run':run,'publish':publish,'sha':sha,'CONFIG':'config','DEPLOYMENT':'identity','CONFIG_SHA':'config','IDENTITY':'identity','SOURCE':'source','CLI':['canonical-cli'],'REC':'rec','D':q/'D','Q':q,'Path':Path,'json':json,'hashlib':hashlib,'subprocess':types.SimpleNamespace(run=command,TimeoutExpired=subprocess.TimeoutExpired),'sys':types.SimpleNamespace(stderr=io.StringIO()),'os':types.SimpleNamespace(chown=lambda *a:None,chmod=lambda *a:None,fsync=os.fsync,environ={'INVOCATION_ID':'native'}),'grp':types.SimpleNamespace(getgrnam=lambda n:types.SimpleNamespace(gr_gid=988)),'capture_boundary':boundary,'exact':lambda:None,'stopped':lambda:None,'admission':lambda *a:{'result':'ADMITTED'},'configured_cohort':lambda c:{'products':[],'expected_contexts':set(),'spools':42},'sample':sample,'read':lambda p:{'pid':17},'retain_observation':lambda *a:None,'uuid':uuid,'TARGET_SECONDS':86400,'TARGET_NS':86400*10**9,'DEPLOY_SHA':'deploy','predecessor':lambda:{},'POLICY':{},'FINALIZATION_POLICY':{},'EXPECTED':runtime}
  exec(code,env)
  with patch.dict(sys.modules,{'binance_market_data_recorder.config':configmod,'qualification_owner_995':ownermod}):
   try:
    with contextlib.redirect_stdout(io.StringIO()):env['capture']()
   except BaseException as caught:failure=type(caught).__name__+':'+str(caught)
  reached='owner-start.json' in saved;assert reached==expected_t0,(name,failure,reached,clock.now)
  if expected_error:assert failure and failure.startswith(expected_error),(name,failure)
  assert len(cli)<=1
  if reached:
   warm=saved['warmup-result.json'];assert warm['samples']>=12 and warm['end_strict']['boottime_ns']-warm['first_strict']['boottime_ns']>=300*10**9 and warm['elapsed_seconds']<1200 and sample_counts['end']==sample_counts['start']==1
   assert len(cli)==1 and cli[0]['began']==(warm['first_strict']['boottime_ns']-BASEBT)/1e9
  z={'case':name,'PASS':True,'T0_reached':reached,'failure':failure,'clock_end':clock.now,'canonical_call_count':len(cli),'sample_counts':sample_counts,'event_order':events,'fresh_end_start_publications':end_samples}
  if mode in {'stale_capacity_after_early_check','wrong_published_identity_after_early_check'}:z['source_dynamic_predicate_bypass_reproduced']=True
  CASES.append(z)
scenario('overlap_fits_actual_shape',expected_t0=True)
scenario('early_ready_still_requires_minimum12_and_300span',ready_after=0,expected_t0=True)
scenario('recoverable_middle_BLOCK_does_not_duplicate_canonical',mode='recoverable_bad_middle',expected_t0=True)
scenario('no_ready_before_original1200_bound',ready_after=2000,expected_t0=False,expected_error='AssertionError')
scenario('late_canonical_READY_still_fails',ready_after=1050,cli_duration=200,expected_t0=False,expected_error='AssertionError')
scenario('canonical_NOT_READY_fails',mode='canonical_not_ready',expected_t0=False,expected_error='AssertionError')
scenario('canonical_timeout_retains_and_fails',mode='canonical_timeout',expected_t0=False,expected_error='AssertionError')
scenario('end_strict_failure_prevents_T0',mode='end_not_strict',expected_t0=False,expected_error='RuntimeError')
scenario('start_strict_failure_prevents_T0',mode='start_not_strict',expected_t0=False,expected_error='RuntimeError')
scenario('same_service_instance_required_after_early_check',mode='instance_changed_after_early_check',expected_t0=False,expected_error='AssertionError')
scenario('same_boot_required_after_early_check',mode='boot_changed_after_early_check',expected_t0=False,expected_error='AssertionError')
scenario('current_resource_failure_prevents_T0',mode='late_resource_failure',expected_t0=False,expected_error='RuntimeError')
scenario('evidence_write_failure_halts_READY_path',mode='fail_success_evidence_write',expected_t0=False,expected_error='OSError')
scenario('slow_durability_cannot_overrun1200_into_T0',mode='slow_success_evidence_write',expected_t0=False,expected_error='AssertionError')
scenario('stale_capacity_after_earlier_READY_currently_reaches_T0',mode='stale_capacity_after_early_check',expected_t0=True)
scenario('changed_published_identity_after_earlier_READY_currently_reaches_T0',mode='wrong_published_identity_after_early_check',expected_t0=True)
output={'scope':'ISOLATED_FULL_CAPTURE_WARM_PREFIX_WITH_SYNTHETIC_TIME_AND_METADATA_ONLY','candidate_sha256':hashlib.sha256(source.encode()).hexdigest(),'case_count':len(CASES),'cases':CASES,'actual_remote_helper_main_or_source_actions':False,'production_Raw_payload_opened':False,'real_Linux_or_duration_credit':False}
(R/'FULL_WARM_FLOW_CASES.json').write_text(json.dumps(output,indent=2,sort_keys=True)+'\n');print(json.dumps({'case_count':len(CASES),'all_expected_behaviors':'PASS','source_dynamic_predicate_bypasses':2}))
