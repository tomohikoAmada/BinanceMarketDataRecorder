"""Actual candidate warm prefix + helper + exact source evaluator, synthetic OS/time.

No VPS/Raw/source actions and no Linux/capture/duration qualification.
"""
import ast, base64, contextlib, copy, hashlib, inspect, io, json, os, sys, tempfile, types
from pathlib import Path
from unittest.mock import patch
ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = Path('/Users/amada/.codex/worktrees/r2-close-diagnostics/BinanceMarketDataRecorder/src')
sys.path[:0] = [str(ROOT/'runtime'), str(SOURCE_ROOT)]
import warm_readiness as wr
import warm_completion as wc
import binance_market_data_recorder.service.readiness as source
import binance_market_data_recorder.service.deployment_identity as did
import binance_market_data_recorder.config as cfg
import binance_market_data_recorder.cli as cli
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.supervisor.readiness import CORE_STREAMS
from binance_market_data_recorder.binance.usdm.side_data_rest import RestSideDataKind
from qualification_auxiliary_gate import check
from qualification_owner_995 import check_write_safety

TREE=ast.parse((ROOT/'runtime/controller.py').read_text())
FUNCS=[copy.deepcopy(x) for x in TREE.body if isinstance(x,ast.FunctionDef) and x.name in {'write_sample','resource_gate','validate_warm','capture'}]
cap=next(x for x in FUNCS if x.name=='capture')
# Stop immediately before the unchanged formal monitor loop: drive every warm/start commit statement.
end=next(i for i,x in enumerate(cap.body) if isinstance(x,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='next_bt' for t in x.targets))
cap.body=cap.body[:end]
CODE=compile(ast.fix_missing_locations(ast.Module(body=FUNCS,type_ignores=[])),'<actual-candidate-warm-prefix>','exec')
BASEUTC=10**18;BASEBT=10**12;BOOT='fixture-boot';NATIVE='c'*32;INV='d'*32;PID='17';INSTANCE='fixture-instance'
CASES=[]


def scenario(name, mode='', *, expected=True, ready_after=810, cli_seconds=115.54344):
 with tempfile.TemporaryDirectory(prefix='warm-successor-flow-fixture-',dir='/private/var/tmp') as td, contextlib.ExitStack() as stack:
  q=Path(td);d=q/'D';(d/'state').mkdir(parents=True);(d/'state/catalog.sqlite').write_bytes(b'fixture-not-a-real-Catalog')
  config_path=q/'config';config_path.write_text('fixture')
  identity_path=q/'identity';identity_path.write_text(json.dumps({'source_git_sha':wr.SOURCE,'wheel_sha256':'wheel','dependency_lock_sha256':'lock','config_sha256':hashlib.sha256(config_path.read_bytes()).hexdigest(),'systemd_unit_sha256':'unit','capacity_profile_id':'vps-production-v1','package_version':'0.1.0'}))
  configsha=hashlib.sha256(config_path.read_bytes()).hexdigest();identitysha=hashlib.sha256(identity_path.read_bytes()).hexdigest()
  clock=types.SimpleNamespace(now=0.0);saved={};events=[];calls=[];source_calls=[];current_phase=['warm'];pair_count={'end':0,'start':0};primary_count=0
  def utc():return BASEUTC+round(clock.now*10**9)
  def bt():return BASEBT+round(clock.now*10**9)
  tm=types.SimpleNamespace(time_ns=utc,monotonic_ns=lambda:round(clock.now*10**9),monotonic=lambda:clock.now,sleep=lambda n:setattr(clock,'now',clock.now+n))
  config=RecorderConfig(data_root=d,spot_symbols=('BTCUSDT','ETHUSDT'),usdm_symbols=('BTCUSDT','ETHUSDT'),heartbeat_seconds=5)
  def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
  def read(path):return json.loads(Path(path).read_bytes())
  def publish(n,body):
   if mode=='evidence_write_failure' and n=='warm-readiness-check.json':raise OSError('fixture full attestation fsync failure')
   if mode=='secondary_evidence_failure' and n=='warm-readiness-check.json':raise OSError('fixture secondary evidence failure')
   path=q/n
   with path.open('x') as out:json.dump(body,out,sort_keys=True)
   saved[n]=copy.deepcopy(body);events.append((n,clock.now))
   if n=='start-observation.json' and mode in {'post_pair_identity','post_pair_stale_capacity','post_pair_hard_reserve'}:
    x=d/'state/service_state.json';v=read(x)
    if mode=='post_pair_identity':v['deployment_identity']['identity_sha256']='other'
    if mode=='post_pair_stale_capacity':v['capacity']['observed_at_utc_ns']-=3600*10**9
    if mode=='post_pair_hard_reserve':v['capacity'].update(actual_hard_reserve_reached=True,capacity_state='HARD_RESERVE',free_bytes=1)
    x.write_text(json.dumps(v))
   if mode=='slow_attestation_write' and n=='warm-readiness-check.json':clock.now=1201
   if mode=='slow_end_write' and n=='warm-completion-end.json':clock.now+=60
   if mode=='slow_start_observation_write' and n=='start-observation.json':clock.now+=60
   if mode=='slow_start_receipt_write' and n=='owner-start.json':clock.now=1201
  # Explicit fixture for kernel receipt ownership only; actual JSON/hash and every adapter run.
  def protected_read(n,digest_expected=None):
   path=q/n;raw=path.read_bytes()
   if digest_expected is not None:wr.require(hashlib.sha256(raw).hexdigest()==digest_expected,'fixture receipt hash mismatch')
   return json.loads(raw)
  runtime={'InvocationID':INV,'MainPID':PID,'NRestarts':'0','ActiveState':'active'}
  def state(unit):
   r=dict(runtime)
   if current_phase[0]!='warm':
    if mode=='recorder_restart':r['NRestarts']='1'
    if mode=='recorder_PID_changed':r['MainPID']='18'
    if mode=='recorder_inv_changed':r['InvocationID']='e'*32
   return r
  def pub(ready=True):
   p={'schema_version':'service-state.v1','status':'RUNNING' if ready else 'STARTING','pid':17,'failure':None,'shutdown_reason':None,'core_ready':ready,'service_instance_id':INSTANCE,'heartbeat_at_utc_ns':utc(),'heartbeat_interval_seconds':5,'capacity_profile_id':'vps-production-v1','catalog_open':True,'startup_recovery_complete':True,'deployment_identity':{'identity_sha256':identitysha,'source_git_sha':wr.SOURCE,'wheel_sha256':'wheel','config_sha256':configsha,'systemd_unit_sha256':'unit','capacity_profile_id':'vps-production-v1'},'capacity':{'observed_at_utc_ns':utc(),'total_bytes':100*1024**3,'free_bytes':25*1024**3,'capacity_profile':'vps-production-v1','capacity_state':'NORMAL','actual_hard_reserve_reached':False},'products':{},'global_usdm_side_data':{}}
   rest={x.value for x in RestSideDataKind}-{'funding_info','exchange_info'}
   for market in ['spot','um_perpetual']:
    p['products'][market]={}
    for symbol in ['BTCUSDT','ETHUSDT']:
     kinds={'exchange_info'} if market=='spot' else rest|{'mark_price','liquidation'}
     sides={k:{'enabled':True,'running':True,'status':'RUNNING','connected':True,'accepted':1,'last_success_at_utc_ns':utc(),'cursor':{'symbol':symbol,'kind':k,'last_persisted_period_timestamp':(utc()//10**6//300000-1)*300000}} for k in kinds}
     p['products'][market][symbol]={'market':market,'symbol':symbol,'status':'READY','ready':True,'failure':None,'connected_streams':sorted(CORE_STREAMS),'persisted_streams':sorted(CORE_STREAMS),'snapshot_persisted':True,'orderbook_synchronized':True,'side_data':sides}
   for k in ['funding_info','exchange_info']:p['global_usdm_side_data'][k]={'enabled':True,'running':True,'status':'RUNNING','accepted':1,'last_success_at_utc_ns':utc()}
   phase=current_phase[0]
   if phase!='warm':
    if mode=='stale_capacity':p['capacity']['observed_at_utc_ns']-=3600*10**9
    if mode=='future_capacity':p['capacity']['observed_at_utc_ns']+=6*10**9
    if mode=='wrong_published_identity':p['deployment_identity']['identity_sha256']='other'
    if mode=='runtime_profile':p['capacity_profile_id']='other'
    if mode=='capacity_profile':p['capacity']['capacity_profile']='other'
    if mode=='capacity_enum':p['capacity']['capacity_state']='OTHER'
    if mode=='forecast_CRITICAL':p['capacity']['capacity_state']='CRITICAL'
    if mode=='capacity_invalid_total':p['capacity']['total_bytes']=True
    if mode=='capacity_free_gt_total':p['capacity']['free_bytes']=200*1024**3
    if mode=='hard_reserve':p['capacity']['actual_hard_reserve_reached']=True
    if mode=='source_HARD_RESERVE':p['capacity']['capacity_state']='HARD_RESERVE'
    if mode=='catalog_closed':p['catalog_open']=False
    if mode=='startup_recovery':p['startup_recovery_complete']=False
    if mode=='topology':p['products']['spot'].pop('ETHUSDT')
    if mode=='product_identity':p['products']['spot']['BTCUSDT']['symbol']='OTHER'
    if mode=='product_core_persistence':p['products']['spot']['BTCUSDT']['persisted_streams']=[]
    if mode=='product_failure':p['products']['spot']['BTCUSDT']['failure']='unexpected'
    if mode=='state_schema':p['schema_version']='other'
    if mode=='heartbeat_stale':p['heartbeat_at_utc_ns']-=3600*10**9
    if mode=='heartbeat_future':p['heartbeat_at_utc_ns']+=6*10**9
    if mode=='instance_change':p['service_instance_id']='other'
    if mode=='end_good_start_bad' and phase=='start':p['deployment_identity']['identity_sha256']='other'
    if mode=='auxiliary_loss':p['products']['spot']['BTCUSDT']['side_data']['exchange_info']['status']='RETRYING'
   return p
  def sample(config,inv,pid,strict=False):
   nonlocal primary_count
   phase=inspect.currentframe().f_back.f_locals.get('phase','warm');current_phase[0]=phase
   primary_count+=phase=='warm'
   ready=clock.now>=ready_after
   if mode=='middle_BLOCK' and 950<clock.now<980 and phase=='warm':ready=False
   p=pub(ready);raw=json.dumps(p,sort_keys=True).encode();(d/'state/service_state.json').write_bytes(raw)
   gate=check(p,config,now_ns=utc(),allow_empty_recovery=False)
   res={'rss_bytes':100*1024**2,'host_available_bytes':4*1024**3,'cgroup_swap_bytes':0,'cgroup_swap_peak_bytes':0,'writer_free_bytes':25*1024**3,'archive_free_bytes':2*1024**4,'pswpin':0,'pswpout':0}
   if mode=='current_disk_failure' and phase!='warm':res['writer_free_bytes']=1
   return {'utc_ns':utc(),'boottime_ns':bt(),'boot_id':BOOT if mode!='boot_change' or phase=='warm' else 'other','config_sha256':configsha,'runtime':state('rec'),'core_ready':p['core_ready'],'heartbeat_at_utc_ns':p['heartbeat_at_utc_ns'],'recorder_service_instance_id':p['service_instance_id'],'resources':res,'auxiliary_gate':copy.deepcopy(gate),'strict_auxiliary_gate':gate,'state_publication_base64':base64.b64encode(raw).decode(),'state_publication_bytes':len(raw),'state_sha256':hashlib.sha256(raw).hexdigest()}
  identity=types.SimpleNamespace(identity_sha256=identitysha,source_git_sha=wr.SOURCE,wheel_sha256='wheel',config_sha256=configsha,systemd_unit_sha256='unit',capacity_profile_id='vps-production-v1',systemd_effective={'user':'bmdr','group':'bmdr'})
  class Manager:
   def verify_install_contract(self):
    if mode=='install_contract' and current_phase[0]!='warm':raise did.DeploymentIdentityError('fixture install contract changed')
    return {'valid':True}
   def verify_effective_properties(self,**kw):
    if mode=='effective_properties' and current_phase[0]!='warm':raise did.DeploymentIdentityError('fixture effective properties changed')
    return {'valid':True}
   def runtime_properties(self):return {'active_state':'active','main_pid':17}
  def fixture_verify(value,**kw):
   assert kw['verify_installed'] is False and kw['require_root_controlled'] is True
   if mode=='small_file_change' and current_phase[0]!='warm':raise did.DeploymentIdentityError('fixture current identity hash changed')
   return {'identity_sha256':identitysha,'installed_artifact':None,'installed_dependencies':None}
  def fixture_chain(*a,**kw):
   if mode=='namespace_change' and current_phase[0]!='warm':raise did.DeploymentIdentityError('fixture namespace writable')
   return {'symlink_free':True,'service_writable':False}
  def full_ready():
   evidence_identity={'identity_sha256':identitysha,**read(identity_path),'installed_artifact':{'record_verified_files':125,'editable':False,'package_version':'0.1.0','venv_root':str(Path(wr.PYTHON).parent.parent),'module_path':str(Path(wr.PYTHON).parent.parent/'lib/python3.12/site-packages/binance_market_data_recorder/__init__.py'),'dist_info_path':str(Path(wr.PYTHON).parent.parent/'lib/python3.12/site-packages/binance_market_data_recorder-0.1.0.dist-info'),'direct_url_path':str(Path(wr.PYTHON).parent.parent/'lib/python3.12/site-packages/binance_market_data_recorder-0.1.0.dist-info/direct_url.json')},'installed_dependencies':{'locked_distribution_count':28,'installed_distribution_count':28,'locked_distributions':{'dep'+str(i):'1.0' for i in range(28)},'protected_file_count':100,'protected_directory_count':10,'exact_match':True,'editable':False,'recorder_distribution_separate':True},'venv_control_chain':{'symlink_free':True,'service_writable':False},'identity_permissions':{'mode':'0640'}}
   return {'command':'deployment.readiness','schema_version':'deployment-readiness.v1','state':'READY','reasons':[],'evidence':{'identity':evidence_identity,'systemd_runtime':{'main_pid':17,'active_state':'active'},'service_state':pub(True)}}
  def command(argv,budget,cap):
   before=wr.clocks()
   if argv[-2:]==['deployment','readiness']:
    calls.append(clock.now);clock.now+=cli_seconds
    body=full_ready()
    (d/'state/service_state.json').write_text(json.dumps(body['evidence']['service_state'],sort_keys=True))
    if mode=='contradictory_static':body['evidence']['identity']['installed_dependencies'].update(exact_match=False,editable=True,locked_distribution_count=0)
    if mode=='boolean_RECORD':body['evidence']['identity']['installed_artifact']['record_verified_files']=True
    if mode in {'canonical_NOT_READY','secondary_evidence_failure'}:body['state']='NOT_READY'
    raw=json.dumps(body).encode()
    if mode=='canonical_malformed':raw=b'\xffpartial-json'
    if mode=='bare_READY':raw=b'{"state":"READY"}'
    timed=mode=='canonical_timeout';rc=2 if body.get('state')!='READY' else 0
   else:
    phase=argv[-5];pair_count[phase]+=1
    if mode=='catalog_replaced':
     x=d/'state/catalog.sqlite';x.rename(x.with_suffix('.old'));x.write_text('new inode')
    if mode=='catalog_growth':
     with (d/'state/catalog.sqlite').open('ab') as out:out.write(b'ordinary size growth')
    if mode=='cache_tampered':
     (q/'warm-readiness-check.json').write_text('{"forged":true}')
    if mode=='one_pair_race' and pair_count[phase]==1 or mode=='endless_pair_race':
     x=d/'state/service_state.json';v=read(x);v['fixture_publication_sequence']=pair_count[phase];x.write_text(json.dumps(v))
    defaults=dict(source.VpsReadinessEvaluator.__init__.__kwdefaults__)
    defaults.update(utc_clock_ns=utc,process_alive=lambda pid:mode!='process_dead',process_environment=lambda pid:{'HTTP_PROXY':'fixture'} if mode=='process_proxy' else {})
    with patch.dict(source.VpsReadinessEvaluator.__init__.__kwdefaults__,defaults):
     try:
      result=wc.evaluate(*argv[-5:]);source_calls.append(result['source_result']);raw=json.dumps(result).encode();rc=0
     except BaseException as exc:
      raw=json.dumps({'error_type':type(exc).__name__,'error':str(exc)}).encode();rc=2
    clock.now+=0.2
    if mode=='current_check_late':clock.now+=31
    timed=False
   try:parsed=json.loads(raw);error=None
   except (ValueError,UnicodeError) as exc:parsed=None;error=type(exc).__name__
   after=wr.clocks()
   if mode=='reversed_command_clock' and argv[-2:]==['deployment','readiness']:after['utc_ns']=before['utc_ns']-1
   return {'argv':argv,'before':before,'after':after,'timed_out':timed,'returncode':rc,'stdout':wr.encoded(raw),'stderr':wr.encoded(b'fixture stderr' if rc else b''),'parse_error':error,'parsed_stdout':parsed,'fixture_process_and_OS':True}
  for mod in [wr,wc]:
   for k,v in {'Q':q,'D':d,'CONFIG':config_path,'DEPLOYMENT':identity_path,'CONFIG_SHA':configsha,'IDENTITY':identitysha}.items():stack.enter_context(patch.object(mod,k,v,create=True))
  for k,v in {'time':tm,'bt':bt,'boot':lambda:BOOT,'state':state,'publish':publish,'read':read,'protected_read':protected_read,'owned_command':command,'origin_guard':lambda:{'fixture_source_sha':digest(SOURCE_ROOT/'binance_market_data_recorder/service/readiness.py')}}.items():stack.enter_context(patch.object(wr,k,v))
  stack.enter_context(patch.object(wc,'protected_read',protected_read))
  stack.enter_context(patch.object(cfg,'load_config',lambda **kw:types.SimpleNamespace(config=config,config_file=config_path)))
  stack.enter_context(patch.object(did,'load_deployment_identity',lambda path:identity))
  stack.enter_context(patch.object(did,'enforce_vps_paths',lambda value:None))
  stack.enter_context(patch.object(did,'verify_identity_files',fixture_verify))
  stack.enter_context(patch.object(did,'verify_vps_identity_permissions',lambda *a,**kw:{'mode':'0640'}))
  stack.enter_context(patch.object(did,'verify_vps_control_chain',fixture_chain))
  stack.enter_context(patch.object(cli,'_identity_systemd_manager',lambda *a,**kw:Manager()))
  stack.enter_context(patch.dict(os.environ,{'INVOCATION_ID':NATIVE}))
  def boundary(*a,**kw):
   f=q/'start-inventory.sqlite';f.write_text('fixture metadata only');return {'start_catalog_path':str(f),'scope':'new-only-fixture'}
  def admission(*args):
   if len(args)==3 and args[0]==86400+660:
    assert args[2]>0
    if mode=='slow_prestart_admission':clock.now+=150
   return {'result':'ADMITTED','fixture':True}
  def run(*a,**kw):clock.now+=1;return ''
  ns={'REC':wr.REC,'WarmStart':wr.WarmStart,'Q':q,'D':d,'CONFIG':config_path,'DEPLOYMENT':identity_path,'CONFIG_SHA':configsha,'IDENTITY':identitysha,'SOURCE':wr.SOURCE,'DEPLOY_SHA':'fixture','time':tm,'bt':bt,'boot':lambda:BOOT,'state':state,'sample':sample,'publish':publish,'sha':digest,'read':lambda path:{'pid':17},'run':run,'exact':lambda:None,'stopped':lambda:None,'admission':admission,'configured_cohort':lambda config:{'products':[],'expected_contexts':set(),'spools':42},'capture_boundary':boundary,'Path':Path,'json':json,'os':types.SimpleNamespace(chown=lambda *a:None,chmod=lambda *a:None,fsync=lambda *a:None,environ=os.environ),'grp':types.SimpleNamespace(getgrnam=lambda n:types.SimpleNamespace(gr_gid=988)),'retain_observation':lambda *a:None,'uuid':__import__('uuid'),'TARGET_SECONDS':86400,'TARGET_NS':86400*10**9,'predecessor':lambda:{},'POLICY':{},'FINALIZATION_POLICY':{}}
  exec(CODE,ns);failure=None
  try:
   with contextlib.redirect_stdout(io.StringIO()):ns['capture']()
  except BaseException as exc:failure=type(exc).__name__+':'+str(exc)
  accepted='owner-start-commit.json' in saved and failure is None
  assert accepted==expected,(name,failure,clock.now,accepted,source_calls[-1:] if source_calls else [])
  if accepted:
   warm=saved['warmup-result.json'];assert warm['samples']>=12
   assert warm['end_strict']['boottime_ns']-warm['first_strict']['boottime_ns']>=300*10**9
   assert clock.now<1200 and len(calls)==1
   assert all(x['evidence']['identity']['installed_artifact'] is None for x in source_calls)
   assert saved['owner-start.json']['eligibility_requires']=='owner-start-commit.json'
  assert len(calls)<=1
  CASES.append({'name':name,'expected':expected,'PASS':True,'accepted_start':accepted,'failure':failure,'clock_end':clock.now,'primary_samples':primary_count,'full_CLI_count':len(calls),'pair_attempts':pair_count,'source_results':[{'state':x['state'],'reasons':x['reasons']} for x in source_calls]})


scenario('observed late first-PASS shape fits without30s rounding waste')
scenario('early ready still requires12 actual primary rows',ready_after=0)
scenario('late first-PASS endpoint cap',ready_after=870)
scenario('recoverable middle warm BLOCK',mode='middle_BLOCK')
scenario('ordinary catalog growth retains earlier-only scope',mode='catalog_growth')
scenario('forecast CRITICAL without actual hard reserve is eligible',mode='forecast_CRITICAL')
scenario('one publication race receives bounded fresh pair',mode='one_pair_race')
scenario('no first PASS',ready_after=2000,expected=False)
scenario('late full canonical READY',ready_after=1050,cli_seconds=200,expected=False)
for mode in ['canonical_NOT_READY','canonical_malformed','canonical_timeout','bare_READY','evidence_write_failure','secondary_evidence_failure','slow_attestation_write','slow_end_write','slow_start_observation_write','slow_start_receipt_write','stale_capacity','future_capacity','wrong_published_identity','runtime_profile','capacity_profile','capacity_enum','capacity_invalid_total','capacity_free_gt_total','hard_reserve','source_HARD_RESERVE','catalog_closed','startup_recovery','topology','product_identity','product_core_persistence','product_failure','state_schema','heartbeat_stale','heartbeat_future','instance_change','end_good_start_bad','auxiliary_loss','recorder_restart','recorder_PID_changed','recorder_inv_changed','boot_change','current_disk_failure','install_contract','effective_properties','small_file_change','namespace_change','process_dead','process_proxy','catalog_replaced','cache_tampered','endless_pair_race','current_check_late','slow_prestart_admission','post_pair_identity','post_pair_stale_capacity','post_pair_hard_reserve','contradictory_static','boolean_RECORD','reversed_command_clock']:
 scenario(mode,mode=mode,expected=False)
(ROOT/'LOCAL_FULL_FLOW_RESULTS.json').write_text(json.dumps({'scope':'SYNTHETIC_OS_CLOCK_FULL_ACTUAL_WARM_PREFIX_AND_HELPER_WITH_EXACT_SOURCE_EVALUATOR','cases':CASES,'case_count':len(CASES),'all_PASS':True,'real_Linux_or_duration_credit':False,'OS_identity_permissions_and_source_static_verifiers_explicit_fixture':True,'Raw_or_VPS_access':False},indent=2)+'\n')
print(json.dumps({'LOCAL_full_flow_cases':len(CASES),'all_PASS':True,'real_Linux_credit':False}))
