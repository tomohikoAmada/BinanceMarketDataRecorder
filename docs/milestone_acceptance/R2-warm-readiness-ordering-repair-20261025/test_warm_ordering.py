"""Pure isolated LOCAL clock/byte cases; no controller/main/SSH/Raw execution."""
from pathlib import Path
import ast,base64,copy,hashlib,json,subprocess,types,io
Q=Path(__file__).resolve().parent
source=(Q/'controller.py').read_text();tree=ast.parse(source)
fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='canonical_readiness_during_warm')
READY={'state':'READY','evidence':{'identity':{'identity_sha256':'identity','source_git_sha':'source','config_sha256':'config'},'systemd_runtime':{'main_pid':17,'active_state':'active'}}}
RUNTIME={'InvocationID':'i'*32,'MainPID':'17','ActiveState':'active','NRestarts':'0'}
cases=[]
def case(name,*,start=850,duration=115.54344,body=None,rc=0,timeout=None,after=None,config_after=False,expected=True,invalid=None,publish_error=False,expected_error=None):
    clock=types.SimpleNamespace(now=float(start));saved=[];calls=[];states=[copy.deepcopy(RUNTIME),copy.deepcopy(after or RUNTIME)];checks=[0]
    def state(_):return states.pop(0)
    def sha(path):
        checks[0]+=1
        return ('bad' if config_after and clock.now>start and path=='config-path' else {'config-path':'config','identity-path':'identity'}[path])
    raw=json.dumps(body or READY).encode() if invalid is None else invalid
    def run(argv,**kwargs):
        calls.append((argv,kwargs));assert argv==['canonical-cli','deployment','readiness'];assert kwargs['capture_output'] is True
        assert 0<kwargs['timeout']<=330 and kwargs['timeout']<=1200-start
        clock.now+=duration
        if timeout:raise subprocess.TimeoutExpired(argv,kwargs['timeout'],output=b'\xffexact\x00',stderr=b'\xfeerr')
        return types.SimpleNamespace(returncode=rc,stdout=raw,stderr=b'')
    def publish(name,value):
        saved.append((name,copy.deepcopy(value)))
        if publish_error:raise OSError('synthetic evidence write failure')
    env={'sys':types.SimpleNamespace(stderr=io.StringIO()),'time':types.SimpleNamespace(monotonic=lambda:clock.now,time_ns=lambda:int(clock.now*1e9)),'bt':lambda:int(clock.now*1e9),'state':state,'REC':'rec','boot':lambda:'boot','sha':sha,'CONFIG':'config-path','DEPLOYMENT':'identity-path','CONFIG_SHA':'config','IDENTITY':'identity','SOURCE':'source','CLI':['canonical-cli'],'hashlib':hashlib,'json':json,'subprocess':types.SimpleNamespace(run=run,TimeoutExpired=subprocess.TimeoutExpired),'publish':publish}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated-canonical-helper>','exec'),env)
    error=None
    try:value=env['canonical_readiness_during_warm']('i'*32,'17',1200,'boot')
    except (AssertionError,KeyError,TypeError,OSError) as caught:error=type(caught).__name__
    assert (error is None)==expected,(name,error)
    if expected_error is not None:assert error==expected_error,(name,error,expected_error)
    assert len(saved)==1 and saved[0][0]=='warm-readiness-check.json'
    record=saved[0][1]
    if calls:
        pack=record['stdout_raw'];decoded=base64.b64decode(pack['base64'],validate=True);assert len(decoded)==pack['bytes'] and hashlib.sha256(decoded).hexdigest()==pack['sha256']
        assert decoded==(b'\xffexact\x00' if timeout else raw)
    assert record['formal_duration_credit_ns']==0 and record['supplies_final_start_freshness'] is False
    cases.append({'case':name,'PASS':True,'expected_ready':expected,'error_type':error})
    return record
case('earlier_single115s_check_fits_existing1200',start=880)
case('postloop115s_would_not_fit_same1200',start=1180,expected=False)
case('check_exact_deadline_does_not_pass',start=1000,duration=200,expected=False)
case('check_below_deadline_passes',start=1000,duration=199)
case('canonical_returncode_failure',rc=2,expected=False)
case('canonical_NOT_READY_fails',body={**READY,'state':'NOT_READY'},expected=False)
case('partial_JSON_retained',invalid=b'{"state":',expected=False)
case('invalid_UTF8_retained',invalid=b'\xff',expected=False)
case('timeout_invalid_UTF8_originals_retained',timeout=True,expected=False)
case('runtime_incarnation_changed',after={**RUNTIME,'InvocationID':'x'*32},expected=False)
case('runtime_PID_changed',after={**RUNTIME,'MainPID':'18'},expected=False)
case('runtime_restart_counter_changed',after={**RUNTIME,'NRestarts':'1'},expected=False)
case('runtime_inactive',after={**RUNTIME,'ActiveState':'inactive'},expected=False)
case('config_changed_after_command',config_after=True,expected=False)
for key,value in [('identity_sha256','wrong'),('source_git_sha','wrong'),('config_sha256','wrong')]:
    b=copy.deepcopy(READY);b['evidence']['identity'][key]=value;case('canonical_'+key+'_mismatch',body=b,expected=False)
for val in [18,True,'17']:
    b=copy.deepcopy(READY);b['evidence']['systemd_runtime']['main_pid']=val;case('canonical_pid_type_or_value_'+str(val),body=b,expected=False)
case('success_without_durable_evidence_pauses',publish_error=True,expected=False,expected_error='OSError')
case('initiating_failure_not_replaced_by_evidence_error',rc=2,publish_error=True,expected=False,expected_error='AssertionError')
old=ast.parse((Q/'controller-original.py').read_text())
def other(t):return ast.dump(ast.Module(body=[n for n in t.body if not isinstance(n,ast.FunctionDef) or n.name not in {'capture','canonical_readiness_during_warm'}],type_ignores=[]),include_attributes=False)
assert other(old)==other(tree);cases.append({'case':'all_other_controller_AST_unchanged','PASS':True})
cap=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capture');calls=[n for n in ast.walk(cap) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)]
assert sum(n.func.id=='canonical_readiness_during_warm' for n in calls)==1
assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='run' and any(isinstance(x,ast.Constant) and x.value=='readiness' for x in ast.walk(n)) for n in ast.walk(cap))
assert "warm_deadline=begin+1200" in source and ">=300*10**9 and samples>=12" in source
assert "validate_warm(end,initial_swap,request,strict=True)" in source and "validate_warm(start_row,initial_swap,request,strict=True)" in source
assert "TARGET_SECONDS" in source and "evaluate_fixed_slots" in source
cases.append({'case':'single_early_check_and_original1200_300_fresh_end_start_preserved','PASS':True})
result={'result':'LOCAL_BOUNDED_WARM_ORDERING_CASES_PASS','case_count':len(cases),'cases':cases,'candidate_sha256':hashlib.sha256((Q/'controller.py').read_bytes()).hexdigest(),'real_Linux_credit':False,'formal_duration_credit_ns':0,'synthetic_clock_only':True,'real_service_or_network_actions':False}
(Q/'parent-tests.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='cases'}))
