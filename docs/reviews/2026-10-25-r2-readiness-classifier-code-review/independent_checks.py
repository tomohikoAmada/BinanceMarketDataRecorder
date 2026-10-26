"""Independent exact-code adversarial checks: pure LOCAL, no operational actions."""
from pathlib import Path
import ast
from copy import deepcopy
import hashlib
import importlib.util
import json
import sys

OUT=Path(__file__).resolve().parent
I=OUT/'inputs'
sys.dont_write_bytecode=True
forbidden=[]
def audit(event,args):
    if event.startswith('socket.') or event in {'subprocess.Popen','os.system','os.posix_spawn'}:
        forbidden.append(event)
        raise RuntimeError('Independent review refuses network or subprocess')
sys.addaudithook(audit)
def sha(b):return hashlib.sha256(b).hexdigest()
old_bytes=(I/'qualification_owner_995-original.py').read_bytes()
new_bytes=(I/'qualification_owner_995.py').read_bytes()
manifest_bytes=(I/'DESIGN_MANIFEST.json').read_bytes()
assert sha(new_bytes)=='621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2'
assert sha(old_bytes)=='187224ed556f2f9f3664095b4ee34aa83be76ac57619a5263abde55b25afe608'
assert sha(manifest_bytes)=='2f6dfe123212e48631ccb4f60d87f7b47c42d109aab16388e37770f49bdeac9f'
old_ast=ast.parse(old_bytes);new_ast=ast.parse(new_bytes)
def function(tree):return next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='check_write_safety')
a,b=function(old_ast),function(new_ast)
old_lines=old_bytes.splitlines(keepends=True);new_lines=new_bytes.splitlines(keepends=True)
assert old_lines[:a.lineno-1]==new_lines[:b.lineno-1]
assert old_lines[a.end_lineno:]==new_lines[b.end_lineno:]
def non_classifier(tree):return ast.dump(ast.Module(body=[n for n in tree.body if n is not function(tree)],type_ignores=[]),include_attributes=False)
assert non_classifier(old_ast)==non_classifier(new_ast)
sets=[{n.value for n in x.comparators[0].elts} for x in ast.walk(b) if isinstance(x,ast.Compare) and isinstance(x.left,ast.Name) and x.left.id=='failure' and len(x.comparators)==1 and isinstance(x.comparators[0],ast.Set)]
expected={'planned_rotation','unexpected_disconnect','server_shutdown','ingress_backpressure','sequence_gap'}
assert sets==[expected]
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
old=module('independently_checked_original',I/'qualification_owner_995-original.py')
new=module('independently_checked_candidate',I/'qualification_owner_995.py')
tests=[]
def case(name,pub,action,**kw):
    before=json.dumps(pub,sort_keys=True);error=None;result=None
    try:result=new.check_write_safety(pub,**kw)
    except Exception as e:error=type(e).__name__+':'+str(e)
    assert json.dumps(pub,sort_keys=True)==before,'classifier mutated original evidence'
    if action=='CONTINUE':assert error is None and result is True,(name,error,result)
    elif action=='PENDING':assert error is None and result is False,(name,error,result)
    else:assert error is not None,(name,result)
    tests.append({'case':name,'expected_action':action,'actual_result':result,'actual_error':error,'input_unchanged':True,'PASS':True})
trigger=json.loads((I/'trigger-publication.json').read_bytes())
assert sha((I/'trigger-publication.json').read_bytes())=='5dcdd17d75bfbd9715ca44ead2ad9f19923db221909f8a68efa58904335957c3'
try:old.check_write_safety(deepcopy(trigger))
except RuntimeError as e:assert str(e)=='critical_product_terminal_failure'
else:raise AssertionError('actual original trigger did not reject')
case('exact_actual_all4_rotation_no_evidence_mutation',trigger,'CONTINUE')
assert trigger['core_ready'] is False and all(p['ready'] is False for ps in trigger['products'].values() for p in ps.values())
base={'status':'RUNNING','failure':None,'shutdown_reason':None,'capacity':{'actual_hard_reserve_reached':False},'products':{'spot':{'BTCUSDT':{'status':'READY','ready':True,'failure':None}},'um_perpetual':{'ETHUSDT':{'status':'READY','ready':True,'failure':None}}}}
for reason in sorted(expected):
    s=deepcopy(base);s['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure=reason);case('one_recovery_among_other_ready_'+reason,s,'CONTINUE')
    s['products']['um_perpetual']['ETHUSDT'].update(status='FAILED',ready=False,failure=None);case('last_product_FAILED_null_after_earlier_recovery_'+reason,s,'PAUSE')
for reason in ['PLANNED_ROTATION',' planned_rotation','planned_rotation\x00','unexpected_disconnect_extra',False,0,{},[]]:
    s=deepcopy(base);s['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure=reason);case('no_fuzzy_or_malformed_allowlist_'+str(len(tests)),s,'PAUSE')
for name,mutate in [
    ('missing_product_status',lambda p:p['products']['spot']['BTCUSDT'].pop('status')),
    ('missing_product_ready',lambda p:p['products']['spot']['BTCUSDT'].pop('ready')),
    ('integer_ready_not_boolean',lambda p:p['products']['spot']['BTCUSDT'].update(ready=1)),
    ('FAILED_missing_failure_is_still_terminal',lambda p:(p['products']['spot']['BTCUSDT'].update(status='FAILED',ready=None),p['products']['spot']['BTCUSDT'].pop('failure'))),
    ('later_unknown_writer_failure_after_recovery',lambda p:p['products']['um_perpetual']['ETHUSDT'].update(status='CONNECTING',ready=False,failure='OSError')),
    ('recovery_must_not_bypass_actual_reserve',lambda p:p['capacity'].update(actual_hard_reserve_reached=True)),
    ('recovery_must_not_bypass_malformed_capflag',lambda p:p['capacity'].update(actual_hard_reserve_reached='false')),
    ('recovery_must_not_bypass_missing_capacity',lambda p:p.pop('capacity')),
    ('recovery_must_not_bypass_service_failure',lambda p:p.update(failure={'task':'writer','error_type':'OSError'})),
    ('recovery_must_not_bypass_shutdown',lambda p:p.update(shutdown_reason='HARD_RESERVE_SAFETY_STOP')),
]:
    s=deepcopy(base);s['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure='planned_rotation');mutate(s);case(name,s,'PAUSE')
s=deepcopy(base);s.update(status='STARTING',capacity=None);s['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False)
case('nonempty_actual_starting_shape_pending_only_preT0',s,'PENDING',allow_starting_capacity=True)
case('same_starting_shape_rejected_without_preT0_allowance',s,'PAUSE')
s['products']['spot']['BTCUSDT']['failure']='planned_rotation';case('known_tag_does_not_bypass_running_requirement',s,'PAUSE',allow_starting_capacity=True)

# Additional pure fixed-denominator boundary/clock cases; zero wall-time credit.
ctx=frozenset([f'c{i}' for i in range(26)])
start_utc=10**18;start_bt=10**12;interval=30*10**9
def rows_with_bad(k):
    out=[]
    for slot in range(2880):
        bad=slot<k;t=slot*interval
        out.append({'observation_role':'nominal_monitor','monitor_slot':slot,'utc_ns':start_utc+t,'boottime_ns':start_bt+t,'scheduled_monitor_boottime_ns':start_bt+t,'skipped_monitor_slots':0,'config_sha256':'current-test','auxiliary_gate':{'result':'BLOCK' if bad else 'PASS','reasons':['core_not_ready'] if bad else [],'expected_auxiliary_contexts':26,'contexts':{c:{} for c in ctx}}})
    return out
def rate(rows):return new.evaluate_fixed_slots([(json.dumps(r,sort_keys=True)+'\n').encode() for r in rows],start_ns=start_utc,end_ns=start_utc+86400*10**9,start_boottime_ns=start_bt,config_sha256='current-test',expected_contexts=ctx)
for k,expect in [(14,'PASS'),(15,'BLOCK')]:
    z=rate(rows_with_bad(k)+[{'observation_role':'recovery_probe'}]*50+[{'observation_role':'finish_probe'}]*50)
    assert z['total']==2880 and z['failed']==k and z['outside_nominal']==100 and z['result']==expect
    tests.append({'case':'exact_995_boundary_'+str(k)+'_bad_100_probes_cannot_dilute','PASS':True,'synthetic_only':True,'result':expect})
for name,mutate in [
    ('UTC_BT_discontinuity_2s_rejected',lambda r:r[20].update(utc_ns=r[20]['utc_ns']+2*10**9)),
    ('duplicate_nominal_rejected',lambda r:r.append(deepcopy(r[20]))),
    ('config_identity_mismatch_rejected',lambda r:r[20].update(config_sha256='other')),
]:
    rr=rows_with_bad(0);mutate(rr)
    try:rate(rr)
    except ValueError:pass
    else:raise AssertionError(name)
    tests.append({'case':name,'PASS':True,'synthetic_only':True})
recover=new.RecoveryObserver();bad={'boottime_ns':100,'core_ready':False,'auxiliary_gate':{'reasons':['core_not_ready']},'strict_auxiliary_gate':{'result':'BLOCK'}};good={'boottime_ns':5*10**9+100,'core_ready':True,'auxiliary_gate':{'reasons':[]},'strict_auxiliary_gate':{'result':'PASS'}}
assert recover.check(bad) is True and recover.finish_endpoint(good) is True and recover.summary()['episodes']==1 and recover.summary()['observed_recovery_seconds']==5
tests.append({'case':'known_recovery_stays_quality_fault_and_observed_episode','PASS':True,'synthetic_only':True})
assert not forbidden
result={'decision':'ACCEPT','scope':'PURE_LOCAL_EXACT_PRIVATE_CLASSIFIER_CODE_REVIEW_ONLY','candidate_sha256':sha(new_bytes),'original_sha256':sha(old_bytes),'final_design_manifest_sha256':sha(manifest_bytes),'actual_trigger_sha256':sha((I/'trigger-publication.json').read_bytes()),'only_changed_function':'check_write_safety','all_non_classifier_AST_and_prefix_suffix_bytes_identical':True,'recoverable_allowlist':sorted(expected),'independent_cases':len(tests),'cases':tests,'network_subprocess_deny_hook_enabled':True,'attempted_forbidden_actions':forbidden,'Linux_qualification':False,'production_deployment_or_ops':False,'formal_duration_or_stability_credit':False}
(OUT/'INDEPENDENT_CHECKS.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='cases'},sort_keys=True))
