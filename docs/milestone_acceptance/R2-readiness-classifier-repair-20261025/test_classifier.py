"""Pure LOCAL classifier regression and fixed-monitor acceptance; no operational actions."""
from pathlib import Path
import ast,copy,hashlib,importlib.util,json
Q=Path(__file__).resolve().parent

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
old=module('original_policy',Q/'qualification_owner_995-original.py')
new=module('corrected_policy',Q/'qualification_owner_995.py')
cases=[]
def expect(name,pub,ok,**kw):
    error=None
    try:result=new.check_write_safety(pub,**kw)
    except (RuntimeError,ValueError,KeyError,TypeError) as caught:error=type(caught).__name__
    assert (error is None)==ok,(name,error)
    cases.append({'case':name,'PASS':True,'expected_action':'continue_quality_logging' if ok else 'pause_untrusted_or_terminal','error_type':error})
base={'status':'RUNNING','failure':None,'shutdown_reason':None,'capacity':{'actual_hard_reserve_reached':False},'products':{'spot':{'BTCUSDT':{'status':'READY','ready':True,'failure':None}}}}
reasons=['planned_rotation','unexpected_disconnect','server_shutdown','ingress_backpressure','sequence_gap']
expect('normal_ready',copy.deepcopy(base),True)
for reason in reasons:
    pub=copy.deepcopy(base);pub['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure=reason)
    expect('recognized_connecting_'+reason,pub,True)
    pub['products']['spot']['BTCUSDT'].update(status='FAILED');expect('real_failed_even_with_'+reason,pub,False)
for status,ready,failure in [('READY',True,'planned_rotation'),('CONNECTING',True,'planned_rotation'),('CONNECTING',False,'unknown_reason'),('MISSING',False,'planned_rotation'),('READY',False,None),('CONNECTING','false','planned_rotation'),([],False,'planned_rotation'),('CONNECTING',False,7),('CONNECTING',False,['planned_rotation'])]:
    pub=copy.deepcopy(base);pub['products']['spot']['BTCUSDT'].update(status=status,ready=ready,failure=failure);expect('malformed_or_unproven_'+str(len(cases)),pub,False)
pub=copy.deepcopy(base);pub['products']['spot']['BTCUSDT'].update(status='FAILED',ready=False,failure=None);expect('FAILED_even_without_failure_text',pub,False)
for tag,mutate in [('service_failure',lambda p:p.update(failure={'error_type':'OSError'})),('shutdown',lambda p:p.update(shutdown_reason='SIGTERM')),('actual_disk_reserve',lambda p:p['capacity'].update(actual_hard_reserve_reached=True)),('invalid_disk_flag',lambda p:p['capacity'].update(actual_hard_reserve_reached=0)),('missing_disk_flag',lambda p:p.update(capacity={})),('writer_error',lambda p:p['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure='OSError')),('write_fatal',lambda p:p['products']['spot']['BTCUSDT'].update(status='FAILED',ready=False,failure='WriterFailure'))]:
    pub=copy.deepcopy(base);pub['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure='planned_rotation');mutate(pub);expect(tag,pub,False)
pub=copy.deepcopy(base);pub['products']['spot']['BTCUSDT'].update(status='CONNECTING',ready=False,failure='planned_rotation');pub['status']='STARTING';expect('recovery_tag_needs_trusted_running_service',pub,False)
pub=copy.deepcopy(base);pub['status']='STARTING';pub['products']={};pub['capacity']=None
assert new.check_write_safety(pub,allow_starting_capacity=True) is False
cases.append({'case':'existing_bounded_initial_capacity_pending_preserved','PASS':True,'result':False})
expect('capacity_pending_not_allowed_in_formal',pub,False)
# Exact real triggering metadata, not Raw. Original rejected; correction must
# only remove the false terminal-stop classification, never call it READY.
trigger=Q/'trigger-publication.json'
pub=json.loads(trigger.read_bytes());assert hashlib.sha256(trigger.read_bytes()).hexdigest()=='5dcdd17d75bfbd9715ca44ead2ad9f19923db221909f8a68efa58904335957c3'
try:old.check_write_safety(pub)
except RuntimeError as error:assert str(error)=='critical_product_terminal_failure'
else:raise AssertionError('old exact trigger did not reproduce')
expect('real_planned_rotation_trigger_stays_write_safe_but_not_ready',pub,True);assert pub['core_ready'] is False and all(v['ready'] is False for m in pub['products'].values() for v in m.values())
mixed=copy.deepcopy(pub);mixed['products']['um_perpetual']['ETHUSDT'].update(status='FAILED',ready=False,failure='planned_rotation');expect('mixed_four_products_real_FAILED_not_masked_by_other_rotations',mixed,False)
mixed=copy.deepcopy(pub);mixed['capacity']['actual_hard_reserve_reached']=True;expect('four_recoveries_do_not_bypass_actual_disk_stop',mixed,False)
# No unrelated runtime/clock/recovery/denominator algorithm is edited.
def rest(tree):return ast.dump(ast.Module(body=[n for n in tree.body if not isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) or n.name!='check_write_safety'],type_ignores=[]),include_attributes=False)
assert rest(ast.parse((Q/'qualification_owner_995-original.py').read_text()))==rest(ast.parse((Q/'qualification_owner_995.py').read_text()))
cases.append({'case':'all_other_policy_functions_and_constants_AST_identical','PASS':True})
# Complete synthetic86400/2880 nominal proof: recoverable rotations are still
# bad availability slots; extra probes never dilute them, missing/late count bad.
contexts=frozenset(['spot:BTCUSDT:exchange_info','spot:ETHUSDT:exchange_info']+[f'um_perpetual:{s}:{k}' for s in ['BTCUSDT','ETHUSDT'] for k in ['basis_5m','funding_history','global_long_short_ratio_5m','liquidation','mark_price','open_interest','open_interest_statistics_5m','premium_index_snapshot','taker_buy_sell_volume_5m','top_long_short_account_ratio_5m','top_long_short_position_ratio_5m']]+['um_perpetual:GLOBAL:exchange_info','um_perpetual:GLOBAL:funding_info']);assert len(contexts)==26
start_utc=1000000000000000000;start_bt=200000000000;config='5b73';rows=[]
for slot in range(2880):
    bad=slot in {200,400,600,800,2847};t=slot*30000000000
    gate={'result':'BLOCK' if bad else 'PASS','reasons':['core_not_ready'] if bad else [],'expected_auxiliary_contexts':26,'contexts':{k:{} for k in contexts}}
    rows.append({'utc_ns':start_utc+t,'boottime_ns':start_bt+t,'scheduled_monitor_boottime_ns':start_bt+t,'monitor_slot':slot,'observation_role':'nominal_monitor','skipped_monitor_slots':0,'config_sha256':config,'auxiliary_gate':gate,'core_ready':not bad})
probe={'observation_role':'recovery_probe'}
def rate(rs):return new.evaluate_fixed_slots([(json.dumps(r,sort_keys=True)+'\n').encode() for r in rs],start_ns=start_utc,end_ns=start_utc+86400*10**9,start_boottime_ns=start_bt,config_sha256=config,expected_contexts=contexts)
z=rate(rows+[probe]*5);assert (z['passed'],z['failed'],z['total'],z['outside_nominal'],z['result'])==(2875,5,2880,5,'PASS')
cases.append({'case':'fake_own86400_complete2880_rotation_faults_remain_five_bad_probes_excluded','PASS':True})
missing=rate(rows[:-1]);assert missing['failed']==6 and missing['missing_slots']==[2879]
cases.append({'case':'missing_nominal_stays_bad','PASS':True})
late=copy.deepcopy(rows);late[1]['boottime_ns']+=30000000000;late[1]['utc_ns']+=30000000000;late[1]['skipped_monitor_slots']=1;late.pop(2)
l=rate(late);assert any('late_monitor_observation' in r['reasons'] for r in l['failed_checks']) and 2 in l['missing_slots']
cases.append({'case':'late_and_missing_both_stay_bad','PASS':True})
for slot in range(15):rows[slot]['auxiliary_gate'].update(result='BLOCK',reasons=['core_not_ready'])
assert rate(rows)['result']=='BLOCK'
cases.append({'case':'more_than_fourteen_bad_cannot_pass_995','PASS':True})
result={'result':'LOCAL_CLASSIFIER_AND_FIXED_MONITOR_REGRESSION_PASS','case_count':len(cases),'cases':cases,'actual_trigger_reproduced_in_original':True,'all_other_policy_AST_unchanged':True,'candidate_sha256':hashlib.sha256((Q/'qualification_owner_995.py').read_bytes()).hexdigest(),'real_linux_credit':False,'production_or_raw_actions':False,'formal_duration_credit_seconds':0,'synthetic_86400_seconds_only':True}
(Q/'parent-tests.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='cases'}))
