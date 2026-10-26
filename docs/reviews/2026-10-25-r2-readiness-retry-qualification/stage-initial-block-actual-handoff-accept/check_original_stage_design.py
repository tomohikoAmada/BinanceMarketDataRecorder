from pathlib import Path
import ast,base64,hashlib,json,os,tempfile,types
from unittest.mock import patch
R=Path(__file__).resolve().parent;I=R/'inputs';m=json.loads((I/'stage-controls-manifest.json').read_bytes());b=(I/'stage-operator.py').read_bytes();assert hashlib.sha256(b).hexdigest()=='b4c7e06084c8cb72a2ab0c021bd266f372b80720d62029b7cc4c9c1ebd647b51'
t=ast.parse(b);allowed=[n for n in t.body if isinstance(n,(ast.FunctionDef,ast.Assign)) and not(isinstance(n,ast.FunctionDef) and n.name=='main')]
# Definitions/constants only, with __file__ rebound to the immutable LOCAL copy.
g={'__file__':str(I/'stage-operator.py'),'Path':Path,'base64':base64,'hashlib':hashlib,'json':json};exec(compile(ast.Module(body=allowed,type_ignores=[]),'<isolated-operator-code-generation-and-binding>','exec'),g)
mm,runtime=g['load_design']();assert mm==m
cases=[]
for mode in ['copy','arm','launch']:
 try:g['build_remote'](mode)
 except AssertionError:cases.append({'case':'UNBOUND_rejects_'+mode,'PASS':True})
 else:raise AssertionError('unbound mutation accepted')
# Generate only, never run, the reviewed remote programs with the approval
# boundary replaced by a fixture payload; this adds no operational acceptance.
g['approved_payload']=lambda m,runtime,mode:runtime
programs={mode:g['build_remote'](mode) for mode in ['copy','arm','launch','read']}
for mode,program in programs.items():
 compile(program,'<compile-only-'+mode+'>','exec');(R/('generated-'+mode+'-original.py')).write_text(program);cases.append({'case':'compile_generated_'+mode,'PASS':True})
assert 'RuntimeMaxSec=89280' not in programs['launch'] # installed launch.py owns its already reviewed native argv
assert "assert not list((c.D/'data/active').rglob('*.partial'))" in programs['copy'] and "assert not list((c.D/'data/active').rglob('*.partial'))" in programs['arm'] and "assert not list((c.D/'data/active').rglob('*.partial'))" in programs['launch']
with tempfile.TemporaryDirectory(prefix='review-local-stage-scan-',dir='/private/var/tmp') as td:
 active=Path(td)/'active';active.mkdir();(active/'retained.partial').touch()
 assert list(active.rglob('*.partial'))
 with patch('os.scandir',side_effect=PermissionError('synthetic incomplete scan')):assert not list(active.rglob('*.partial'))
 cases.append({'case':'fresh_rglob_gate_silently_accepts_scan_error','PASS':True,'reproduces_blocker':True})
assert "'linux-classifier-qa.json'" not in programs['read'] and "'parent-tests.json'" not in programs['read'];cases.append({'case':'reader_omits_pre_copy_completion_QA_controls','PASS':True,'reproduces_blocker':True})
for name in ['copy','arm','launch']:
 assert str(m['scope']) in programs[name] and 'c.stopped()' in programs[name]
 assert "'stage-launch-attempt.json'" in programs['launch'] and "'arm-native-attempt.json'" in programs['arm']
copytree=ast.parse(programs['copy']);publishlines=[n.lineno for n in ast.walk(copytree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='publish' and n.args and isinstance(n.args[0],ast.Constant) and n.args[0].value=='linux-classifier-qa.json'];assert len(publishlines)==1
qaassert=next(n.lineno for n in copytree.body if isinstance(n,ast.Assert) and 'GENUINE_LINUX_CLASSIFIER39_PASS' in ast.unparse(n));assert publishlines[0]<qaassert
assert "timeout=120" in programs['copy'] and "4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81" in programs['copy'] and "fixture_result_raw" in programs['copy'] and "stdout_raw" in programs['copy'] and "stderr_raw" in programs['copy']
assert '--property=Type=oneshot' in programs['arm'] and '--property=TimeoutStartSec=2400' in programs['arm'] and '--property=TimeoutStopSec=1500' in programs['arm'] and '--property=UMask=0027' in programs['arm'] and '/restore.py capture-failure' in programs['arm']
old=json.loads(Path('/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/capacity-n2-stage-controls-manifest.json').read_bytes())['members'];changed={n for n,h in m['members'].items() if old[n]!=h};assert changed=={'common.py','capacity-policy.json','qualification_owner_995.py'}
previous=Path('/private/var/tmp/bmdr-r2-n2-instrumented-20261024-4r2aauoj/runtime/common.py')
oldtree=ast.parse(previous.read_bytes());newtree=ast.parse((I/'runtime/common.py').read_bytes());f=lambda x:{n.name:ast.dump(n,include_attributes=False) for n in x.body if isinstance(n,ast.FunctionDef)};assert f(oldtree)==f(newtree)
assert m['members']['qualification_owner_995.py']=='621063b04ba21103206282519fa4b14731e599118a05f5a132c11b72abf3cdd2'
result={'decision':'BLOCK','operator_sha256':hashlib.sha256(b).hexdigest(),'controls_manifest_sha256':hashlib.sha256((I/'stage-controls-manifest.json').read_bytes()).hexdigest(),'cases':cases,'case_count':len(cases),'all_other_11_historical_runtime_bytes_identical':True,'common_all_function_AST_unchanged_from_previous_instrumented':True,'classifier_is_prior_accepted_exact_bytes':True,'linux39_result_is_persisted_before_pass_assertion':True,'remote_programs_compile_only':True,'helper_main_or_remote_program_executed':False,'network_service_or_payload_action':False,'formal_credit_seconds':0,'findings':[{'id':'S1','priority':1,'description':'Fresh copy/ARM/launch rglob partial check suppresses scan errors and lacks trusted ancestor/entry validation; cannot prove promised fresh complete custody.'},{'id':'S2','priority':2,'description':'Read program omits linux-classifier-qa.json and parent-tests.json, so a failed copy before software-copy publication cannot export its newly persisted QA failure/partial-result bytes using this reader.'}]}
(R/'ORIGINAL_STAGE_CHECKS.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({'case_count':len(cases),'decision':'BLOCK','findings':2}))
