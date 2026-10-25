from pathlib import Path
import ast,base64,hashlib,json,types
R=Path(__file__).resolve().parent
old=Path('/private/var/tmp/bmdr-r2-retry-preflight-correction-review-20261025-s8m5a0o7/inputs/read_current_capacity.py')
new=R/'inputs/read_current_capacity.py'
def remote(p):
 t=ast.parse(p.read_bytes());return t,next(ast.literal_eval(n.value) for n in t.body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='REMOTE' for x in n.targets))
def without_remote(t):return ast.dump(ast.Module(body=[n for n in t.body if not(isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='REMOTE' for x in n.targets))],type_ignores=[]),include_attributes=False)
o,ot=remote(old);n,nt=remote(new);assert without_remote(o)==without_remote(n)
O=ast.parse(ot);N=ast.parse(nt)
def before_try(tree):
 a=[]
 for node in tree.body:
  if isinstance(node,ast.Try):break
  if isinstance(node,ast.FunctionDef) and node.name in {'raw_bytes','run','state'}:continue
  if isinstance(node,ast.Import):node=ast.Import(names=[x for x in node.names if x.name!='base64'])
  a.append(node)
 return ast.dump(ast.Module(body=a,type_ignores=[]),include_attributes=False)
assert before_try(O)==before_try(N)
otry=next(i for i,v in enumerate(O.body) if isinstance(v,ast.Try));ntry=next(i for i,v in enumerate(N.body) if isinstance(v,ast.Try))
assert ast.dump(ast.Module(body=O.body[otry+1:],type_ignores=[]),include_attributes=False)==ast.dump(ast.Module(body=N.body[ntry+1:],type_ignores=[]),include_attributes=False)
def assignment(tree,name):return next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id==name for x in n.targets))
for name in ['D','A','Q','SOURCE','IDENTITY','CONFIG','DEPLOY','start','end','PIN']:
 assert ast.dump(assignment(O,name),include_attributes=False)==ast.dump(assignment(N,name),include_attributes=False),name
pin=ast.literal_eval(assignment(N,'PIN').value)
assert 'LoadUnit(unit);m.RefUnit(unit)' in pin and 'finally:m.UnrefUnit(unit)' in pin
assert all(x not in pin for x in ['StartUnit','StopUnit','RestartUnit','EnableUnit','DisableUnit','MaskUnit'])
constants={name:ast.literal_eval(assignment(N,name).value) for name in ['SOURCE','IDENTITY','CONFIG','start','end']}
assert constants=={'SOURCE':'20e57f30682e88b4090cc2f0da34aa26fca4de2e','IDENTITY':'9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715','CONFIG':'5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419','start':1792821038109362000,'end':1792907346512229095}
verify=assignment(N,'verify').value
argv=ast.literal_eval(ast.List(elts=[v if not(isinstance(v,ast.Name) and v.id=='PIN') else ast.Constant(value=pin) for v in verify.args[0].elts],ctx=ast.Load()))
assert argv[:3]==['/usr/bin/python3.12','-c',pin]
assert argv[3:]==['/usr/sbin/runuser','-u','bmdr','--','/opt/binance-market-data-recorder/venv/bin/python','-m','binance_market_data_recorder','--config','/etc/binance-market-data-recorder/recorder.toml','deployment','verify']
assert not any(isinstance(v,ast.Call) and isinstance(v.func,ast.Attribute) and v.func.attr in {'mkdir','unlink','rmdir','rename','replace','write_bytes','write_text','touch'} for v in ast.walk(N))
assert 'mode=ro' in nt and "PRAGMA query_only=ON" in nt and "os.O_RDONLY|os.O_NOFOLLOW" in nt and 'fcntl.LOCK_EX|fcntl.LOCK_NB' in nt
compile(nt,'<static-only-remote>','exec')
functions=compile(ast.Module(body=[v for v in N.body if isinstance(v,ast.FunctionDef) and v.name in {'raw_bytes','run','state'}],type_ignores=[]),'<isolated-raw-functions>','exec')
responses=[]
def fake(argv,capture_output,timeout):
 assert capture_output and timeout>0
 return responses.pop(0)
g={'base64':base64,'hashlib':hashlib,'subprocess':types.SimpleNamespace(run=fake)};exec(functions,g)
cases=[]
for value in [None,b'',b'\xff\x00\xfe\r\n',b'valid\r\ntext','字串']:
 raw=g['raw_bytes'](value)
 if value is None:assert raw is None
 else:
  expected=value if isinstance(value,bytes) else value.encode();assert base64.b64decode(raw['base64'])==expected and raw['bytes']==len(expected) and raw['sha256']==hashlib.sha256(expected).hexdigest()
 cases.append({'case':'raw_pack_'+str(len(cases)),'result':'PASS'})
for out,err in [(b'x=y\r\n',b''),(b'\xffstate',b''),(b'x=y',b'\xfestderr')]:
 responses.append(types.SimpleNamespace(returncode=0,stdout=out,stderr=err));v=g['run'](['synthetic'],timeout=1)
 for name,original in [('stdout',out),('stderr',err)]:assert base64.b64decode(v[name+'_raw']['base64'])==original
 if out.startswith(b'\xff'):assert v['stdout'] is None and v['stdout_decode_error']
 if err.startswith(b'\xfe'):assert v['stderr'] is None and v['stderr_decode_error']
 cases.append({'case':'run_original_bytes_'+str(len(cases)),'result':'PASS'})
for out,err in [(b'\xffstate',b''),(b'x=y',b'\xfestderr')]:
 responses.append(types.SimpleNamespace(returncode=0,stdout=out,stderr=err))
 try:g['state']('synthetic',timeout=1)
 except AssertionError:pass
 else:raise AssertionError('invalid UTF8 state accepted')
 cases.append({'case':'state_invalid_utf8_rejected_'+str(len(cases)),'result':'PASS'})
refs={}
for p in [old,R/'inputs/preflight-design-manifest.json',Path('/private/var/tmp/bmdr-r2-retry-preflight-design-review-20261025-n59txw4H/MANIFEST.json'),Path('/private/var/tmp/bmdr-r2-retry-preflight-correction-review-20261025-s8m5a0o7/MANIFEST.json')]:refs[str(p)]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
result={'scope':'STATIC_BINDINGS_AND_ISOLATED_BYTE_SERIALIZATION_ONLY','outer_module_AST_unchanged_since_4f4c':True,'remote_pretry_bindings_unchanged_except_raw_run_state':True,'remote_aftertry_result_AST_unchanged':True,'constants':constants,'pin_sha256':hashlib.sha256(pin.encode()).hexdigest(),'definition_only_verify_argv':argv,'service_lock_concurrency':'exclusive lock retained; deployment verify has no runtime service-lock acquisition, prior static source inspection retained','read_only_scope_preserved':True,'raw_case_count':len(cases),'cases':cases,'references':refs,'helper_main_or_whole_remote_executed':False,'real_subprocess_or_network_called':False,'formal_credit_seconds':0,'linux_credit':False}
(R/'static-and-raw-results.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
print(json.dumps({'raw_case_count':len(cases),'static_binding_and_delta':'PASS'}))
