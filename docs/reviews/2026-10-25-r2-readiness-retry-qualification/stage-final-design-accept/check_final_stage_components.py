from pathlib import Path
import ast,base64,contextlib,hashlib,io,json,os,stat,tempfile,types
from unittest.mock import patch
R=Path(__file__).resolve().parent;I=R/'inputs';H='f3b7b6bfc159b934408be326e6e157f031c3714b14fe8e09719c50dc06e603a9'
b=(I/'stage-operator.py').read_bytes();assert hashlib.sha256(b).hexdigest()==H;t=ast.parse(b)
g={'__file__':str(I/'stage-operator.py'),'Path':Path,'base64':base64,'hashlib':hashlib,'json':json};exec(compile(ast.Module(body=[n for n in t.body if isinstance(n,(ast.FunctionDef,ast.Assign)) and not(isinstance(n,ast.FunctionDef) and n.name=='main')],type_ignores=[]),'<isolated-definitions>','exec'),g)
m,runtime=g['load_design']();cases=[]
for mode in ['copy','arm','launch']:
 try:g['build_remote'](mode)
 except AssertionError:cases.append({'case':'UNBOUND_rejects_'+mode,'PASS':True})
 else:raise AssertionError('unbound mutation accepted')
g['approved_payload']=lambda m,runtime,mode:runtime
programs={k:g['build_remote'](k) for k in ['copy','arm','launch','read']}
for mode,program in programs.items():
 compile(program,'<compile-only-'+mode+'>','exec');(R/('generated-'+mode+'.py')).write_text(program);cases.append({'case':'compile_only_'+mode,'PASS':True})
invtrees=[next(n for n in ast.parse(program).body if isinstance(n,ast.FunctionDef) and n.name=='fresh_active_inventory') for program in programs.values()]
assert len({ast.dump(n,include_attributes=False) for n in invtrees})==1
invdef=compile(ast.Module(body=[invtrees[0]],type_ignores=[]),'<isolated-fresh-inventory>','exec')
class Clock:
 def __init__(self):self.n=0
 def monotonic(self):self.n+=1;return 31 if self.n>5 else 0
 def time_ns(self):return 1000
for mode in ['empty','nested_empty','partial','regular','scan_error','ancestor_symlink','dir_symlink','file_symlink','fifo','missing_root','expired_entry']:
 with tempfile.TemporaryDirectory(prefix='review-local-zero-inventory-',dir='/private/var/tmp') as td:
  D=Path(td).resolve()/'D';active=D/'data/active';active.mkdir(parents=True)
  if mode=='nested_empty':(active/'nested').mkdir()
  if mode in {'partial','expired_entry'}:(active/'sentinel.partial').touch()
  if mode=='regular':(active/'retained.raw-metadata-only-empty').touch()
  if mode=='ancestor_symlink':
   outside=D.parent/'outside';(outside/'active').mkdir(parents=True);active.rmdir();(D/'data').rmdir();(D/'data').symlink_to(outside,target_is_directory=True)
  if mode=='dir_symlink':(active/'link').symlink_to(D.parent,target_is_directory=True)
  if mode=='file_symlink':(active/'link').symlink_to(D.parent/'missing')
  if mode=='fifo':os.mkfifo(active/'fifo')
  if mode=='missing_root':active.rmdir()
  def mapped(value):return active if str(value)=='/var/lib/binance-market-data-recorder/data/active' else Path(value)
  clock=Clock() if mode=='expired_entry' else types.SimpleNamespace(monotonic=lambda:0,time_ns=lambda:1000)
  env={'Path':mapped,'os':os,'stat':stat,'time':clock,'json':json};exec(invdef,env);out=io.StringIO();error=None;value=None
  try:
   with contextlib.redirect_stdout(out):
    if mode=='scan_error':
     with patch('os.scandir',side_effect=PermissionError('synthetic scan failure')):value=env['fresh_active_inventory']()
    else:value=env['fresh_active_inventory']()
  except BaseException as caught:error=type(caught).__name__
  if mode in {'empty','nested_empty'}:assert error is None and value['walk_complete'] and value['file_count']==value['partial_count']==0
  else:
   assert error is not None;failure=json.loads(out.getvalue());assert failure['result']=='FAILED_FRESH_ZERO_INVENTORY' and failure['startup_allowed'] is False and failure['raw_or_partial_bytes_opened'] is False
   if mode=='expired_entry':assert failure['entries'] and failure['partial_count']==1
  cases.append({'case':'fresh_inventory_'+mode,'PASS':True,'expected_accept':mode in {'empty','nested_empty'},'error':error})
readtree=ast.parse(programs['read']);record=next(n for n in readtree.body if isinstance(n,ast.FunctionDef) and n.name=='record');recordcode=compile(ast.Module(body=[record],type_ignores=[]),'<isolated-reader-record>','exec')
parsebegin=next(i for i,n in enumerate(readtree.body) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='origin_ns' for x in n.targets))
parsecode=compile(ast.Module(body=readtree.body[parsebegin:parsebegin+4],type_ignores=[]),'<isolated-QA-parse-and-missing>','exec')
# The first four nodes include the explicit QA parse loop, before stage-control loops.
assert isinstance(readtree.body[parsebegin+3],ast.For)
for mode in ['both_missing','partial_json','invalid_utf8','valid_json']:
 with tempfile.TemporaryDirectory(prefix='review-local-small-QA-controls-',dir='/private/var/tmp') as td:
  q=Path(td);data=b'{"result":' if mode=='partial_json' else (b'\xffexact\x00' if mode=='invalid_utf8' else b'{"result":"FAILED"}\n')
  if mode!='both_missing':(q/'linux-classifier-qa.json').write_bytes(data);(q/'parent-tests.json').write_bytes(data)
  env={'q':q,'result':{'files':{}},'Path':Path,'stat':stat,'hashlib':hashlib,'base64':base64,'json':json};exec(recordcode,env)
  for f in q.iterdir():env['record'](f)
  exec(parsecode,env)
  z=env['result']
  if mode=='both_missing':assert 'linux-classifier-qa.json' in z['missing_fixed_controls'] and 'parent-tests.json' in z['missing_fixed_controls']
  else:
   for f in ['linux-classifier-qa.json','parent-tests.json']:
    item=z['files'][f];assert base64.b64decode(item['base64'])==data and item['bytes']==len(data) and item['sha256']==hashlib.sha256(data).hexdigest()
    assert (f in z['parse_errors'])==(mode in {'partial_json','invalid_utf8'})
  cases.append({'case':'reader_lossless_'+mode,'PASS':True})
for mode in ['copy','arm','launch']:
 assert 'fresh_inventory=fresh_active_inventory()' in programs[mode] and "c.stopped()['NRestarts']=='0'" in programs[mode]
 assert ".rglob('*.partial')" not in programs[mode]
assert "'linux-classifier-qa.json'" in programs['read'] and "'parent-tests.json'" in programs['read']
result={'decision':'COMPONENT_CASES_PASS','operator_sha256':H,'controls_manifest_sha256':hashlib.sha256((I/'stage-controls-manifest.json').read_bytes()).hexdigest(),'case_count':len(cases),'cases':cases,'generated_programs_compile_only':True,'all_modes_share_identical_fresh_inventory_AST':True,'helper_main_or_remote_program_executed':False,'real_network_service_or_payload_action':False,'formal_credit_seconds':0,'linux_credit':False}
(R/'FINAL_COMPONENT_CHECKS.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({'case_count':len(cases),'result':'PASS'}))
