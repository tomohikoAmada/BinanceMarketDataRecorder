"""Independent LOCAL metadata scope-race and parser error-custody probes."""
from pathlib import Path
import base64,hashlib,json,os,sys,tempfile
from unittest.mock import patch
R=Path(__file__).resolve().parent;sys.path.insert(0,str(R/'run'))
import control_files as c
cases=[]
with tempfile.TemporaryDirectory(prefix='collection-adversarial-',dir='/private/var/tmp') as td:
 base=Path(td);(base/'trusted/root').mkdir(parents=True);(base/'foreign/root').mkdir(parents=True)
 trusted=base/'trusted';root=trusted/'root';foreign=base/'foreign'
 (root/'evidence.json').write_bytes(b'{"origin":"TRUSTED"}')
 foreign_raw=b'{"origin":"FOREIGN"}';(foreign/'root/evidence.json').write_bytes(foreign_raw)
 orig=Path.resolve;once=[False]
 def resolve_then_swap(self,*a,**kw):
  value=orig(self,*a,**kw)
  if self==root and not once[0]:
   once[0]=True;trusted.rename(base/'saved-trusted');trusted.symlink_to(foreign,target_is_directory=True)
  return value
 with patch.object(Path,'resolve',resolve_then_swap):
  out=c.collect_file(root,'evidence.json',allowed_names={'evidence.json'},max_bytes=1024,timeout_seconds=1)
 assert out['read_complete'] and not out['custody_race'] and out['primary_error'] is None,out
 assert base64.b64decode(out['raw']['base64'])==foreign_raw and out['parsed']=={'origin':'FOREIGN'}
 cases.append({'case':'ancestor_redirect_after_canonical_resolve_before_root_lstat','outcome':'BUG_REPRODUCED_FOREIGN_BYTES_MARKED_STABLE_COMPLETE','scope':'Only two temporary metadata roots; Path.resolve hook schedules actual ancestor rename/symlink before root open','result':out})
 # Root path now contains an ancestor link; use a separate ordinary canonical temporary root.
 parse=base/'parse';parse.mkdir()
 for name,raw in [('deep.json',b'['*1500+b'0'+b']'*1500),('deep.jsonl',b'{"ok":1}\n'+b'['*1500+b'0'+b']'*1500+b'\n')]:
  (parse/name).write_bytes(raw)
  try:
   out=c.collect_file(parse,name,allowed_names={name},max_bytes=10000,timeout_seconds=1)
  except RecursionError as e:
   cases.append({'case':'deep_parse_preserves_result:'+name,'outcome':'BUG_REPRODUCED_EXCEPTION_ESCAPES_AND_COLLECTED_RAW_RESULT_LOST','exception_type':type(e).__name__,'message':str(e),'original_raw':{'base64':base64.b64encode(raw).decode(),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},'result_returned':False})
  else:raise AssertionError('Expected exact candidate recursion fault absent:'+str(out))
(R/'ADVERSARIAL_COLLECTION_RESULTS.json').write_text(json.dumps({'case_count':len(cases),'cases':cases,'all_faults_reproduced':True,'scope':'LOCAL temporary metadata files only; no source/Raw/service/network or Linux qualification'},indent=2,sort_keys=True)+'\n')
print(json.dumps({'independent_collection_defects_reproduced':len(cases),'scope_race':True,'JSON_and_JSONL_error_custody':True}))
