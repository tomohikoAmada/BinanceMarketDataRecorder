from pathlib import Path
import base64,hashlib,json,tempfile
from unittest.mock import patch
import control_files as c
R=Path(__file__).resolve().parent;CASES=[]
def record(name,r,raw=None):
 if raw is not None:assert base64.b64decode(r['raw']['base64'])==raw and r['raw']['bytes']==len(raw) and r['raw']['sha256']==hashlib.sha256(raw).hexdigest()
 json.dumps(r,allow_nan=False)
 CASES.append({'case':name,'PASS':True,'result':r})
with tempfile.TemporaryDirectory(prefix='reader-ancestor-regression-',dir='/private/var/tmp') as td:
 base=Path(td);ancestor=base/'ancestor';root=ancestor/'root';root.mkdir(parents=True);(root/'control.json').write_text('{"ORIGINAL":1}')
 foreign=base/'foreign';(foreign/'root').mkdir(parents=True);(foreign/'root/control.json').write_text('{"FOREIGN":1}')
 original=Path.resolve;once=[False]
 def replace_after_resolve(self,*a,**kw):
  value=original(self,*a,**kw)
  if self==root and not once[0]:
   once[0]=True;ancestor.rename(base/'kept-original');ancestor.symlink_to(foreign,target_is_directory=True)
  return value
 with patch.object(Path,'resolve',replace_after_resolve):r=c.collect_file(root,'control.json',allowed_names={'control.json'},max_bytes=1024,timeout_seconds=1)
 assert once[0] and r['raw'] is None and not r['read_complete'] and r['primary_error'];record('absolute ancestor replacement after resolve cannot read FOREIGN bytes',r)
with tempfile.TemporaryDirectory(prefix='reader-parser-resource-regression-',dir='/private/var/tmp') as td:
 root=Path(td);deep=b'['*20000+b'0'+b']'*20000
 for name,raw in [('deep.json',deep),('deep.jsonl',b'{"earlier":1}\n'+deep+b'\n')]:
  (root/name).write_bytes(raw);r=c.collect_file(root,name,allowed_names={name},max_bytes=100000,timeout_seconds=2)
  assert r['parse_errors']
  if name.endswith('.jsonl'):assert r['parsed'][0]['value']=={'earlier':1} and r['parse_errors'][0]['line']==2
  record('parse resource exception is structured and earlier JSONL survives:'+name,r,raw)
 raw=b'['*100+b'0'+b']'*100;(root/'view.json').write_bytes(raw)
 r=c.collect_file(root,'view.json',allowed_names={'view.json'},max_bytes=1024,timeout_seconds=1,max_parse_depth=64)
 assert r['parse_errors'] and 'depth bound' in r['parse_errors'][0]['message'];record('bounded parsed view keeps serializable error plus complete original bytes',r,raw)
with tempfile.TemporaryDirectory(prefix='reader-nonfinite-view-',dir='/private/var/tmp') as td:
 root=Path(td)
 for raw in [b'{"number":NaN}',b'{"number":1e999}']:
  (root/'number.json').write_bytes(raw)
  r=c.collect_file(root,'number.json',allowed_names={'number.json'},max_bytes=1024,timeout_seconds=1)
  assert r['parse_errors'];record('non-finite derived value cannot break strict export serialization',r,raw)
(R/'LOCAL_COLLECTION_REGRESSIONS.json').write_text(json.dumps({'case_count':len(CASES),'cases':CASES,'all_PASS':True,'Linux_or_whole_reader_or_T0_credit':False},indent=2)+'\n')
print(json.dumps({'LOCAL_collection_regressions':len(CASES),'all_PASS':True,'whole_reader_or_Linux_credit':False}))
