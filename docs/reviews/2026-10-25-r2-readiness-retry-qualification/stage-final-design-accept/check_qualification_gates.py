from pathlib import Path
import ast,base64,copy,hashlib,json,tempfile
R=Path(__file__).resolve().parent;I=R/'inputs';tree=ast.parse((I/'stage-operator.py').read_bytes())
g={'__file__':str(I/'stage-operator.py'),'Path':Path,'base64':base64,'hashlib':hashlib,'json':json};exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.Assign)) and not(isinstance(n,ast.FunctionDef) and n.name=='main')],type_ignores=[]),'<isolated-qualification-gate-functions>','exec'),g)
m,runtime=g['load_design']();original=json.loads((I/'stage-qualification.json').read_bytes());cases=[]
with tempfile.TemporaryDirectory(prefix='review-local-synthetic-qualification-',dir='/private/var/tmp') as td:
 T=Path(td);counter=0
 def artifact(body):
  global counter
  counter+=1;p=T/(str(counter)+'.json');b=(json.dumps(body,sort_keys=True)+'\n').encode() if not isinstance(body,bytes) else body;p.write_bytes(b);return {'path':str(p),'sha256':hashlib.sha256(b).hexdigest()}
 design_result={'decision':'ACCEPT','bindings':{'operator_sha256':original['operator_sha256'],'controls_manifest_sha256':original['manifest_sha256'],'runtime_members':m['members'],'scope':m['scope'],'source':m['source'],'identity_sha256':m['actual_identity_sha256']}}
 binding=copy.deepcopy(original);binding['state']='DESIGN_ACCEPTED_COPY_ONLY';binding['design']={n:artifact(design_result if n=='result' else b'SYNTHETIC_REVIEW_FIXTURE_ONLY') for n in ['report','result','manifest']};binding['preflight_acceptance']={n:artifact({'decision':'ACCEPT','accepted_preflight_sha256':binding['current_preflight']['sha256']} if n=='result' else b'SYNTHETIC_REVIEW_FIXTURE_ONLY') for n in ['report','result','manifest']}
 path=T/'qualification.json';g['BINDING']=path
 def expect(name,binding,mode,ok):
  path.write_text(json.dumps(binding));error=None
  try:value=g['approved_payload'](m,runtime,mode)
  except (AssertionError,KeyError,ValueError,TypeError) as caught:error=type(caught).__name__;value=None
  assert (error is None)==ok,(name,error)
  cases.append({'case':name,'expected_gate_acceptance':ok,'PASS':True,'error':error});return value
 payload=expect('design_acceptance_permits_only_copy_payload_preparation',binding,'copy',True)
 expect('copy_only_binding_still_refuses_ARM',binding,'arm',False);expect('copy_only_binding_still_refuses_launch',binding,'launch',False)
 linux={'result':'GENUINE_LINUX_CLASSIFIER39_PASS','platform':'linux','candidate_sha256':m['members']['qualification_owner_995.py'],'fixture_result_sha256':'4b5ad6d4c8ae395f7520e54d2d48dbb59192dcc9de7228b15a5f7b8dfe3bcd81','case_count':39,'returncode':0}
 smoke={'result':'CURRENT_LINUX_CLEAN_IMPORT_AND_BINDING_SMOKE_PASS','members':m['members'],'imports':{str(i):str(i) for i in range(12)},'source':m['source'],'identity_sha256':m['actual_identity_sha256'],'historical_n1_source':'c3686dd76406c8d483b23eb10417349e5a1ffecd','inherited_duration_ns':0,'formal_credit_seconds':0}
 copied={'result':'EXCLUSIVE_N2_CONTROLS_COPIED_NOT_ARMED','scope':m['scope'],'census':{'counts':m['expected_stopped_counts']},'physical':{n:{'sha256':hashlib.sha256(b).hexdigest()} for n,b in payload.items()},'linux_binding_smoke':smoke,'linux_classifier_qualification':linux}
 def with_actual(copied):
  z=copy.deepcopy(binding);z['state']='CURRENT_LINUX_PHYSICAL_INDEPENDENTLY_ACCEPTED';cp=artifact(copied);accepted={'decision':'ACCEPT','accepted_copy_sha256':cp['sha256'],'controls_manifest_sha256':original['manifest_sha256']};z['actual']={'copy':cp,'independent_result':artifact(accepted),'independent_report':artifact(b'SYNTHETIC_REVIEW_FIXTURE_ONLY'),'independent_manifest':artifact(b'SYNTHETIC_REVIEW_FIXTURE_ONLY'),'accepted_copy_sha256':cp['sha256']};return z
 good=with_actual(copied);expect('complete_synthetic_binding_allows_ARM_payload_only',good,'arm',True);expect('complete_synthetic_binding_allows_launch_payload_only',good,'launch',True)
 for field,bad in [('result','FAILED_CLASSIFIER_LINUX_QUALIFICATION'),('platform','darwin'),('candidate_sha256','0'*64),('fixture_result_sha256','0'*64),('case_count',38),('returncode',1)]:
  cp=copy.deepcopy(copied);cp['linux_classifier_qualification'][field]=bad;expect('reject_linux_'+field,with_actual(cp),'arm',False)
 for field,bad in [('result','FAILED'),('imports',{}),('source','oldsource'),('identity_sha256','oldidentity'),('inherited_duration_ns',1),('formal_credit_seconds',1)]:
  cp=copy.deepcopy(copied);cp['linux_binding_smoke'][field]=bad;expect('reject_smoke_'+field,with_actual(cp),'launch',False)
 cp=copy.deepcopy(copied);cp['physical']['controller.py']['sha256']='0'*64;expect('reject_physical_payload_hash',with_actual(cp),'arm',False)
 cp=copy.deepcopy(copied);cp['census']['counts']=[0]*5;expect('reject_copied_counts',with_actual(cp),'arm',False)
 z=copy.deepcopy(good);z['actual']['accepted_copy_sha256']='0'*64;expect('reject_independent_copy_hash_binding',z,'arm',False)
 for field,bad in [('operator_sha256','0'*64),('manifest_sha256','0'*64),('state','UNBOUND_NO_COPY_ARM_LAUNCH')]:
  z=copy.deepcopy(binding);z[field]=bad;expect('reject_binding_'+field,z,'copy',False)
 z=copy.deepcopy(binding);z['preflight_acceptance']['result']=artifact({'decision':'ACCEPT','accepted_preflight_sha256':'0'*64});expect('reject_wrong_preflight_acceptance',z,'copy',False)
 z=copy.deepcopy(binding);d=copy.deepcopy(design_result);d['bindings']['source']='other';z['design']['result']=artifact(d);expect('reject_wrong_design_source',z,'copy',False)
 z=copy.deepcopy(binding);c={'decision':'ACCEPT','bindings':{'candidate_sha256':'0'*64},'provided_fixtures_independently_executed_cases':39};z['classifier_code']['result']=artifact(c);expect('reject_wrong_accepted_classifier',z,'copy',False)
result={'scope':'ISOLATED_SYNTHETIC_DOCUMENT_GATING_ONLY_NOT_ACTUAL_ACCEPTANCE','case_count':len(cases),'cases':cases,'actual_Linux_copy_ARM_or_launch_performed':False,'remote_or_helper_main_executed':False,'source_or_classifier_tests_repeated':False,'formal_credit_seconds':0}
(R/'QUALIFICATION_GATE_CASES.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({'case_count':len(cases),'result':'PASS'}))
