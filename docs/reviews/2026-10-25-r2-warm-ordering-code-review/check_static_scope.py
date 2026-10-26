from pathlib import Path
import ast,hashlib,json
R=Path(__file__).resolve().parent;I=R/'inputs';old=ast.parse((I/'controller-original.py').read_bytes());new=ast.parse((I/'controller.py').read_bytes())
def rest(tree):return ast.dump(ast.Module(body=[n for n in tree.body if not(isinstance(n,ast.FunctionDef) and n.name in {'capture','canonical_readiness_during_warm'})],type_ignores=[]),include_attributes=False)
assert rest(old)==rest(new)
def tail(tree):
 cap=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capture');start=next(i for i,n in enumerate(cap.body) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='next_bt' for x in n.targets));return ast.dump(ast.Module(body=cap.body[start:],type_ignores=[]),include_attributes=False)
assert tail(old)==tail(new)
m=json.loads((I/'DESIGN_MANIFEST.json').read_bytes())
for n,v in m['members'].items():
 p=I/n;b=p.read_bytes();assert len(b)==v['bytes'] and hashlib.sha256(b).hexdigest()==v['sha256']
source=Path('/private/var/tmp/bmdr-r2-retry-warm-failure-review-20261025-cir8iwya/source/service/readiness.py');b=source.read_bytes();text=b.decode();assert 'if claimed_identity != expected_identity:' in text and 'if now - capacity_observed > maximum_age' in text and 'self.catalog_ready(' in text
refs={'path':str(source),'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'published_identity_check_line':next(i for i,x in enumerate(text.splitlines(),1) if 'if claimed_identity != expected_identity:' in x),'capacity_freshness_check_line':next(i for i,x in enumerate(text.splitlines(),1) if 'if now - capacity_observed > maximum_age' in x)}
(R/'STATIC_SCOPE_AND_SOURCE_REFERENCES.json').write_text(json.dumps({'all_other_controller_AST_unchanged':True,'all_capture_post_T0_monitor_duration_audit_restore_AST_unchanged':True,'current_manifest_members_verified':7,'original_controller_sha256':hashlib.sha256((I/'controller-original.py').read_bytes()).hexdigest(),'candidate_sha256':hashlib.sha256((I/'controller.py').read_bytes()).hexdigest(),'source_dynamic_predicate_reference':refs,'source_or_candidate_module_imported_or_executed':False},indent=2)+'\n')
print(json.dumps({'static_scope':'PASS','source_reference_lines':[refs['published_identity_check_line'],refs['capacity_freshness_check_line']]}))
