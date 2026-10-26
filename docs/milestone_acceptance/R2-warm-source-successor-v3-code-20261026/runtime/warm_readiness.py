"""Private bounded warm orchestration; no service actions on import or in the child.

Conditional static reuse is deliberately distinct from stock fresh full readiness.
"""
from common import *
import base64
import importlib
import signal
import stat
import sys

KIND = 'WARM_COMPLETION_BOUNDED_STATIC_AND_FRESH_SOURCE_DYNAMIC_V1'
FULL_KIND = 'WARM_EARLIER_FULL_CANONICAL_ATTESTATION_V1'
CLEANUP_SECONDS = 2.0
SOURCE_MODULE_HASHES = {'service/readiness.py': '7f53a042b454f38d7269da4ece711219573d52d745bad40234b042381acaea38', 'service/deployment_identity.py': '0decd8e6cb745b04e69f103b133248b15cd12a5fa6cd729d85b2de54db8744b7', 'service/systemd.py': '82c6d8d9195e4698f5dfb029dfc8f8db66a87fa8fb4c54f447176779f794a3a7', 'service/state.py': '49a575f7d3abca370b971abf6cb5814b05973635e8debd3add44d9ec37a60066', 'cli.py': 'b3ede1e1c7b59e8e7a6eed49e0bf4654a85b1b940adf56c1d817f43ff4d9c913', 'config.py': '7e5eda9773120ad83e9154116664eda07827c51891b6675fce82a235c228f506'}


def require(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def clocks():
    return {'utc_ns': time.time_ns(), 'boottime_ns': bt(),
            'monotonic_ns': time.monotonic_ns()}


def clock_guard(budget):
    require(budget['kind'] == 'WARM_FIXED_1200S_BUDGET_V1'
            and budget['deadline_monotonic_ns'] == budget['begin']['monotonic_ns'] + 1200 * 10**9
            and budget['deadline_boottime_ns'] == budget['begin']['boottime_ns'] + 1200 * 10**9,
            'warm_budget_extension_or_invalid_kind')
    now = clocks()
    require(boot() == budget['boot_id'], 'warm_boot_changed')
    for name in now:
        require(now[name] >= budget['begin'][name], 'warm_clock_regressed:' + name)
    du = now['utc_ns'] - budget['begin']['utc_ns']
    db = now['boottime_ns'] - budget['begin']['boottime_ns']
    dm = now['monotonic_ns'] - budget['begin']['monotonic_ns']
    require(abs(du - db) < 2 * 10**9 and abs(dm - db) < 2 * 10**9,
            'warm_clock_discontinuity')
    require(now['monotonic_ns'] < budget['deadline_monotonic_ns']
            and now['boottime_ns'] < budget['deadline_boottime_ns'],
            'warm_original_1200s_deadline_exceeded')
    return now


def remaining(budget):
    now = clock_guard(budget)
    return min(budget['deadline_monotonic_ns'] - now['monotonic_ns'],
               budget['deadline_boottime_ns'] - now['boottime_ns']) / 10**9


def file_identity(path):
    require(path.resolve(strict=True) == path and not path.is_symlink(),
            'warm_noncanonical_file:' + str(path))
    s = path.stat()
    require(stat.S_ISREG(s.st_mode), 'warm_not_regular_file:' + str(path))
    return {'path': str(path), 'device': s.st_dev, 'inode': s.st_ino,
            'type': stat.S_IFMT(s.st_mode), 'uid': s.st_uid, 'gid': s.st_gid,
            'mode': stat.S_IMODE(s.st_mode)}


def protected_read(name, digest=None):
    path = Q / name
    require(path.parent == Q and Q.resolve(strict=True) == Q,
            'warm_evidence_scope_invalid')
    require(Q.stat().st_uid == 0 and not Q.stat().st_mode & 0o022,
            'warm_evidence_directory_untrusted')
    info = file_identity(path)
    require(info['uid'] == 0 and info['mode'] == 0o640,
            'warm_evidence_custody_invalid')
    raw = path.read_bytes()
    if digest is not None:
        require(hashlib.sha256(raw).hexdigest() == digest,
                'warm_evidence_digest_mismatch:' + name)
    return json.loads(raw)


def origin_guard():
    import binance_market_data_recorder as package
    root = Path(package.__file__).parent
    expected_venv = Path(PYTHON).parent.parent
    require(root.is_relative_to(expected_venv) and str(Path(sys.executable)) == PYTHON,
            'warm_noncanonical_python_or_package')
    result = {}
    for relative, digest in SOURCE_MODULE_HASHES.items():
        path = root / relative
        file_identity(path)
        require(sha(path) == digest, 'warm_source_module_changed:' + relative)
        module_name = 'binance_market_data_recorder.' + relative[:-3].replace('/', '.')
        module = importlib.import_module(module_name)
        require(Path(module.__file__) == path, 'warm_source_origin_changed:' + relative)
        result[relative] = {'path': str(path), 'sha256': digest}
    return result


def snapshot(budget, binding, instance):
    clock_guard(budget)
    require(budget['scope'] == str(Q) and budget['source_git_sha'] == SOURCE
            and budget['identity_sha256'] == IDENTITY
            and budget['config_sha256'] == CONFIG_SHA,
            'warm_scope_or_expected_identity_changed')
    require(os.environ.get('INVOCATION_ID') == budget['native_capture_invocation']
            and budget['native_capture_invocation'], 'warm_native_changed')
    require(sha(CONFIG) == CONFIG_SHA and sha(DEPLOYMENT) == IDENTITY,
            'warm_deployment_changed')
    r = state(REC)
    runtime = {n: r[n] for n in ('InvocationID', 'MainPID', 'NRestarts', 'ActiveState')}
    require(runtime == {'InvocationID': binding['invocation'], 'MainPID': binding['pid'],
                        'NRestarts': '0', 'ActiveState': 'active'},
            'warm_recorder_incarnation_changed')
    publication = (D / 'state/service_state.json').read_bytes()
    require(len(publication) <= 1024*1024, 'warm_live_publication_oversize')
    s = json.loads(publication)
    require(s.get('pid') == int(binding['pid']) and s.get('status') == 'RUNNING'
            and s.get('service_instance_id') == instance and instance,
            'warm_service_instance_changed')
    return {'clock': clocks(), 'scope': str(Q), 'boot_id': boot(),
            'native_capture_invocation': budget['native_capture_invocation'],
            'config_sha256': sha(CONFIG), 'identity_sha256': sha(DEPLOYMENT),
            'runtime': runtime, 'service_instance_id': instance,
            'publication_sha256': hashlib.sha256(publication).hexdigest(),
            'catalog_identity': file_identity(D / 'state/catalog.sqlite'),
            'source_modules': origin_guard()}


def same_guard(a, b):
    require({k: v for k, v in a.items() if k not in {'clock', 'publication_sha256'}} ==
            {k: v for k, v in b.items() if k not in {'clock', 'publication_sha256'}},
            'warm_attestation_guard_changed')


def encoded(raw):
    return {'base64': base64.b64encode(raw).decode('ascii'), 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest()}


def decoded(value):
    raw = base64.b64decode(value['base64'], validate=True)
    require(len(raw) == value['bytes'] and hashlib.sha256(raw).hexdigest() == value['sha256'],
            'warm_command_bytes_mismatch')
    return raw


def owned_command(argv, budget, cap):
    from owned_command import run_owned
    available = min(cap, remaining(budget))
    require(available > CLEANUP_SECONDS, 'warm_command_no_cleanup_reserve')
    before = clocks()
    receipt = run_owned(argv, python_executable=PYTHON,
                        timeout=available-CLEANUP_SECONDS, cleanup_seconds=CLEANUP_SECONDS)
    receipt.update(before=before, after=clocks())
    for raw_name, name in [('stdout_raw','stdout'),('stderr_raw','stderr'),
                           ('control_raw','keeper_control'),
                           ('timeout_prefix_stdout_raw','timeout_prefix_stdout'),
                           ('timeout_prefix_stderr_raw','timeout_prefix_stderr')]:
        receipt[name] = encoded(receipt.pop(raw_name))
    try:
        receipt['parsed_stdout'] = json.loads(decoded(receipt['stdout']))
        receipt['parse_error'] = None
    except (ValueError, UnicodeError) as exc:
        receipt['parsed_stdout'] = None
        receipt['parse_error'] = type(exc).__name__ + ':' + str(exc)
    return receipt


def command_ok(receipt):
    require(receipt.get('timed_out') is False and receipt.get('cleanup_error') is None
            and type(receipt.get('unexpected_live_descendants')) is list
            and receipt['unexpected_live_descendants'] == []
            and type(receipt.get('returncode')) is int
            and type(receipt.get('keeper_returncode')) is int
            and receipt['keeper_returncode'] == 0
            and receipt.get('lease_released') is True
            and receipt.get('group_KILL_used') is False
            and receipt.get('cleanup') == 'LEASED_GROUP_NO_LIVE_MEMBERS'
            and receipt['returncode'] == 0 and receipt['parse_error'] is None,
            'warm_command_failed_or_invalid')
    control = json.loads(decoded(receipt['keeper_control']))
    require(all(type(receipt.get(n)) is int and receipt[n] > 0
                for n in ['child_pid','owned_pgid','inner_pid'])
            and all(type(control.get(n)) is int and control[n] > 0
                    for n in ['keeper_pid','pgid','inner_pid']),
            'warm_keeper_process_ids_invalid')
    require(type(control.get('returncode')) is int and control['returncode'] == 0
            and control.get('keeper_pid') == receipt['child_pid'] == receipt['owned_pgid']
            and control.get('pgid') == receipt['owned_pgid']
            and control.get('inner_pid') == receipt['inner_pid'],
            'warm_keeper_success_protocol_mismatch')


def command_interval(receipt):
    before, after = receipt['before'], receipt['after']
    for name in ('utc_ns','boottime_ns','monotonic_ns'):
        require(type(before.get(name)) is int and type(after.get(name)) is int
                and after[name] >= before[name], 'warm_command_clock_order:' + name)
    du = after['utc_ns'] - before['utc_ns']
    db = after['boottime_ns'] - before['boottime_ns']
    dm = after['monotonic_ns'] - before['monotonic_ns']
    require(abs(du-db) < 2*10**9 and abs(dm-db) < 2*10**9,
            'warm_command_clock_interval_disagreement')


def full_result(receipt, binding, instance):
    require(receipt['argv'] == CLI + ['deployment', 'readiness'], 'warm_full_argv_changed')
    command_ok(receipt)
    command_interval(receipt)
    ready = json.loads(decoded(receipt['stdout']))
    require(ready == receipt['parsed_stdout'], 'warm_parsed_stdout_changed')
    require(ready.get('command') == 'deployment.readiness'
            and ready.get('schema_version') == 'deployment-readiness.v1'
            and ready.get('state') == 'READY' and ready.get('reasons') == [],
            'warm_full_canonical_not_READY')
    ev = ready['evidence']; identity = ev['identity']; runtime = ev['systemd_runtime']
    expected = read(DEPLOYMENT)
    require(all(identity[n] == expected[n] for n in ('source_git_sha', 'wheel_sha256',
        'dependency_lock_sha256', 'config_sha256', 'systemd_unit_sha256', 'capacity_profile_id')),
        'warm_full_release_fields_mismatch')
    require(identity['identity_sha256'] == IDENTITY and identity['source_git_sha'] == SOURCE
            and identity['config_sha256'] == CONFIG_SHA, 'warm_full_identity_mismatch')
    artifact = identity['installed_artifact']; dependencies = identity['installed_dependencies']
    require(type(artifact.get('record_verified_files')) is int
            and artifact['record_verified_files'] > 0
            and dependencies.get('exact_match') is True
            and dependencies.get('editable') is False
            and dependencies.get('recorder_distribution_separate') is True,
            'warm_contradictory_static_attestation')
    for n in ['locked_distribution_count','installed_distribution_count',
              'protected_file_count','protected_directory_count']:
        require(type(dependencies.get(n)) is int and dependencies[n] > 0,
                'warm_static_positive_nonboolean_count:' + n)
    locked = dependencies.get('locked_distributions')
    require(isinstance(locked,dict) and len(locked) == dependencies['locked_distribution_count']
            == dependencies['installed_distribution_count']
            and all(type(k) is str and k and type(v) is str and v for k,v in locked.items()),
            'warm_static_distribution_inventory_mismatch')
    venv = Path(PYTHON).parent.parent
    require(artifact.get('venv_root') == str(venv)
            and artifact.get('package_version') == expected['package_version'],
            'warm_static_artifact_version_or_venv_mismatch')
    for n in ['module_path','dist_info_path','direct_url_path']:
        require(type(artifact.get(n)) is str and Path(artifact[n]).is_absolute()
                and Path(artifact[n]).is_relative_to(venv)
                and '..' not in Path(artifact[n]).parts, 'warm_static_artifact_path_invalid:' + n)
    require(Path(artifact['module_path']).name == '__init__.py'
            and Path(artifact['module_path']).parent.name == 'binance_market_data_recorder'
            and Path(artifact['direct_url_path']).name == 'direct_url.json'
            and Path(artifact['direct_url_path']).parent == Path(artifact['dist_info_path'])
            and Path(artifact['dist_info_path']).name.endswith('.dist-info'),
            'warm_static_artifact_paths_inconsistent')
    require(identity['installed_artifact']['record_verified_files'] > 0
            and identity['installed_artifact']['editable'] is False
            and identity['installed_dependencies']
            and identity['venv_control_chain']['symlink_free'] is True
            and identity['venv_control_chain']['service_writable'] is False
            and identity['identity_permissions']['mode'] == '0640',
            'warm_full_static_evidence_incomplete')
    require(runtime['main_pid'] == int(binding['pid']) and runtime['active_state'] == 'active'
            and ev['service_state']['service_instance_id'] == instance
            and ev['service_state']['pid'] == int(binding['pid']),
            'warm_full_runtime_mismatch')
    return ready


def load_attestation(budget_digest, attestation_digest):
    budget = protected_read('warm-budget.json', budget_digest)
    binding = protected_read('warm-start-binding.json')
    first = protected_read('warm-first-strict.json')
    att = protected_read('warm-readiness-check.json', attestation_digest)
    require(binding['budget_sha256'] == budget_digest, 'warm_binding_budget_mismatch')
    require(att['kind'] == FULL_KIND and att['budget_sha256'] == budget_digest
            and att['binding_sha256'] == sha(Q / 'warm-start-binding.json')
            and att['first_strict_sha256'] == sha(Q / 'warm-first-strict.json'),
            'warm_attestation_reference_mismatch')
    first_raw = base64.b64decode(first['state_publication_base64'], validate=True)
    require(len(first_raw) == first['state_publication_bytes']
            and hashlib.sha256(first_raw).hexdigest() == first['state_sha256'],
            'warm_first_raw_mismatch')
    first_pub = json.loads(first_raw)
    require(first['boot_id'] == budget['boot_id']
            and first['config_sha256'] == CONFIG_SHA
            and first['boottime_ns'] >= budget['begin']['boottime_ns']
            and first['strict_auxiliary_gate']['result'] == 'PASS'
            and first_pub['service_instance_id'] == first['recorder_service_instance_id'],
            'warm_first_witness_invalid')
    require(att['command']['before']['boottime_ns'] >= first['boottime_ns']
            and att['command']['before']['monotonic_ns'] >= budget['begin']['monotonic_ns']
            and att['command']['after']['monotonic_ns'] < budget['deadline_monotonic_ns']
            and att['command']['after']['boottime_ns'] < budget['deadline_boottime_ns'],
            'warm_attestation_outside_original_deadline')
    command_interval(att['command'])
    for name in ['utc_ns','boottime_ns','monotonic_ns']:
        require(budget['begin'][name] <= att['before']['clock'][name]
                <= att['command']['before'][name] <= att['command']['after'][name]
                <= att['after']['clock'][name], 'warm_attestation_clock_binding:' + name)
    same_guard(att['before'], att['after'])
    ready = full_result(att['command'], binding, first['recorder_service_instance_id'])
    current = snapshot(budget, binding, first['recorder_service_instance_id'])
    same_guard(att['after'], current)
    return budget, binding, first, att, ready, current


class WarmStart:
    def __init__(self):
        begin = clocks()
        self.budget = {'kind': 'WARM_FIXED_1200S_BUDGET_V1', 'scope': str(Q),
                       'begin': begin, 'boot_id': boot(), 'source_git_sha': SOURCE,
                       'identity_sha256': IDENTITY, 'config_sha256': CONFIG_SHA,
                       'native_capture_invocation': os.environ.get('INVOCATION_ID'),
                       'deadline_monotonic_ns': begin['monotonic_ns'] + 1200 * 10**9,
                       'deadline_boottime_ns': begin['boottime_ns'] + 1200 * 10**9}
        publish('warm-budget.json', self.budget)
        self.attestation = None
        self.binding = None

    def bind(self, invocation, pid, request):
        self.binding = {'invocation': invocation, 'pid': pid, 'enable_request_utc_ns': request,
                        'budget_sha256': sha(Q / 'warm-budget.json')}
        publish('warm-start-binding.json', self.binding)

    def check(self):
        return clock_guard(self.budget)

    def early(self, first):
        publish('warm-first-strict.json', dict(first))
        instance = first['recorder_service_instance_id']
        before = snapshot(self.budget, self.binding, instance)
        command = owned_command(CLI + ['deployment', 'readiness'], self.budget, 330)
        # Preserve command evidence before validation, including malformed/partial stdout.
        receipt = {'kind': FULL_KIND, 'budget_sha256': sha(Q / 'warm-budget.json'),
                   'binding_sha256': sha(Q / 'warm-start-binding.json'),
                   'first_strict_sha256': sha(Q / 'warm-first-strict.json'),
                   'before': before, 'command': command, 'after': None,
                   'scope_note': 'EARLIER full installed/Catalog attestation; not T0 freshness'}
        try:
            command_ok(command)
            receipt['after'] = snapshot(self.budget, self.binding, instance)
            same_guard(before, receipt['after'])
            full_result(command, self.binding, instance)
        except BaseException as original:
            receipt['failure'] = type(original).__name__ + ':' + str(original)
            try:
                publish('warm-readiness-check.json', receipt)
            except Exception as secondary:
                print('WARM_EVIDENCE_WRITE_FAILED ' + repr(secondary) + '; original=' + repr(original),
                      file=sys.stderr, flush=True)
            raise
        publish('warm-readiness-check.json', receipt)
        self.check()
        self.attestation = receipt
        return receipt['command']['parsed_stdout']

    def sleep(self, first, samples):
        wait = min(30.0, remaining(self.budget))
        if first is not None and samples >= 12:
            wait = min(wait, max(0, first['boottime_ns'] + 300 * 10**9 - bt()) / 10**9)
        time.sleep(wait)
        self.check()

    def completion(self, phase, config, invocation, pid, sample_fn, validate_fn, initial_swap, request):
        limit = min(self.budget['deadline_monotonic_ns'], time.monotonic_ns() + 30 * 10**9)
        for attempt in range(1, 4):
            self.check()
            row = sample_fn(config, invocation, pid, True)
            row['observation_role'] = 'warm_completion_probe'
            validate_fn(row, initial_swap, request, strict=True)
            witness = f'warm-completion-{phase}-{attempt}-state.json'
            publish(witness, dict(row))
            cap = (limit - time.monotonic_ns()) / 10**9
            require(cap > CLEANUP_SECONDS, 'warm_pair_budget_exhausted')
            argv = ['/usr/sbin/runuser', '-u', 'bmdr', '--', PYTHON, '-B',
                    str(Q / 'warm_completion.py'), phase, sha(Q / 'warm-budget.json'),
                    sha(Q / 'warm-readiness-check.json'), witness, sha(Q / witness)]
            command = owned_command(argv, self.budget, cap)
            receipt_name = f'warm-completion-{phase}-{attempt}.json'
            publish(receipt_name, {'kind': KIND, 'phase': phase, 'attempt': attempt,
                                   'witness': witness, 'witness_sha256': sha(Q / witness),
                                   'command': command})
            command_ok(command)
            result = command['parsed_stdout']
            require(result['kind'] == KIND and result['phase'] == phase
                    and result['witness_sha256'] == sha(Q / witness)
                    and result['attestation_sha256'] == sha(Q / 'warm-readiness-check.json'),
                    'warm_child_protocol_mismatch')
            require(result['source_result']['state'] == 'READY', 'warm_source_current_not_READY')
            self.check()
            require(time.monotonic_ns() < limit, 'warm_current_pair_30s_deadline_exceeded')
            if not result['pair_match']:
                continue
            self.fresh(row, config, result)
            publish(f'warm-completion-{phase}.json', {'kind': KIND, 'phase': phase,
                    'selected_receipt': receipt_name, 'selected_receipt_sha256': sha(Q / receipt_name),
                    'witness': witness, 'witness_sha256': sha(Q / witness),
                    'source_result_kind': 'EXACT_SOURCE_WITH_DECLARED_EARLIER_STATIC_SUBPROOFS',
                    'static_attestation_sha256': sha(Q / 'warm-readiness-check.json')})
            self.fresh(row, config, result)
            return row, result
        raise RuntimeError('warm_publication_pair_never_settled')

    def fresh(self, row, config, result):
        from qualification_auxiliary_gate import check
        from qualification_owner_995 import check_write_safety
        self.check()
        pub = json.loads(base64.b64decode(row['state_publication_base64'], validate=True))
        require(pub == result['source_result']['evidence']['service_state'],
                'warm_strict_source_witness_mismatch')
        require(check(pub, config, now_ns=time.time_ns(), allow_empty_recovery=False)['result'] == 'PASS',
                'warm_decision_strict_evidence_stale')
        check_write_safety(pub)
        now = time.time_ns()
        maximum = int(max(30, pub['heartbeat_interval_seconds'] * 3) * 10**9)
        require(-5 * 10**9 <= now - pub['heartbeat_at_utc_ns'] <= maximum
                and -5 * 10**9 <= now - pub['capacity']['observed_at_utc_ns'] <= maximum,
                'warm_decision_source_evidence_stale')
        current = snapshot(self.budget, self.binding, row['recorder_service_instance_id'])
        same_guard(self.attestation['after'], current)
        require(current['publication_sha256'] == row['state_sha256'],
                'warm_live_publication_advanced_after_pair')
        # Module/custody guards also consume time under CPU contention. Recheck
        # the accepted witnesses at the actual decision clock after those reads.
        self.check()
        now = time.time_ns()
        require(check(pub, config, now_ns=now, allow_empty_recovery=False)['result'] == 'PASS'
                and -5*10**9 <= now-pub['heartbeat_at_utc_ns'] <= maximum
                and -5*10**9 <= now-pub['capacity']['observed_at_utc_ns'] <= maximum,
                'warm_decision_evidence_expired_during_guard')
