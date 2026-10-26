import base64
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import command_receipts as c
import owned_command


CASES = []
REAL = []


def check(name, result, predicate):
    assert predicate(result), name
    for item in result['raw'].values():
        if item['available']:
            raw = base64.b64decode(item['bytes_observed']['base64'], validate=True)
            assert len(raw) == item['bytes_observed']['bytes']
            assert hashlib.sha256(raw).hexdigest() == item['bytes_observed']['sha256']
    json.dumps(result, allow_nan=False)
    CASES.append({'name': name, 'PASS': True, 'receipt': result})


def call(argv, runner=owned_command.run_owned, **options):
    return c.collect_command(argv, allowed_argv={tuple(argv)}, runner=runner,
        python_executable=sys.executable, timeout_seconds=options.pop('timeout_seconds', 2),
        cleanup_seconds=2, **options)


def real(name, code, **options):
    result = call([sys.executable, '-c', code], **options)
    REAL.append(result)
    return result


def main():
    good = real('success', 'import sys;sys.stdout.buffer.write(b"{\\"a\\":1}\\n");sys.stderr.buffer.write(b"observed\\xff")', parse_mode='json')
    check('real_success_json_and_raw_stderr', good, lambda x: x['protocol_complete'] and x['parsed_stdout']['value'] == {'a': 1})
    bad_utf8 = real('invalidutf8', 'import sys;sys.stdout.buffer.write(b"\\xff\\xfe")', parse_mode='utf8')
    check('real_utf8_error_raw_retained', bad_utf8, lambda x: x['protocol_complete'] and x['parsed_stdout']['errors'] and x['raw']['stdout_raw']['bytes_observed']['bytes'] == 2)
    jsonl = real('jsonl', 'import sys;sys.stdout.buffer.write(b"{\\"one\\":1}\\nnope\\n{\\"three\\":3}\\n")', parse_mode='jsonl')
    check('real_partial_jsonl_continues', jsonl, lambda x: [i['line'] for i in x['parsed_stdout']['value']] == [1, 3] and x['parsed_stdout']['errors'][0]['line'] == 2)
    nonzero = real('nonzero', 'import sys;sys.stdout.buffer.write(b"original");sys.stderr.buffer.write(b"failure");sys.exit(7)', parse_mode='utf8')
    check('real_nonzero_retains_both_streams', nonzero, lambda x: not x['protocol_complete'] and x['owned_receipt']['returncode'] == 7 and x['parsed_stdout']['value'] == 'original')
    timeout = real('timeout', 'import sys,time;sys.stdout.buffer.write(b"prefix");sys.stdout.flush();time.sleep(10)', timeout_seconds=0.4, parse_mode='utf8')
    check('real_timeout_prefix_cleanup_receipt', timeout, lambda x: not x['protocol_complete'] and x['owned_receipt']['timed_out'] and x['raw']['timeout_prefix_stdout_raw']['bytes_observed']['bytes'] >= 6 and x['owned_receipt']['inventory_receipts'])
    # Provider-response fixtures below do not execute OS commands or real clocks.
    base = owned_command.run_owned([sys.executable, '-c', 'print("0")'], python_executable=sys.executable, timeout=2, cleanup_seconds=2)
    argv = list(base['argv'])
    def fixture(change):
        def run(*args, **kwargs):
            value = copy.deepcopy(base); change(value); return value
        return run
    check('fixture_bad_keeper_rc_rejected', call(argv, fixture(lambda x: x.update(keeper_returncode=-9))), lambda x: not x['protocol_complete'])
    check('fixture_bad_control_kept', call(argv, fixture(lambda x: x.update(control_raw=b'not json'))), lambda x: not x['protocol_complete'] and x['raw']['control_raw']['bytes_observed']['bytes'] == 8)
    check('fixture_foreign_argv_rejected', call(argv, fixture(lambda x: x.update(argv=['foreign']))), lambda x: not x['protocol_complete'])
    check('fixture_boolean_pgid_rejected', call(argv, fixture(lambda x: x.update(owned_pgid=True))), lambda x: not x['protocol_complete'])
    check('fixture_empty_stream_available', call(argv, fixture(lambda x: x.update(stdout_raw=b''))), lambda x: x['raw']['stdout_raw']['available'] and x['raw']['stdout_raw']['bytes_observed']['bytes'] == 0)
    def missing(x): del x['stderr_raw']
    check('fixture_missing_stream_distinct', call(argv, fixture(missing)), lambda x: not x['protocol_complete'] and not x['raw']['stderr_raw']['available'] and x['receipt_error'])
    def raises(*args, **kwargs): raise subprocess.TimeoutExpired(args[0], 1, output=b'partial\xff', stderr=b'stderr')
    check('fixture_provider_timeout_prefix_retained', call(argv, raises), lambda x: x['runner_exception']['type'] == 'TimeoutExpired' and x['raw']['stdout_raw']['available'] and x['raw']['stdout_raw']['partial'])
    def denied(*args, **kwargs): raise PermissionError('fixture access denied')
    check('fixture_provider_failure_unavailable_distinct', call(argv, denied), lambda x: x['runner_exception']['type'] == 'PermissionError' and not x['raw']['stdout_raw']['available'])
    for name, output in [('deep', b'['*20000+b'0'+b']'*20000), ('nonfinite', b'NaN')]:
        check('fixture_'+name+'_view_failure_preserves_raw', call(argv, fixture(lambda x, raw=output: x.update(stdout_raw=raw)), parse_mode='json'), lambda x: x['parsed_stdout']['errors'] and x['raw']['stdout_raw']['available'])
    invoked = []
    try:
        c.collect_command(argv, allowed_argv=set(), runner=lambda *a, **k: invoked.append(1), python_executable=sys.executable, timeout_seconds=1, cleanup_seconds=1)
    except ValueError: pass
    else: raise AssertionError('unlisted command admitted')
    assert not invoked
    CASES.append({'name': 'unlisted_argv_rejected_before_runner', 'PASS': True})
    out = {'scope': 'LOCAL_MAC_POSIX_METADATA_COMMANDS_AND_EXPLICIT_PROVIDER_FIXTURES_ONLY',
           'case_count': len(CASES), 'cases': CASES, 'new_T0_or_Linux_or_whole_reader_credit': False,
           'subject_sha256': hashlib.sha256(Path(c.__file__).read_bytes()).hexdigest(),
           'runner_sha256': hashlib.sha256(Path(owned_command.__file__).read_bytes()).hexdigest()}
    Path('LOCAL_COMMAND_CASES.json').write_text(json.dumps(out, allow_nan=False, indent=2)+'\n')
    print(json.dumps({'case_count': len(CASES), 'all_PASS': True}))


if __name__ == '__main__': main()
