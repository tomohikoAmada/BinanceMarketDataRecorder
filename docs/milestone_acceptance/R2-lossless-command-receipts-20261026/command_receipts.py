"""Lossless read-command receipts; no service policy or start eligibility.

The frozen caller must supply an exact read-only argv allowlist and qualified
owned-command runner. This definition-only adapter never infers those facts
from a command's spelling or exit status. Byte custody precedes parsing.
"""
import base64
import hashlib
import json
import math
import time


RAW_FIELDS = ('stdout_raw', 'stderr_raw', 'control_raw',
              'timeout_prefix_stdout_raw', 'timeout_prefix_stderr_raw')


def pack(raw):
    return {'base64': base64.b64encode(raw).decode('ascii'),
            'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def error(exc):
    return {'type': type(exc).__name__, 'message': str(exc)}


def _view(raw, depth):
    value = json.loads(raw)
    stack = [(value, 0)]
    while stack:
        node, level = stack.pop()
        if level > depth or isinstance(node, float) and not math.isfinite(node):
            raise ValueError('bounded non-authoritative parsed view unavailable')
        if isinstance(node, dict):
            stack.extend((item, level + 1) for item in node.values())
        elif isinstance(node, list):
            stack.extend((item, level + 1) for item in node)
    return value


def parse_bytes(raw, mode, max_depth):
    result = {'mode': mode, 'value': None, 'errors': []}
    if mode == 'none':
        return result
    if mode == 'utf8':
        try:
            result['value'] = raw.decode('utf8', errors='strict')
        except Exception as exc:
            result['errors'].append(error(exc))
        return result
    if mode == 'json':
        try:
            result['value'] = _view(raw, max_depth)
        except Exception as exc:
            result['errors'].append(error(exc))
        return result
    values = []
    offset = 0
    for line_number, line in enumerate(raw.splitlines(keepends=True), 1):
        try:
            values.append({'line': line_number, 'value': _view(line, max_depth)})
        except Exception as exc:
            result['errors'].append({'line': line_number, 'byte_offset': offset,
                                     **error(exc)})
        offset += len(line)
    result['value'] = values
    return result


def protocol_ok(receipt):
    """Exact leased-group success, without parsing stdout or granting trust."""
    def positive(value):
        return type(value) is int and value > 0
    try:
        control = json.loads(receipt['control_raw'])
        return (type(receipt['returncode']) is int and receipt['returncode'] == 0
                and type(receipt['keeper_returncode']) is int
                and receipt['keeper_returncode'] == 0
                and receipt['timed_out'] is False
                and receipt['lease_released'] is True
                and receipt['group_KILL_used'] is False
                and receipt['cleanup_error'] is None
                and receipt['cleanup'] == 'LEASED_GROUP_NO_LIVE_MEMBERS'
                and receipt['unexpected_live_descendants'] == []
                and isinstance(receipt['final_group_members'], list)
                and all(isinstance(x, dict) and x.get('live') is False
                        for x in receipt['final_group_members'])
                and positive(receipt['child_pid'])
                and positive(receipt['owned_pgid'])
                and receipt['owned_pgid'] == receipt['child_pid']
                and positive(receipt['inner_pid'])
                and set(control) == {'keeper_pid', 'pgid', 'inner_pid', 'returncode'}
                and all(type(control[n]) is int for n in control)
                and control == {'keeper_pid': receipt['child_pid'],
                                'pgid': receipt['child_pid'],
                                'inner_pid': receipt['inner_pid'], 'returncode': 0})
    except Exception:
        return False


def collect_command(argv, *, allowed_argv, runner, python_executable,
                    timeout_seconds, cleanup_seconds, parse_mode='none',
                    max_parse_depth=64):
    """Keep every returned owned receipt/raw field, including failed commands.

    Runner qualification and the allowlist's read-only nature are caller gates.
    The unchanged leased runner has bounded execution/cleanup times, not a hard
    output-memory limit or universal control of descendants that escape groups.
    """
    if (type(argv) not in (list, tuple) or not argv
            or any(type(x) is not str or not x or '\x00' in x for x in argv)
            or tuple(argv) not in allowed_argv):
        raise ValueError('argv is not in the frozen exact read-command allowlist')
    if (any(type(x) not in (int, float) or not math.isfinite(x) or x <= 0
            for x in (timeout_seconds, cleanup_seconds))
            or type(max_parse_depth) is not int or not 1 <= max_parse_depth <= 256
            or parse_mode not in ('none', 'utf8', 'json', 'jsonl')
            or type(python_executable) is not str or not python_executable.startswith('/')
            or not callable(runner)):
        raise ValueError('explicit finite budgets/interpreter/parser/runner required')
    result = {'argv': list(argv), 'timeout_seconds': timeout_seconds,
              'cleanup_seconds': cleanup_seconds, 'started_monotonic_ns': time.monotonic_ns(),
              'started_utc_ns': time.time_ns(), 'runner_exception': None,
              'receipt_error': None, 'owned_receipt': None, 'raw': {},
              'protocol_complete': False, 'parsed_stdout': None,
              'authority': 'COMMAND_BYTE_CUSTODY_ONLY_NOT_READER_OR_T0_QUALIFICATION'}
    receipt = None
    try:
        receipt = runner(list(argv), python_executable=python_executable,
                         timeout=timeout_seconds, cleanup_seconds=cleanup_seconds)
    except Exception as exc:
        result['runner_exception'] = error(exc)
        # An exceptional provider may still expose observed timeout prefixes.
        for name, value in [('stdout_raw', getattr(exc, 'stdout', None)),
                            ('stderr_raw', getattr(exc, 'stderr', None))]:
            result['raw'][name] = {'available': isinstance(value, bytes),
                                   'partial': True,
                                   'bytes_observed': pack(value) if isinstance(value, bytes) else None}
    if receipt is not None:
        if not isinstance(receipt, dict):
            result['receipt_error'] = 'runner did not return an owned receipt mapping'
        else:
            # Preserve the complete structured keeper and every ps inventory
            # receipt. Raw observations are encoded before any parse attempt.
            result['owned_receipt'] = {k: v for k, v in receipt.items() if k not in RAW_FIELDS}
            for name in RAW_FIELDS:
                value = receipt.get(name)
                result['raw'][name] = {'available': isinstance(value, bytes),
                    'partial': receipt.get('timed_out') is True,
                    'bytes_observed': pack(value) if isinstance(value, bytes) else None}
                if not isinstance(value, bytes):
                    result['receipt_error'] = 'owned receipt is missing original byte fields'
            if all(result['raw'][n]['available'] for n in RAW_FIELDS):
                result['protocol_complete'] = receipt.get('argv') == list(argv) and protocol_ok(receipt)
                result['parsed_stdout'] = parse_bytes(receipt['stdout_raw'], parse_mode, max_parse_depth)
    result['ended_monotonic_ns'] = time.monotonic_ns()
    result['ended_utc_ns'] = time.time_ns()
    return result
