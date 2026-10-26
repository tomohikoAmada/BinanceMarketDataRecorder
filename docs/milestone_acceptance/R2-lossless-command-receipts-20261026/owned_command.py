"""Finite leased process-group command wrapper, not a daemon/service.

Only descendants retaining this new session/group are covered. Exact Linux
runuser/helper group retention must be qualified before any operational use.
"""
import json
import base64
import hashlib
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time


def _bytepack(raw):
    return {'base64': base64.b64encode(raw).decode('ascii'), 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest()}


def _members(pgid, timeout, evidence=None):
    proof = {'argv': ['/bin/ps', '-axo', 'pid=,pgid=,stat='],
             'timeout_seconds': timeout, 'before_monotonic_ns': time.monotonic_ns(),
             'stdout': _bytepack(b''), 'stderr': _bytepack(b''), 'returncode': None,
             'exception': None, 'parse_error': None, 'stdout_available': False,
             'stderr_available': False, 'partial_by_timeout': False}
    if evidence is not None:
        if len(evidence) >= 128:
            raise RuntimeError('owned_group_inventory_receipt_bound_exceeded')
        evidence.append(proof)
    try:
        if timeout <= 0:
            raise TimeoutError('owned_group_inventory_budget_exhausted')
        try:
            result = subprocess.run(proof['argv'], capture_output=True,
                                    timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            proof['stdout_available'] = exc.output is not None
            proof['stderr_available'] = exc.stderr is not None
            proof['partial_by_timeout'] = True
            proof['stdout'] = _bytepack(exc.output or b'')
            proof['stderr'] = _bytepack(exc.stderr or b'')
            proof['exception'] = 'TimeoutExpired'
            raise
        proof['stdout_available'] = proof['stderr_available'] = True
        proof['stdout'] = _bytepack(result.stdout)
        proof['stderr'] = _bytepack(result.stderr)
        proof['returncode'] = result.returncode
        if result.returncode != 0:
            raise RuntimeError('owned_group_inventory_nonzero_exit')
        members = []
        for line in result.stdout.decode('ascii', errors='strict').splitlines():
            fields = line.split()
            if len(fields) != 3 or not fields[0].isdecimal() or not fields[1].isdecimal():
                raise RuntimeError('owned_group_ps_malformed')
            pid, group = int(fields[0]), int(fields[1])
            if group == pgid:
                state = fields[2]
                if not state:
                    raise RuntimeError('owned_group_ps_state_empty')
                members.append({'pid': pid, 'pgid': group, 'state': state,
                                'live': not state.startswith('Z')})
        proof['matching_members'] = members
        return members
    except BaseException as exc:
        proof['exception'] = type(exc).__name__ + ':' + str(exc)
        proof['parse_error'] = (type(exc).__name__ + ':' + str(exc)
                                if proof['returncode'] == 0 else None)
        raise
    finally:
        proof['after_monotonic_ns'] = time.monotonic_ns()


def run_owned(argv, *, python_executable, timeout, cleanup_seconds):
    """Hold an unreaped keeper PID/PGID until bounded cleanup is observed."""
    if timeout <= 0 or cleanup_seconds <= 0:
        raise ValueError('owned_group_nonpositive_budget')
    begin = time.monotonic()
    deadline = begin + timeout
    final_deadline = deadline + cleanup_seconds
    read_fd, write_fd = os.pipe()
    keeper_argv = [python_executable, '-B', str(Path(__file__).resolve()),
                   '--keeper', str(write_fd), str(int(final_deadline * 10**9)), json.dumps(argv)]
    proc = None
    buffers = {'stdout': bytearray(), 'stderr': bytearray(), 'control': bytearray()}
    timed_out = False
    control = None
    cleanup_error = None
    unexpected_live = []
    final_members = []
    inventory_receipts = []
    timeout_prefix_stdout = timeout_prefix_stderr = b''
    released = False
    killed = False
    sel = selectors.DefaultSelector()

    def drain(wait):
        for key, _ in sel.select(max(0.0, wait)):
            data = os.read(key.fd, 65536)
            if not data:
                sel.unregister(key.fileobj)
            else:
                buffers[key.data].extend(data)
                if key.data == 'control' and len(buffers['control']) > 4096:
                    raise RuntimeError('owned_group_control_oversize')

    def inventory():
        values = _members(proc.pid, min(0.3, final_deadline - time.monotonic()), inventory_receipts)
        if not any(x['pid'] == proc.pid for x in values):
            raise RuntimeError('owned_keeper_missing_from_inventory_before_reap')
        return values

    def signal_owned(sig):
        # proc has never been polled/waited/communicated/reaped here. Its PID
        # remains leased even if the keeper is already a zombie.
        if os.getpgid(proc.pid) != proc.pid or os.getsid(proc.pid) != proc.pid:
            raise RuntimeError('owned_group_lease_identity_changed')
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass

    try:
        proc = subprocess.Popen(keeper_argv, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, pass_fds=(write_fd,),
            start_new_session=True)
        os.close(write_fd); write_fd = -1
        if os.getpgid(proc.pid) != proc.pid or os.getsid(proc.pid) != proc.pid:
            raise RuntimeError('owned_group_lease_not_created')
        for stream, name in [(proc.stdout, 'stdout'), (proc.stderr, 'stderr'), (read_fd, 'control')]:
            fd = stream if isinstance(stream, int) else stream.fileno()
            os.set_blocking(fd, False); sel.register(stream, selectors.EVENT_READ, name)
        while time.monotonic() < deadline:
            drain(min(0.02, max(0, deadline-time.monotonic())))
            if b'\n' in buffers['control']:
                control = json.loads(bytes(buffers['control']))
                if (set(control) != {'keeper_pid', 'pgid', 'inner_pid', 'returncode'}
                        or control['keeper_pid'] != proc.pid or control['pgid'] != proc.pid
                        or type(control['inner_pid']) is not int or control['inner_pid'] <= 0
                        or type(control['returncode']) is not int):
                    raise RuntimeError('owned_group_control_invalid')
                break
            if not any(k.data == 'control' for k in sel.get_map().values()):
                raise RuntimeError('owned_group_keeper_exited_without_result')
        if control is None:
            timed_out = True
        final_deadline = min(final_deadline, time.monotonic() + cleanup_seconds)
        if timed_out:
            timeout_prefix_stdout = bytes(buffers['stdout'])
            timeout_prefix_stderr = bytes(buffers['stderr'])
        members = inventory()
        unexpected_live = [x for x in members if x['live'] and x['pid'] != proc.pid]
        if not timed_out and not unexpected_live and control is not None:
            # The keeper itself is the only live member and frozen keeper code
            # never forks again after emitting control. Release the lease last.
            proc.stdin.write(b'R'); proc.stdin.flush(); released = True
        else:
            signal_owned(signal.SIGTERM)
            term_deadline = min(time.monotonic() + cleanup_seconds / 2, final_deadline)
            while time.monotonic() < term_deadline:
                drain(min(0.02, term_deadline-time.monotonic()))
                members = inventory()
                if not any(x['live'] and x['pid'] != proc.pid for x in members):
                    break
            # Kill the still-leased group even when the command wrapper exited
            # and closed its pipes; never use direct-child EOF as tree evidence.
            signal_owned(signal.SIGKILL); killed = True
        while time.monotonic() < final_deadline:
            drain(min(0.02, max(0, final_deadline-time.monotonic())))
            final_members = inventory()
            if not any(x['live'] for x in final_members) and not sel.get_map():
                break
        if any(x['live'] for x in final_members) or sel.get_map():
            raise TimeoutError('owned_group_unresolved_cleanup_or_pipes')
    except BaseException as exc:
        cleanup_error = type(exc).__name__ + ':' + str(exc)
        final_deadline = min(final_deadline, time.monotonic() + cleanup_seconds)
        if proc is not None:
            try:
                signal_owned(signal.SIGKILL); killed = True
                while time.monotonic() < final_deadline:
                    drain(min(0.01, max(0, final_deadline-time.monotonic())))
                    final_members = inventory()
                    if not any(x['live'] for x in final_members) and not sel.get_map():
                        break
            except BaseException as secondary:
                cleanup_error += '; cleanup=' + type(secondary).__name__ + ':' + str(secondary)
    finally:
        # No group signal may occur after this point: reaping releases the PID.
        if proc is not None:
            try:
                guardian_returncode = proc.wait(timeout=max(0, final_deadline-time.monotonic()))
            except subprocess.TimeoutExpired:
                guardian_returncode = None
                cleanup_error = (cleanup_error or '') + '; keeper_not_reaped_within_budget'
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()
        else:
            guardian_returncode = None
        sel.close()
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)
    live_after = [x for x in final_members if x['live']]
    if live_after:
        cleanup_error = (cleanup_error or '') + '; live_owned_members_remaining'
    return {'argv': argv, 'keeper_argv': keeper_argv,
            'child_pid': proc.pid if proc is not None else None,
            'owned_pgid': proc.pid if proc is not None else None,
            'inner_pid': control.get('inner_pid') if control else None,
            'returncode': control.get('returncode') if control else None,
            'keeper_returncode': guardian_returncode,
            'timed_out': timed_out, 'timeout_seconds': timeout,
            'cleanup_reserve_seconds': cleanup_seconds,
            'cleanup': 'LEASED_GROUP_NO_LIVE_MEMBERS' if not live_after and cleanup_error is None
                       else 'UNRESOLVED_OWNED_GROUP_CLEANUP',
            'cleanup_error': cleanup_error, 'lease_released': released,
            'group_KILL_used': killed, 'unexpected_live_descendants': unexpected_live,
            'final_group_members': final_members, 'inventory_receipts': inventory_receipts,
            'ownership_scope': 'new leased session/group only; no universal escaped-descendant claim',
            'stdout_raw': bytes(buffers['stdout']), 'stderr_raw': bytes(buffers['stderr']),
            'control_raw': bytes(buffers['control']),
            'timeout_prefix_stdout_raw': timeout_prefix_stdout,
            'timeout_prefix_stderr_raw': timeout_prefix_stderr}


def keeper_main(fd, deadline_ns, argv):
    if os.getpgrp() != os.getpid() or os.getsid(0) != os.getpid():
        raise RuntimeError('keeper_missing_new_session')
    # A caught handler is reset to default by exec. SIG_IGN would leak to inner.
    signal.signal(signal.SIGTERM, lambda *_: None)
    os.set_blocking(sys.stdin.fileno(), False)
    inner = subprocess.Popen(argv, stdin=subprocess.DEVNULL, close_fds=True)
    sent = False
    while time.monotonic_ns() < deadline_ns:
        code = inner.poll()
        if code is not None and not sent:
            body = json.dumps({'keeper_pid': os.getpid(), 'pgid': os.getpgrp(),
                               'inner_pid': inner.pid, 'returncode': code}).encode() + b'\n'
            while body:
                n = os.write(fd, body); body = body[n:]
            os.close(fd); sent = True
        try:
            data = os.read(sys.stdin.fileno(), 1)
        except BlockingIOError:
            data = None
        if data == b'R' and sent:
            return 0
        if data == b'' or data not in (None, b'R'):
            break
        time.sleep(0.02)
    # Parent lost, deadline expired, or invalid release: only this owned group.
    os.killpg(os.getpgrp(), signal.SIGKILL)
    return 2


if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '--keeper':
        raise SystemExit('private keeper protocol only')
    try:
        result = keeper_main(int(sys.argv[2]), int(sys.argv[3]), json.loads(sys.argv[4]))
    except BaseException:
        if os.getpgrp() == os.getpid() and os.getsid(0) == os.getpid():
            os.killpg(os.getpgrp(), signal.SIGKILL)
        raise
    raise SystemExit(result)
