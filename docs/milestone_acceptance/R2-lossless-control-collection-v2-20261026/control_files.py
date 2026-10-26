"""Definition-only lossless metadata-file collector, not start eligibility.

The caller supplies a reviewed allowlist and explicit byte/time limits. Deadline
checks bracket reads; they do not claim preemption of a blocked kernel syscall.
"""
import base64
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import stat
import time


def _stat(value):
    return {n: getattr(value, n) for n in ('st_dev','st_ino','st_mode','st_uid','st_gid',
            'st_nlink','st_size','st_mtime_ns','st_ctime_ns')}


def _identity(value):
    return tuple(value[n] for n in ('st_dev','st_ino','st_mode','st_uid','st_gid'))


def _error(exc):
    return {'type': type(exc).__name__, 'message': str(exc), 'errno': getattr(exc,'errno',None)}


def _parse_view(raw, maximum_depth):
    value=json.loads(raw)
    stack=[(value,0)]
    while stack:
        node,depth=stack.pop()
        if isinstance(node,float) and not math.isfinite(node):
            raise ValueError('non-authoritative parsed view contains non-finite number; original bytes retained')
        if depth>maximum_depth:
            raise ValueError('non-authoritative parsed view exceeds depth bound; original bytes retained')
        if isinstance(node,dict):stack.extend((v,depth+1) for v in node.values())
        elif isinstance(node,list):stack.extend((v,depth+1) for v in node)
    return value


def collect_file(root, relative, *, allowed_names, max_bytes, timeout_seconds, max_parse_depth=64):
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError('explicit positive nonboolean byte limit required')
    if type(timeout_seconds) not in (int,float) or not math.isfinite(timeout_seconds) or not timeout_seconds > 0:
        raise ValueError('explicit positive time budget required')
    if type(max_parse_depth) is not int or not 1 <= max_parse_depth <= 256:
        raise ValueError('parsed view depth must be an explicit bounded integer')
    result = {'name':relative,'missing':False,'raw':None,'read_complete':False,
              'eof_observed':False,'truncated':False,'custody_race':False,
              'primary_error':None,'secondary_errors':[],'custody':[],
              'parsed':None,'parse_errors':[],'max_parse_view_depth':max_parse_depth,
              'deadline_semantics':'cooperative before-read checks, not kernel-I/O preemption'}
    opened = []; directory_links = []; file_fd = None; chunks = []
    root = Path(root).absolute(); deadline = time.monotonic()+timeout_seconds
    phase='request'
    try:
        path = PurePosixPath(relative)
        if (type(relative) is not str or relative not in allowed_names or path.is_absolute()
                or str(path) != relative or not path.parts or '..' in path.parts
                or '\\' in relative):
            raise ValueError('file is outside the explicit metadata allowlist')
        phase='root'
        if root.resolve(strict=True) != root:
            raise ValueError('metadata root is not canonical/symlink-free')
        # Never reopen an absolute path through a re-resolvable ancestor.
        # Anchor every component from / with a retained no-follow directory fd.
        directory_fd=os.open(os.sep,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        opened.append(directory_fd)
        root_before=_stat(os.fstat(directory_fd))
        for part in root.parts[1:]:
            before=_stat(os.stat(part,dir_fd=directory_fd,follow_symlinks=False))
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory_fd)
            opened.append(child);actual=_stat(os.fstat(child))
            result['custody'].append({'root_part':part,'before':before,'opened':actual})
            if not stat.S_ISDIR(actual['st_mode']) or _identity(before)!=_identity(actual):
                raise RuntimeError('absolute metadata ancestor changed while opening')
            directory_links.append((directory_fd,part,actual))
            root_before=before;directory_fd=child
        root_open=_stat(os.fstat(directory_fd))
        result['root_before']=root_before;result['root_open']=root_open
        if _identity(root_before)!=_identity(root_open):
            raise RuntimeError('metadata root changed while opening')
        for part in path.parts[:-1]:
            phase='parent_directory'
            before=_stat(os.stat(part,dir_fd=directory_fd,follow_symlinks=False))
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory_fd)
            opened.append(child);actual=_stat(os.fstat(child))
            result['custody'].append({'part':part,'before':before,'opened':actual})
            if not stat.S_ISDIR(actual['st_mode']) or _identity(before)!=_identity(actual):
                raise RuntimeError('metadata parent changed while opening')
            directory_links.append((directory_fd,part,actual));directory_fd=child
        phase='entry_stat';name=path.parts[-1]
        entry_before=_stat(os.stat(name,dir_fd=directory_fd,follow_symlinks=False))
        result['entry_before']=entry_before
        if not stat.S_ISREG(entry_before['st_mode']):
            raise ValueError('metadata entry is not an ordinary file')
        phase='open_entry'
        file_fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory_fd)
        opened.append(file_fd);fd_before=_stat(os.fstat(file_fd));result['fd_before']=fd_before
        if not stat.S_ISREG(fd_before['st_mode']) or _identity(entry_before)!=_identity(fd_before):
            raise RuntimeError('metadata entry changed while opening')
        phase='read';length=0
        while True:
            if time.monotonic() >= deadline:raise TimeoutError('metadata read budget exhausted')
            # Keep the one observed overflow byte too; it is an explicit prefix,
            # never relabelled a complete file.
            count=min(65536,max_bytes+1-length)
            if count<=0:result['truncated']=True;break
            block=os.read(file_fd,count)
            if not block:result['eof_observed']=True;break
            chunks.append(block);length+=len(block)
            if length>max_bytes:result['truncated']=True;break
        phase='after_read'
        fd_after=_stat(os.fstat(file_fd));entry_after=_stat(os.stat(name,dir_fd=directory_fd,follow_symlinks=False))
        result['fd_after']=fd_after;result['entry_after']=entry_after
        changed=(fd_before!=fd_after or fd_after!=entry_after)
        for parent,part,actual in directory_links:
            after=_stat(os.stat(part,dir_fd=parent,follow_symlinks=False))
            result['custody'].append({'part':part,'after':after})
            changed=changed or _identity(actual)!=_identity(after)
        result['root_after']=_stat(root.lstat())
        changed=changed or _identity(root_open)!=_identity(result['root_after'])
        result['custody_race']=bool(changed)
        result['read_complete']=result['eof_observed'] and not result['truncated'] and not changed
    except BaseException as exc:
        result['primary_error']={'phase':phase,**_error(exc)}
        if isinstance(exc,FileNotFoundError) and file_fd is None:result['missing']=True
    finally:
        if file_fd is not None:
            raw=b''.join(chunks)
            result['raw']={'base64':base64.b64encode(raw).decode('ascii'),'bytes':len(raw),
                           'sha256':hashlib.sha256(raw).hexdigest()}
        for fd in reversed(opened):
            try:os.close(fd)
            except OSError as exc:result['secondary_errors'].append(_error(exc))
    if result['secondary_errors']:
        result['read_complete']=False
    if result['raw'] is not None:
        raw=base64.b64decode(result['raw']['base64'])
        if relative.endswith('.jsonl'):
            values=[];offset=0
            for line_number,line in enumerate(raw.splitlines(keepends=True),1):
                try:values.append({'line':line_number,'value':_parse_view(line,max_parse_depth)})
                except Exception as exc:
                    result['parse_errors'].append({'line':line_number,'byte_offset':offset,**_error(exc)})
                offset+=len(line)
            result['parsed']=values
        else:
            try:result['parsed']=_parse_view(raw,max_parse_depth)
            except Exception as exc:result['parse_errors'].append(_error(exc))
    return result
