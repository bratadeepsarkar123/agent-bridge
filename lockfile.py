"""Serialize service lock creation and safely recover a confirmed stale PID."""
from contextlib import contextmanager
import json
import os
from pathlib import Path

from status import _process_is_alive


@contextmanager
def _guard(path):
    guard_path = Path(str(path) + '.guard')
    with guard_path.open('a+b') as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def acquire(path, metadata):
    path = Path(path)
    with _guard(path):
        if path.exists():
            try:
                previous = json.loads(path.read_text(encoding='utf-8'))
                pid = previous['pid']
                if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                    raise ValueError('invalid PID')
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise RuntimeError('existing service lock is invalid; refusing to remove it') from exc
            if _process_is_alive(pid):
                raise RuntimeError('service lock belongs to a running process')
            path.unlink()
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({'pid': os.getpid(), **metadata}, stream)


def release(path):
    path = Path(path)
    with _guard(path):
        try:
            state = json.loads(path.read_text(encoding='utf-8'))
            if state.get('pid') == os.getpid():
                path.unlink()
        except (FileNotFoundError, ValueError, OSError):
            pass
