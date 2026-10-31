"""Supervise the local broker without starting or controlling agent sessions.

If a broker process crashes, recover its original local endpoint so existing
channel adapters can reconnect without restarting Claude. Explicit shutdown,
where the discovery file is removed, is not treated as a crash.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

from status import _process_is_alive
from lockfile import acquire, release

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'broker-session.json'
LOCK = ROOT / 'supervisor-session.json'
LOG = ROOT / 'supervisor.log'


def log(message):
    with LOG.open('a', encoding='utf-8') as stream:
        stream.write(time.strftime('%Y-%m-%dT%H:%M:%S') + ' ' + message + '\n')


def inspect_broker():
    try:
        state = json.loads(STATE.read_text(encoding='utf-8'))
        pid = state['pid']
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            return 'invalid'
        return 'running' if _process_is_alive(pid) else 'crashed'
    except FileNotFoundError:
        return 'stopped'
    except (OSError, ValueError, KeyError, TypeError):
        return 'invalid'


def run():
    try:
        acquire(LOCK, {})
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    try:
        previous = None
        while True:
            current = inspect_broker()
            if current != previous:
                log('broker state: ' + current)
                previous = current
            if current == 'crashed':
                # broker.py verifies that the old PID is absent and binds the
                # original port before claiming the stale discovery file.
                try:
                    child = subprocess.Popen([sys.executable, '-B', str(ROOT / 'broker.py'), '--recover'],
                                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL,
                                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                    time.sleep(3)
                    if child.poll() is not None:
                        log('broker recovery failed; will retry later')
                except OSError:
                    log('broker recovery could not launch; will retry later')
                time.sleep(27)
            else:
                time.sleep(10)
    finally:
        release(LOCK)


if __name__ == '__main__':
    run()
