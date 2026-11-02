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
from broker import require_recovery_allowed

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'broker-session.json'
LOCK = ROOT / 'supervisor-session.json'
LOG = ROOT / 'supervisor.log'
# Graceful stop channel: service_stop.py creates this file. The supervisor exits
# without any recovery launch. It never touches broker-session.json, the
# stop-pending marker, or the recovery lock; those are left exactly as they are.
STOP_REQUEST = ROOT / 'supervisor.stop-request'


def log(message):
    with LOG.open('a', encoding='utf-8') as stream:
        stream.write(time.strftime('%Y-%m-%dT%H:%M:%S') + ' ' + message + '\n')


def _honor_stop_request():
    """Return True when a stop request was present. Consume it and log once."""
    if not STOP_REQUEST.exists():
        return False
    log('stop requested; exiting without broker recovery')
    try:
        STOP_REQUEST.unlink()
    except FileNotFoundError:
        pass
    return True


def _pause(seconds):
    """Sleep in slices of at most one second, returning early when a stop request appears."""
    remaining = seconds
    while remaining > 0 and not STOP_REQUEST.exists():
        step = min(1.0, remaining)
        time.sleep(step)
        remaining -= step


def inspect_broker():
    try:
        require_recovery_allowed(STATE)
        state = json.loads(STATE.read_text(encoding='utf-8'))
        pid = state['pid']
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            return 'invalid'
        return 'running' if _process_is_alive(pid) else 'crashed'
    except FileNotFoundError:
        return 'stopped'
    except RuntimeError:
        return 'stop_pending'
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
            if _honor_stop_request():
                break
            current = inspect_broker()
            if current != previous:
                log('broker state: ' + current)
                previous = current
            if current == 'crashed':
                # Re-checked right before any launch, so a stop requested during
                # inspection cannot start a recovery.
                if _honor_stop_request():
                    break
                # broker.py verifies that the old PID is absent and binds the
                # original port before claiming the stale discovery file.
                try:
                    # Optimization only: broker --recover rechecks under the
                    # recovery gate, closing the check-to-launch race. Never
                    # clear an inhibit here, including malformed/stale markers.
                    require_recovery_allowed(STATE)
                    # Launch through broker_launcher.py so the recovered broker holds the
                    # control pipe; broker_stop.py can then request a drain instead of
                    # timing out on a pipe-less process.
                    child = subprocess.Popen([sys.executable, '-B', str(ROOT / 'broker_launcher.py'), '--recover'],
                                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL,
                                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                    time.sleep(3)
                    if child.poll() is not None:
                        log('broker recovery failed; will retry later')
                except RuntimeError:
                    log('broker recovery inhibited; operator review required')
                except OSError:
                    log('broker recovery could not launch; will retry later')
                _pause(27)
            else:
                _pause(10)
    finally:
        release(LOCK)


if __name__ == '__main__':
    run()
