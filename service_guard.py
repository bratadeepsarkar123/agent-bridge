"""Watch explicitly configured local bridge services; never start agent chats."""
import argparse
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from lockfile import acquire, release

ROOT = Path(__file__).resolve().parent
LOCK = ROOT / 'service-guard.json'
# Graceful stop channel: service_stop.py creates this file; the guard consumes it
# and exits its loop. The guard never signals or kills its children.
STOP_REQUEST = ROOT / 'service-guard.stop-request'
ALLOWED = {
    'native_worker.py',
    'watch_session.py',
    'watch_errors.py',
    'supervisor.py',
    'multi_dispatcher.py',
}


def process_identity(pid):
    """Return Windows creation time, or None when identity cannot be verified."""
    if os.name != 'nt' or not isinstance(pid, int) or pid <= 0:
        return None
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel.OpenProcess.restype = wintypes.HANDLE
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return None
    try:
        code = wintypes.DWORD()
        kernel.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel.GetExitCodeProcess.restype = wintypes.BOOL
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value != 259:
            return None
        creation, exit_time, kernel_time, user_time = (wintypes.FILETIME() for _ in range(4))
        kernel.GetProcessTimes.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.FILETIME),
                                           ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
                                           ctypes.POINTER(wintypes.FILETIME))
        kernel.GetProcessTimes.restype = wintypes.BOOL
        if not kernel.GetProcessTimes(handle, ctypes.byref(creation), ctypes.byref(exit_time),
                                      ctypes.byref(kernel_time), ctypes.byref(user_time)):
            return None
        return (creation.dwHighDateTime << 32) | creation.dwLowDateTime
    finally:
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel.CloseHandle(handle)


def validate_service(service):
    script = service.get('script')
    args = service.get('args')
    if script not in ALLOWED or not isinstance(args, list) or not all(isinstance(v, str) for v in args):
        raise ValueError('invalid service configuration')


def check_once(services, identify=process_identity, popen=subprocess.Popen):
    events = []
    for service in services:
        validate_service(service)
        actual = identify(service.get('pid'))
        if service.get('creation') is None and actual is not None:
            events.append({'service': service['script'], 'status': 'identity_unknown'})
            continue
        if actual is not None and actual == service.get('creation'):
            continue
        # Do not replace a service when the process exists but its identity
        # cannot be verified; PID reuse and access denial must not cause duplicates.
        from status import _process_is_alive
        if actual is None and _process_is_alive(service.get('pid', 0)):
            events.append({'service': service['script'], 'status': 'identity_unknown'})
            continue
        child = popen([sys.executable, '-u', str(ROOT / service['script']), *service['args']],
                      stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                      creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        service['pid'] = child.pid
        service['creation'] = identify(child.pid)
        events.append({'service': service['script'], 'status': 'start_requested', 'pid': child.pid})
    return events


def save_config(path, services):
    temporary = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    try:
        temporary.write_text(json.dumps({'services': services}, indent=2), encoding='utf-8')
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _pause(seconds, stop_request, pause):
    """Sleep in slices of at most one second, returning early when a stop request appears."""
    remaining = seconds
    while remaining > 0 and not stop_request.exists():
        step = min(1.0, remaining)
        pause(step)
        remaining -= step


def run_loop(services, config_path, interval, stop_request, *, check=check_once,
             save=save_config, say=print, pause=time.sleep):
    """Check services each pass until a stop request is seen.

    The request is checked before every pass, so a stopped guard spawns nothing.
    On a stop, the request file is consumed and the loop returns. Children are
    neither signalled nor killed here; they keep running.
    """
    while True:
        if stop_request.exists():
            say(json.dumps({'status': 'stop_requested'}))
            try:
                stop_request.unlink()
            except FileNotFoundError:
                pass
            return
        try:
            events = check(services)
            if any(event.get('status') == 'start_requested' for event in events):
                save(config_path, services)
            for event in events:
                say(json.dumps(event))
        except (OSError, ValueError) as exc:
            say(json.dumps({'status': 'guard_error', 'error_type': type(exc).__name__}))
        _pause(interval, stop_request, pause)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    if args.interval < 10:
        parser.error('interval must be at least 10 seconds')
    config = json.loads(args.config.read_text(encoding='utf-8'))
    services = config.get('services')
    if not isinstance(services, list):
        raise SystemExit('Invalid service configuration.')
    for service in services:
        validate_service(service)
    acquire(LOCK, {})
    try:
        run_loop(services, args.config, args.interval, STOP_REQUEST, say=lambda line: print(line, flush=True))
    finally:
        release(LOCK)


if __name__ == '__main__':
    main()
