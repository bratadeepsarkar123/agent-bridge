"""Request a broker drain through its launcher-owned control pipe."""

import argparse
import ctypes
import errno
import json
import os
from pathlib import Path
import time
from ctypes import wintypes


ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'broker-session.json'
STOP_REQUEST = ROOT / 'broker-stop-request'


def process_is_alive(pid):
    if os.name == 'nt':
        if pid > 0xFFFFFFFF:
            return False
        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        # SYNCHRONIZE permits a zero-time wait without sending a signal.
        handle = kernel32.OpenProcess(0x00100000, False, pid)
        if not handle:
            error = ctypes.get_last_error()
            if error in (5,):  # ERROR_ACCESS_DENIED: conservatively treat it as alive.
                return True
            if error in (87, 1168):  # ERROR_INVALID_PARAMETER / ERROR_NOT_FOUND.
                return False
            raise ctypes.WinError(error)
        try:
            result = kernel32.WaitForSingleObject(handle, 0)
            if result == 0x00000102:  # WAIT_TIMEOUT
                return True
            if result == 0x00000000:  # WAIT_OBJECT_0
                return False
            if result == 0xFFFFFFFF:  # WAIT_FAILED
                raise ctypes.WinError(ctypes.get_last_error())
            raise OSError(f'Unexpected process wait result: {result}')
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        raise
    return True


def _read_pid(state_path):
    with state_path.open('r', encoding='utf-8') as state_file:
        state = json.load(state_file)
    pid = state.get('pid') if isinstance(state, dict) else None
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError('broker state has no valid pid')
    return pid


def stop_broker(*, root=ROOT, timeout=120, poll_interval=0.5):
    root = Path(root)
    state_path = root / 'broker-session.json'
    request_path = root / 'broker-stop-request'

    if not state_path.exists():
        print('Broker is not running.')
        return 0

    try:
        pid = _read_pid(state_path)
    except (OSError, ValueError, json.JSONDecodeError):
        print('Cannot verify broker state; refusing to request a stop.')
        return 2

    try:
        alive = process_is_alive(pid)
    except OSError:
        print('Cannot verify broker process; refusing to request a stop.')
        return 2
    if not alive:
        print('Broker state exists, but its process is not running; refusing to stop.')
        return 2

    try:
        request_fd = os.open(request_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        print('Broker stop request is already pending.')
    else:
        os.close(request_fd)
        print('Broker stop request submitted.')

    deadline = time.monotonic() + timeout
    while True:
        if not state_path.exists():
            print('Broker stopped cleanly.')
            return 0
        try:
            alive = process_is_alive(pid)
        except OSError:
            alive = True
        if not alive:
            print('Broker process has exited.')
            return 0

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print('Timed out waiting for broker stop; broker state is still present.')
            return 1
        time.sleep(min(poll_interval, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Request a coordinated broker drain.')
    parser.add_argument('--timeout', type=float, default=120,
                        help='maximum seconds to wait for the broker to stop (default: 120)')
    args = parser.parse_args(argv)
    if args.timeout < 0:
        parser.error('--timeout must be non-negative')
    return stop_broker(timeout=args.timeout)


if __name__ == '__main__':
    raise SystemExit(main())
