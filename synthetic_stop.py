"""Cooperative shutdown of children launched here, over private inherited pipes.

No discovery files, arbitrary PIDs, network endpoints, signals or forced kills.
Callers serialize access. Only synthetic_service.py children are supported.
"""
import ctypes
from ctypes import wintypes
import json
import math
import os
from pathlib import Path
import queue
import secrets
import subprocess
import sys
import threading


ORDER = {'guard': 0, 'supervisor': 1, 'worker': 2, 'broker': 3}


def creation_time(handle):
    """Read creation time from a retained Windows process handle, never a PID."""
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    values = [wintypes.FILETIME() for _ in range(4)]
    if not kernel.GetProcessTimes(handle, *(ctypes.byref(v) for v in values)):
        raise OSError('cannot verify child creation time')
    return (values[0].dwHighDateTime << 32) | values[0].dwLowDateTime


def _bounded(value):
    if (isinstance(value, bool) or not isinstance(value, (float, int))
            or not math.isfinite(value) or not 0 < value <= 10):
        raise ValueError('timeout must be between 0 and 10 seconds')


class SyntheticStack:
    """Owns a complete inventory of explicitly launched synthetic children."""

    def __init__(self):
        self._children = []
        self.events = []

    def launch(self, role, *, delay=0):
        if role not in ORDER or type(delay) not in (int, float) or not math.isfinite(delay) or not 0 <= delay <= 2:
            raise ValueError('invalid synthetic service configuration')
        child = _Child(role, delay)
        self._children.append(child)
        child.start()
        return child

    def stop(self, *, timeout=3):
        _bounded(timeout)
        ordered = sorted(self._children, key=lambda c: ORDER[c.role])
        # Check every still-running child before requesting any shutdown.
        for child in ordered:
            child.verify(timeout)
        result = []
        for child in ordered:
            status = child.stop(timeout)
            result.append((child.role, status))
            self.events.append((child.role, status))
            if status not in ('stopped', 'already_stopped'):
                break
        return result

    def close(self):
        # EOF is a cooperative fallback for our own children, never termination.
        for child in self._children:
            if child.process.stdin and not child.process.stdin.closed:
                child.process.stdin.close()
        for child in self._children:
            child.process.wait(timeout=15)
            child.process.stdout.close()
            child.reader.join(timeout=1)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _Child:
    def __init__(self, role, delay):
        self.role = role
        self.token = secrets.token_hex(32)
        self.messages = queue.Queue()
        self.process = subprocess.Popen(
            [sys.executable, '-B', '-u', str(Path(__file__).with_name('synthetic_service.py')),
             '--role', role, '--delay', str(delay)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding='utf-8',
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        self.creation = creation_time(int(self.process._handle)) if os.name == 'nt' else None
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.identity = None
        self.requested = False

    def _read(self):
        try:
            for line in self.process.stdout:
                self.messages.put(json.loads(line))
        finally:
            self.messages.put(None)

    def _send(self, command):
        self.process.stdin.write(json.dumps(command) + '\n')
        self.process.stdin.flush()

    def _receive(self, timeout):
        try:
            response = self.messages.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError('synthetic service did not respond') from None
        if response is None:
            raise RuntimeError('synthetic service pipe closed')
        return response

    def start(self):
        self._send({'op': 'hello', 'token': self.token})
        ready = self._receive(5)
        self.identity = ready['identity']
        self.workspace = Path(ready['workspace'])
        self.verify(3)

    def verify(self, timeout):
        if self.process.poll() is not None:
            if self.process.returncode != 0:
                raise RuntimeError('synthetic child failed')
            return
        if os.name == 'nt' and creation_time(int(self.process._handle)) != self.creation:
            raise RuntimeError('child creation identity mismatch')
        expected = {'pid': self.process.pid, 'creation': self.creation,
                    'token': self.token, 'role': self.role, 'synthetic': True}
        if self.identity != expected:
            raise RuntimeError('synthetic identity mismatch')
        if not self.requested:
            self._send({'op': 'ping', 'identity': expected})
            if self._receive(timeout) != expected:
                raise RuntimeError('synthetic challenge failed')

    def stop(self, timeout):
        self.verify(timeout)
        if self.process.poll() is not None:
            return 'already_stopped'
        if not self.requested:
            self._send({'op': 'stop', 'identity': self.identity})
            self.requested = True
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return 'timeout'
        if self.process.returncode != 0:
            raise RuntimeError('synthetic shutdown failed')
        return 'stopped'
