"""Private-pipe test child only. No broker, discovery, network or real services.

Marker failure deliberately exits nonzero on stop without writing a marker.
An idle lease and parent-pipe EOF bound orphan lifetime in crash tests.
"""
import argparse
import ctypes
import json
import math
import os
import queue
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from synthetic_stop import creation_time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic-test-only', required=True, action='store_true')
    parser.add_argument('--marker-failure', action='store_true')
    parser.add_argument('--stop-delay', type=float, default=0)
    parser.add_argument('--idle-lease', type=float, default=10)
    parser.add_argument('--ack-mode', choices=('normal', 'omit', 'wrong_operation', 'wrong_identity',
                                               'fail_after_ack'),
                        default='normal')
    args = parser.parse_args()
    if not math.isfinite(args.stop_delay) or not 0 <= args.stop_delay <= 2:
        parser.error('stop delay must be finite and in [0, 2]')
    if not math.isfinite(args.idle_lease) or not .05 <= args.idle_lease <= 10:
        parser.error('idle lease must be finite and in [.05, 10]')
    incoming = queue.Queue()

    def read():
        try:
            for line in sys.stdin:
                incoming.put(json.loads(line))
        finally:
            incoming.put(None)

    threading.Thread(target=read, daemon=True).start()

    def receive():
        try:
            return incoming.get(timeout=args.idle_lease)
        except queue.Empty:
            return None

    hello = receive()
    if not isinstance(hello, dict) or hello.get('op') != 'hello':
        return 2
    token = hello.get('token')
    if not isinstance(token, str) or len(token) != 64:
        return 2
    creation = None
    if os.name == 'nt':
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        creation = creation_time(kernel.GetCurrentProcess())
    identity = {'pid': os.getpid(), 'creation': creation,
                'token': token, 'synthetic': True}

    def emit(value):
        print(json.dumps(value), flush=True)

    with tempfile.TemporaryDirectory(prefix='lifecycle-intent-child-') as directory:
        emit(identity)
        while True:
            command = receive()
            if command is None:
                return 0
            if not isinstance(command, dict) or command.get('identity') != identity:
                emit({'error': 'identity mismatch'})
                continue
            if command.get('op') == 'ping':
                emit(identity)
            elif command.get('op') == 'stop':
                operation = command.get('operation')
                try:
                    if str(uuid.UUID(operation)) != operation:
                        raise ValueError()
                except (TypeError, ValueError, AttributeError):
                    emit({'error': 'invalid operation'})
                    continue
                if args.marker_failure:
                    # Models a child that cannot persist its broker-side marker,
                    # then exits. The coordinator's intent must still inhibit.
                    return 3
                marker = Path(directory) / 'synthetic-stop-marker'
                marker.write_text('synthetic only', encoding='ascii')
                time.sleep(args.stop_delay)
                completion = {'event': 'stop_complete', 'operation': operation, 'identity': identity}
                break
            else:
                emit({'error': 'unsupported command'})
    # Acknowledge only AFTER synthetic cleanup. Parent must also verify zero
    # process exit: writing this frame alone cannot prove a successful stop.
    if args.ack_mode == 'wrong_operation':
        completion['operation'] = str(uuid.uuid4())
    elif args.ack_mode == 'wrong_identity':
        completion['identity'] = dict(identity, pid=identity['pid'] + 1)
    if args.ack_mode != 'omit':
        emit(completion)
    return 4 if args.ack_mode == 'fail_after_ack' else 0


if __name__ == '__main__':
    raise SystemExit(main())
