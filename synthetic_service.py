"""Private-pipe synthetic child. Never imports live state or runs broker.main."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import queue
import sys
import tempfile
import threading
import time

from synthetic_stop import ORDER, creation_time


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=ORDER, required=True)
    parser.add_argument('--delay', type=float, default=0)
    args = parser.parse_args()
    if not 0 <= args.delay <= 2:
        parser.error('delay out of range')
    messages = queue.Queue()

    def read():
        try:
            for line in sys.stdin:
                messages.put(json.loads(line))
        finally:
            messages.put(None)

    threading.Thread(target=read, daemon=True).start()
    # Bounded idle lease also prevents orphaned test children on parent failure.
    def receive():
        try:
            return messages.get(timeout=10)
        except queue.Empty:
            return None

    hello = receive()
    if not isinstance(hello, dict) or hello.get('op') != 'hello' or not isinstance(hello.get('token'), str):
        return
    if os.name == 'nt':
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        creation = creation_time(kernel.GetCurrentProcess())
    else:
        creation = None
    identity = dict(pid=os.getpid(), creation=creation, token=hello['token'],
                    role=args.role, synthetic=True)

    def emit(value):
        print(json.dumps(value), flush=True)

    with tempfile.TemporaryDirectory(prefix='bridge-synthetic-service-') as folder:
        if args.role == 'broker':
            from broker import Broker
            broker = Broker(Path(folder) / 'synthetic.sqlite3')
            for name in ('synthetic-sender', 'synthetic-recipient'):
                broker.register({'id': name, 'kind': 'system', 'opt_in': True})
            message = broker.enqueue({'sender': 'synthetic-sender',
                                      'recipient': 'synthetic-recipient', 'text': 'synthetic only'})
        emit({'identity': identity, 'workspace': folder})
        while True:
            command = receive()
            if command is None:
                return
            if not isinstance(command, dict) or command.get('identity') != identity:
                emit({'error': 'identity mismatch'})
                continue
            if command.get('op') == 'ping':
                emit(identity)
            elif command.get('op') == 'stop':
                time.sleep(args.delay)
                if args.role == 'broker':
                    broker.ack({'id': message['id'], 'recipient': 'synthetic-recipient'})
                    assert broker.inbox('synthetic-recipient')['messages'][0]['status'] == 'acknowledged'
                # Returning exits the temp-directory context before process exit.
                return
            else:
                emit({'error': 'unsupported operation'})


if __name__ == '__main__':
    run()
