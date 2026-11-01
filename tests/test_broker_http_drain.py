"""Exercise shutdown races against an owned broker and synthetic database."""
import json
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HARNESS = r'''
import os, sys, time
from pathlib import Path
import broker
original = broker.Broker.register
def blocked(self, data, *, credential=None):
    print('REGISTER_ENTERED', flush=True)
    release = Path(os.environ['BRIDGE_TEST_RELEASE'])
    deadline = time.monotonic() + 15
    while not release.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('test failed to release admitted request')
        time.sleep(.01)
    return original(self, data, credential=credential)
broker.Broker.register = blocked
broker.main()
'''


class BrokerHttpDrainTests(unittest.TestCase):
    def _exercise(self, *, close_pipe, timeout=False):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state, release = root / 'state.json', root / 'release'
            import os
            env = dict(os.environ, BRIDGE_TEST_RELEASE=str(release))
            argv = [sys.executable, '-B', '-u', '-c', HARNESS, '--control-stdin',
                    '--database', str(root / 'db.sqlite3'), '--state', str(state)]
            if timeout:
                argv.extend(['--drain-timeout', '1'])
            proc = subprocess.Popen(
                argv,
                cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            lines = queue.Queue()
            def collect():
                for line in proc.stdout:
                    lines.put(line.strip())
            threading.Thread(target=collect, daemon=True).start()
            errors = queue.Queue()
            def collect_errors():
                for line in proc.stderr:
                    errors.put(line.strip())
            threading.Thread(target=collect_errors, daemon=True).start()
            requester = None
            try:
                self.assertIn('listening', lines.get(timeout=10))
                info = json.loads(state.read_text(encoding='utf-8'))
                base = 'http://127.0.0.1:%s' % info['port']
                headers = {'Authorization': 'Bearer ' + info['token'],
                           'Content-Type': 'application/json'}
                outcome = queue.Queue()
                def register():
                    try:
                        req = urllib.request.Request(base + '/v1/register', headers=headers,
                            data=json.dumps({'id': 'test-codex', 'kind': 'codex', 'opt_in': True}).encode())
                        with urllib.request.urlopen(req, timeout=12) as response:
                            outcome.put((response.status, json.load(response)))
                    except Exception as exc:
                        outcome.put(exc)
                requester = threading.Thread(target=register, daemon=True)
                requester.start()
                self.assertEqual('REGISTER_ENTERED', lines.get(timeout=10))
                if close_pipe:
                    proc.stdin.close()
                else:
                    proc.stdin.write('stop\n')
                    proc.stdin.flush()
                deadline = time.monotonic() + 5
                rejected = False
                while time.monotonic() < deadline:
                    try:
                        req = urllib.request.Request(base + '/v1/sessions', headers=headers)
                        with urllib.request.urlopen(req, timeout=1) as response:
                            self.assertEqual(200, response.status)
                    except urllib.error.HTTPError as exc:
                        self.assertEqual(503, exc.code)
                        rejected = True
                        break
                    except (ConnectionAbortedError, ConnectionResetError, urllib.error.URLError):
                        # A request accepted just before the drain gate closes
                        # may be interrupted before its headers are parsed.
                        # Keep probing; a complete later request must get 503.
                        pass
                    time.sleep(.01)
                self.assertTrue(rejected, 'new request was not rejected during drain')
                self.assertIsNone(proc.poll(), 'broker exited with an admitted request in progress')
                if timeout:
                    self.assertIn('drain failed', errors.get(timeout=6))
                    self.assertIsNone(proc.poll(), 'timed-out handler must keep process alive')
                    self.assertTrue(state.exists(), 'timeout must preserve recovery state')
                    release.touch()
                    requester.join(timeout=5)
                    self.assertFalse(requester.is_alive())
                    self.assertEqual(2, proc.wait(timeout=5))
                    self.assertTrue(state.exists())
                    with closing(sqlite3.connect(root / 'db.sqlite3')) as db:
                        self.assertIsNotNone(db.execute(
                            'SELECT id FROM sessions WHERE id=?', ('test-codex',)).fetchone())
                    return
                release.touch()
                result = outcome.get(timeout=10)
                self.assertIsInstance(result, tuple, repr(result))
                self.assertEqual(200, result[0])
                self.assertEqual(0, proc.wait(timeout=10))
                self.assertFalse(state.exists())
            finally:
                release.touch()
                if requester is not None:
                    requester.join(timeout=3)
                if proc.poll() is None:
                    proc.terminate()  # exclusively the test-owned child
                    proc.wait(timeout=5)
                for stream in (proc.stdin, proc.stdout, proc.stderr):
                    stream.close()

    def test_stop_drains_admitted_request_and_rejects_new_work(self):
        self._exercise(close_pipe=False)

    def test_owner_eof_uses_same_drain(self):
        self._exercise(close_pipe=True)

    def test_timed_out_admitted_request_keeps_process_and_state_until_it_finishes(self):
        self._exercise(close_pipe=False, timeout=True)


if __name__ == '__main__':
    unittest.main()
