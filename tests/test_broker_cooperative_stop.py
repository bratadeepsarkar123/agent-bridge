"""Real isolated HTTP broker shutdown over a launcher-owned private pipe."""
import json
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path


class BrokerCooperativeStopTests(unittest.TestCase):
    def test_owned_broker_stops_and_cleans_state(self):
        self.check_owned_broker_stop(close_pipe=False)

    def test_owner_pipe_eof_stops_broker(self):
        self.check_owned_broker_stop(close_pipe=True)

    def check_owned_broker_stop(self, close_pipe):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / 'state.json'
            database = root / 'broker.sqlite3'
            proc = subprocess.Popen([sys.executable, '-B', '-u', 'broker.py', '--control-stdin',
                                     '--database', str(database), '--state', str(state)],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            try:
                output = queue.Queue()
                threading.Thread(target=lambda: output.put(proc.stdout.readline()), daemon=True).start()
                line = output.get(timeout=10)
                self.assertIn('listening', line)
                config = json.loads(state.read_text(encoding='utf-8'))
                url = 'http://127.0.0.1:%d/v1/sessions' % config['port']
                request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + config['token']})
                with urllib.request.urlopen(request, timeout=3) as response:
                    self.assertEqual(200, response.status)
                if close_pipe:
                    proc.stdin.close()
                else:
                    proc.stdin.write('stop\n')
                    proc.stdin.flush()
                self.assertEqual(0, proc.wait(timeout=10))
                self.assertFalse(state.exists())
                with self.assertRaises((urllib.error.URLError, OSError)):
                    urllib.request.urlopen(request, timeout=2)
            finally:
                if proc.poll() is None:
                    proc.terminate()  # only the test-owned child
                    proc.wait(timeout=10)
                for stream in (proc.stdin, proc.stdout, proc.stderr):
                    stream.close()
