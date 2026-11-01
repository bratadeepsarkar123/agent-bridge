"""Real broker process chain: stop fence, supervisor check, and recovery refusal.

Every process here is test-owned, runs in a TemporaryDirectory with its own
database and state file, and uses the ephemeral port the broker writes to that
state file. The live bridge is never read or contacted.
"""
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import broker
import supervisor


ROOT = Path(__file__).resolve().parents[1]


class StopChainRealProcessTests(unittest.TestCase):
    def start_broker(self, root):
        state = root / 'state.json'
        database = root / 'broker.sqlite3'
        proc = subprocess.Popen([sys.executable, '-B', '-u', str(ROOT / 'broker.py'), '--control-stdin',
                                 '--database', str(database), '--state', str(state)],
                                cwd=str(ROOT), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        output = queue.Queue()
        threading.Thread(target=lambda: output.put(proc.stdout.readline()), daemon=True).start()
        line = output.get(timeout=15)
        self.assertIn('listening', line)
        return proc, state

    def close(self, proc):
        if proc.poll() is None:
            proc.kill()  # only the test-owned child
            proc.wait(timeout=10)
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            stream.close()

    def inspect(self, state):
        with patch.object(supervisor, 'STATE', state):
            return supervisor.inspect_broker()

    def test_clean_stop_leaves_no_state_and_supervisor_sees_stopped(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            proc, state = self.start_broker(root)
            try:
                self.assertEqual('running', self.inspect(state))
                proc.stdin.write('stop\n')
                proc.stdin.flush()
                self.assertEqual(0, proc.wait(timeout=15))
                self.assertFalse(state.exists())
                self.assertFalse(broker.stop_marker_path(state).exists())
                self.assertEqual('stopped', self.inspect(state))
            finally:
                self.close(proc)

    def test_inhibit_blocks_recovery_after_hard_kill(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            proc, state = self.start_broker(root)
            try:
                # Fence the running broker, as the real stop path does before drain.
                broker.publish_stop_inhibit(state, state.read_bytes())
                proc.kill()
                proc.wait(timeout=10)
                # The unexpected death leaves discovery behind, but the inhibit wins.
                self.assertTrue(state.exists())
                self.assertEqual('stop_pending', self.inspect(state))
                recovery = subprocess.run([sys.executable, '-B', str(ROOT / 'broker.py'), '--recover',
                                           '--database', str(root / 'broker.sqlite3'),
                                           '--state', str(state)],
                                          cwd=str(ROOT), capture_output=True, text=True, timeout=30)
                self.assertNotEqual(0, recovery.returncode)
                self.assertIn('inhibited', recovery.stderr + recovery.stdout)
                self.assertTrue(state.exists())
            finally:
                self.close(proc)

    def test_hard_kill_without_inhibit_is_crashed_and_recoverable(self):
        # Documents the known residual: a kill before any fence is a plain crash,
        # so the supervisor is allowed to recover it. This is the limit in item 1.
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            proc, state = self.start_broker(root)
            try:
                proc.kill()
                proc.wait(timeout=10)
                self.assertEqual('crashed', self.inspect(state))
                self.assertFalse(broker.stop_marker_path(state).exists())
            finally:
                self.close(proc)


if __name__ == '__main__':
    unittest.main()
