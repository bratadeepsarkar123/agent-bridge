"""Real-process checks for start-broker.ps1 identity, run as -DryRun on temp checkouts.

Only temp folders and fake broker processes (sleeping Python scripts) are used. The
live folder, real broker state and real supervisors are never touched.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'start-broker.ps1'
FAKE_BROKER = 'import time\ntime.sleep(300)\n'
HARD_CODED_NAME = 'agent-bridge-experiment'


def _requires_powershell(test):
    available = os.name == 'nt' and shutil.which('powershell') is not None
    return unittest.skipUnless(available, 'requires Windows PowerShell')(test)


class StartBrokerIdentityTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.base = Path(self._temp.name).resolve()
        self.processes = []
        self.addCleanup(self._stop_processes)

    def _stop_processes(self):
        for process in self.processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    def make_checkout(self, name):
        root = self.base / name
        root.mkdir()
        shutil.copyfile(SCRIPT, root / 'start-broker.ps1')
        (root / 'broker.py').write_text(FAKE_BROKER, encoding='utf-8')
        (root / 'broker_launcher.py').write_text('# placeholder\n', encoding='utf-8')
        return root

    def start_fake_broker(self, root):
        process = subprocess.Popen(
            [sys.executable, '-B', str(root / 'broker.py')],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.processes.append(process)
        return process

    def write_state(self, root, pid):
        state = {'port': 1, 'token': 'not-a-real-token', 'pid': pid}
        (root / 'broker-session.json').write_text(json.dumps(state), encoding='utf-8')

    def dry_run(self, root):
        result = subprocess.run(
            ['powershell', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
             '-File', str(root / 'start-broker.ps1'), '-DryRun'],
            capture_output=True, text=True, timeout=120,
        )
        return result.returncode, result.stdout + result.stderr

    @_requires_powershell
    def test_own_root_broker_found_through_state_file(self):
        own = self.make_checkout('bridge')
        broker = self.start_fake_broker(own)
        self.write_state(own, broker.pid)

        code, output = self.dry_run(own)

        self.assertEqual(code, 0, output)
        self.assertIn('Bridge broker already running.', output)

    @_requires_powershell
    def test_copy_with_other_folder_name_recognizes_its_own_broker(self):
        copy = self.make_checkout('bridge-copy')
        broker = self.start_fake_broker(copy)
        self.write_state(copy, broker.pid)

        code, output = self.dry_run(copy)

        self.assertEqual(code, 0, output)
        self.assertIn('Bridge broker already running.', output)

    @_requires_powershell
    def test_other_checkout_broker_is_not_taken_for_own(self):
        own = self.make_checkout('bridge')
        copy = self.make_checkout('bridge-copy')
        other_broker = self.start_fake_broker(own)
        self.write_state(copy, other_broker.pid)

        code, output = self.dry_run(copy)

        self.assertNotEqual(code, 0, output)
        self.assertIn('could not be verified', output)
        self.assertNotIn('already running', output)

    @_requires_powershell
    def test_running_broker_without_state_blocks_duplicate_for_same_root(self):
        copy = self.make_checkout('bridge-copy')
        self.start_fake_broker(copy)

        code, output = self.dry_run(copy)

        self.assertNotEqual(code, 0, output)
        self.assertIn('no broker state exists', output)
        self.assertNotIn('Would start', output)

    @_requires_powershell
    def test_other_checkout_broker_does_not_block_start_for_prefix_named_root(self):
        own = self.make_checkout('bridge')
        copy = self.make_checkout('bridge-copy')
        self.start_fake_broker(own)

        code, output = self.dry_run(copy)

        self.assertEqual(code, 0, output)
        self.assertIn('Dry run', output)
        self.assertIn(str(copy), output)
        self.assertNotIn(HARD_CODED_NAME, output)

    @_requires_powershell
    def test_clean_checkout_dry_run_names_its_own_root(self):
        copy = self.make_checkout('staged-checkout')

        code, output = self.dry_run(copy)

        self.assertEqual(code, 0, output)
        self.assertIn('Dry run', output)
        self.assertIn(str(copy), output)
        self.assertNotIn(HARD_CODED_NAME, output)

    def test_script_has_no_hard_coded_checkout_name(self):
        text = SCRIPT.read_text(encoding='utf-8')

        self.assertNotIn(HARD_CODED_NAME, text)
        self.assertIn('$PSScriptRoot', text)


if __name__ == '__main__':
    unittest.main()
