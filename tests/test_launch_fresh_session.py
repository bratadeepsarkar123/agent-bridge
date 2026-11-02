"""Launcher fresh-session flow (-FreshSessionId) in -PrepareOnly dry runs only.

Each test copies the launcher and its Python modules into a TemporaryDirectory,
creates a fresh temp broker database there, and runs the copied script. Nothing
here reads the live broker database, broker state, or any credential file, and
the real Claude executable is never started (-PrepareOnly returns before launch).
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

import credential_store as store
from broker import Broker

ROOT = Path(__file__).resolve().parent.parent
POWERSHELL = Path(os.environ.get('SystemRoot', r'C:\Windows')) / 'System32' / 'WindowsPowerShell' / 'v1.0' / 'powershell.exe'


@unittest.skipUnless(sys.platform == 'win32' and POWERSHELL.exists(), 'Launcher test needs Windows PowerShell')
class FreshSessionLauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.copy = Path(self.temp.name) / 'copy'
        self.copy.mkdir()
        # Copy only code: no database, broker state, or credential files.
        shutil.copy2(ROOT / 'launch-claude.ps1', self.copy / 'launch-claude.ps1')
        for module in ROOT.glob('*.py'):
            shutil.copy2(module, self.copy / module.name)
        self.database = self.copy / 'broker.sqlite3'
        Broker(self.database)  # schema in the temp copy only
        self.credentials = Path(self.temp.name) / 'credentials'

    def run_launcher(self, *args):
        command = [str(POWERSHELL), '-NoProfile', '-ExecutionPolicy', 'Bypass',
                   '-File', str(self.copy / 'launch-claude.ps1'), *args]
        return subprocess.run(command, capture_output=True, text=True, timeout=180,
                              cwd=self.temp.name)

    def bound(self, profile):
        return Broker(self.database).profile_status(profile)['bound']

    def dpapi_files(self):
        if not self.credentials.exists():
            return []
        return sorted(p.name for p in self.credentials.glob('credential-*.dpapi'))

    def test_prepare_only_fresh_session_provisions_and_does_not_launch(self):
        session = str(uuid.uuid4())
        result = self.run_launcher('-FreshSessionId', session, '-CredentialsDir', str(self.credentials),
                                   '-PrepareOnly')
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(session, report['sessionId'])
        self.assertTrue(report['fresh'])
        self.assertFalse(report['resume'])
        self.assertTrue(report['credentialProvisioned'])
        self.assertFalse(report['launched'])
        self.assertEqual(['BRIDGE_SESSION_CREDENTIAL_FILE', 'BRIDGE_SESSION_CREDENTIAL_FORMAT'],
                         report['credentialEnvironment'])
        self.assertTrue(self.bound(session))
        self.assertEqual(1, len(self.dpapi_files()))
        self.assertTrue((self.copy / ('mcp-' + session + '.json')).exists())

    def test_credential_value_is_never_printed_or_passed_on_the_command_line(self):
        session = str(uuid.uuid4())
        result = self.run_launcher('-FreshSessionId', session, '-CredentialsDir', str(self.credentials),
                                   '-PrepareOnly')
        self.assertEqual(0, result.returncode, result.stderr)
        token = store.read_generation(self.credentials / self.dpapi_files()[0]).decode('ascii')
        self.assertNotIn(token, result.stdout)
        self.assertNotIn(token, result.stderr)
        self.assertNotIn(token, ' '.join(map(str, [POWERSHELL, self.copy])))
        self.assertNotIn('credential-', result.stdout)  # the file name is not printed either

    def test_resume_path_is_unchanged_and_provisions_nothing(self):
        session = str(uuid.uuid4())
        result = self.run_launcher('-ClaudeSessionId', session, '-PrepareOnly')
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(report['resume'])
        self.assertFalse(report['fresh'])
        self.assertFalse(report['credentialProvisioned'])
        self.assertEqual([], report['credentialEnvironment'])
        self.assertIn('--resume', report['arguments'])
        self.assertFalse(self.bound(session))
        self.assertEqual([], self.dpapi_files())

    def test_already_bound_id_is_refused_and_nothing_new_is_written(self):
        session = str(uuid.uuid4())
        self.assertEqual(0, self.run_launcher('-FreshSessionId', session,
                                              '-CredentialsDir', str(self.credentials),
                                              '-PrepareOnly').returncode)
        before = self.dpapi_files()
        result = self.run_launcher('-FreshSessionId', session, '-CredentialsDir', str(self.credentials),
                                   '-PrepareOnly')
        self.assertNotEqual(0, result.returncode)
        self.assertIn('Nothing was launched', result.stderr)
        self.assertEqual(before, self.dpapi_files())

    def test_fresh_session_rejects_bad_input_before_provisioning(self):
        session = str(uuid.uuid4())
        cases = [
            (['-FreshSessionId', 'not-a-uuid', '-CredentialsDir', str(self.credentials), '-PrepareOnly'],
             'must be a UUID'),
            (['-FreshSessionId', session, '-PrepareOnly'], 'needs -CredentialsDir'),
            (['-FreshSessionId', session, '-ClaudeSessionId', session,
              '-CredentialsDir', str(self.credentials), '-PrepareOnly'], 'not both'),
        ]
        for args, expected in cases:
            with self.subTest(expected=expected):
                result = self.run_launcher(*args)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stderr)
                self.assertFalse(self.bound(session))
        self.assertEqual([], self.dpapi_files())


if __name__ == '__main__':
    unittest.main()
