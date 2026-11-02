"""Gap fixes in provision_profile.py: bound shared-token refusal and broker PID liveness.

Temp folders only. Process liveness is faked where a dead or unknown PID is needed;
no process is started, stopped, or signalled.
"""
from contextlib import closing, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import secrets
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from broker import Broker
import provision_profile


@unittest.skipUnless(sys.platform == 'win32', 'Real user-bound DPAPI requires Windows')
class ProvisionGapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / 'isolated.sqlite3'
        self.state = self.root / 'broker-session.json'
        self.credentials = self.root / 'credentials'
        Broker(self.database)  # schema in the temp database only

    def cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = provision_profile.main(list(args))
        return code, out.getvalue(), err.getvalue()

    def provision_args(self, profile):
        return ['provision', profile, '--credentials-dir', str(self.credentials),
                '--database', str(self.database), '--state', str(self.state)]

    def write_state(self, token, pid):
        self.state.write_text(json.dumps({'port': 1, 'token': token, 'pid': pid}),
                              encoding='utf-8')

    def bind_token_to_profile(self, token):
        # Binds the hash of an existing token to a profile in the temp database only.
        Broker(self.database).provision_session_credential('owner', token)

    def bound(self, profile):
        return Broker(self.database).profile_status(profile)['bound']

    def binding_count(self):
        with closing(sqlite3.connect(self.database)) as db:
            return db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0]

    # GAP 1: the state token already belongs to a bound profile.

    def test_bound_shared_token_is_refused_cleanly_with_no_traceback_and_no_file(self):
        token = secrets.token_urlsafe(32)
        self.bind_token_to_profile(token)
        self.write_state(token, os.getpid())
        with patch('provision_profile.process_is_alive', return_value=True):
            code, out, err = self.cli(*self.provision_args('beta'))
        self.assertEqual(1, code, err)
        self.assertNotIn('Traceback', err)
        self.assertIn('Refused:', err)
        self.assertIn('already belongs to a bound profile', err)
        self.assertIn('exit code 1', err)
        self.assertEqual('', out)
        self.assertFalse(self.credentials.exists())  # refused before any directory or file
        self.assertFalse(self.bound('beta'))
        self.assertEqual(1, self.binding_count())  # only the original binding

    # GAP 2: the recorded broker PID may be stale.

    def test_stale_pid_reports_shared_token_not_checked_with_reason(self):
        token = secrets.token_urlsafe(32)
        self.write_state(token, 4242)
        with patch('provision_profile.process_is_alive', return_value=False) as alive:
            code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)
        alive.assert_called_once_with(4242)
        result = json.loads(out)
        self.assertFalse(result['shared_token_checked'])
        self.assertIn('not running', result['reason'])
        self.assertIn('not trusted', result['reason'])
        self.assertNotIn(token, out)
        self.assertTrue(self.bound('alpha'))

    def test_stale_pid_does_not_load_the_stored_token_into_the_collision_check(self):
        token = secrets.token_urlsafe(32)
        self.write_state(token, 4242)
        with patch('provision_profile.process_is_alive', return_value=False), \
                patch('credential_store.secrets.token_urlsafe', return_value=token):
            code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)  # the untrusted token is not loaded, so no collision
        self.assertFalse(json.loads(out)['shared_token_checked'])

    def test_live_pid_keeps_shared_token_checked_true_without_reason(self):
        token = secrets.token_urlsafe(32)
        self.write_state(token, os.getpid())
        with patch('provision_profile.process_is_alive', return_value=True) as alive:
            code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)
        alive.assert_called_once_with(os.getpid())
        result = json.loads(out)
        self.assertTrue(result['shared_token_checked'])
        self.assertNotIn('reason', result)
        self.assertTrue(self.bound('alpha'))

    def test_unknown_liveness_counts_as_alive_and_keeps_the_token_checked(self):
        token = secrets.token_urlsafe(32)
        self.write_state(token, 4242)
        with patch('provision_profile.process_is_alive', side_effect=OSError('unknown')):
            code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)
        self.assertTrue(json.loads(out)['shared_token_checked'])

    def test_absent_state_file_reports_reason(self):
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)
        result = json.loads(out)
        self.assertFalse(result['shared_token_checked'])
        self.assertIn('no broker state file', result['reason'])

    def test_state_without_valid_pid_is_refused_before_any_file_is_created(self):
        token = secrets.token_urlsafe(32)
        self.state.write_text(json.dumps({'port': 1, 'token': token}), encoding='utf-8')
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(1, code)
        self.assertIn('invalid', err)
        self.assertNotIn('Traceback', err)
        self.assertFalse(self.credentials.exists())
        self.assertFalse(self.bound('alpha'))


if __name__ == '__main__':
    unittest.main()
