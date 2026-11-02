"""Provisioning CLI and broker admin operation: temp folders and real DPAPI only.

Every database, state file, and credential directory lives in a TemporaryDirectory.
No live broker database, broker state, or credential file is read or written.
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

import credential_store as store
from broker import Broker
import provision_profile


@unittest.skipUnless(sys.platform == 'win32', 'Real user-bound DPAPI requires Windows')
class ProvisionProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / 'isolated.sqlite3'
        self.state = self.root / 'missing-broker-session.json'  # no broker running
        self.credentials = self.root / 'credentials'
        Broker(self.database)  # create the schema in the temp database only

    def cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = provision_profile.main(list(args))
        return code, out.getvalue(), err.getvalue()

    def provision_args(self, profile, *extra):
        return ['provision', profile, '--credentials-dir', str(self.credentials),
                '--database', str(self.database), '--state', str(self.state), *extra]

    def bound(self, profile):
        return Broker(self.database).profile_status(profile)['bound']

    def binding_count(self):
        with closing(sqlite3.connect(self.database)) as db:
            return db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0]

    def dpapi_files(self):
        return sorted(p.name for p in self.credentials.glob('credential-*.dpapi'))

    def test_provision_success_creates_private_directory_and_binds_hash_only(self):
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(0, code, err)
        result = json.loads(out)
        self.assertEqual('alpha', result['id'])
        self.assertFalse(result['shared_token_checked'])  # no state file: nothing loaded
        self.assertTrue(self.bound('alpha'))
        store.verify_private_acl(self.credentials)
        self.assertEqual(1, len(self.dpapi_files()))
        path = Path(result['credential_file'])
        token = store.read_generation(path)
        self.assertNotIn(token, out.encode() + err.encode())
        with closing(sqlite3.connect(self.database)) as db:
            stored = db.execute('SELECT token_sha256 FROM session_credentials').fetchone()[0]
        self.assertNotIn(token.decode(), stored)
        self.assertEqual(1, self.binding_count())

    def test_broker_provision_profile_success_via_standalone_broker(self):
        broker = Broker(self.database)
        shared = secrets.token_urlsafe(32)
        result = broker.provision_profile('beta', self.make_dir(), active_shared_token=shared)
        self.assertEqual('beta', result['id'])
        self.assertTrue(self.bound('beta'))

    def make_dir(self):
        path = self.root / 'standalone-creds'
        store.prepare_credentials_directory(path)
        return str(path)

    def test_duplicate_profile_refused_without_writing_a_second_file(self):
        self.assertEqual(0, self.cli(*self.provision_args('alpha'))[0])
        before = self.dpapi_files()
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(1, code)
        self.assertIn('already has a credential', err)
        self.assertEqual(before, self.dpapi_files())
        self.assertEqual(1, self.binding_count())

    def test_collision_with_shared_token_refused_via_standalone_broker(self):
        shared = secrets.token_urlsafe(32)
        broker = Broker(self.database)  # standalone: no handler_for was ever built
        directory = self.make_dir()
        with patch('credential_store.secrets.token_urlsafe', return_value=shared):
            with self.assertRaises(store.CredentialStoreError) as error:
                broker.provision_profile('gamma', directory, active_shared_token=shared)
        self.assertIn('collides', str(error.exception))
        self.assertEqual([], list(Path(directory).glob('*.dpapi')))  # refused before any write
        self.assertFalse(self.bound('gamma'))
        self.assertEqual(0, self.binding_count())

    def test_collision_with_active_token_in_state_file_refused_by_cli(self):
        shared = secrets.token_urlsafe(32)
        # The recorded PID must be alive for the token to be checked (this process is).
        self.state.write_text(json.dumps({'port': 1, 'token': shared, 'pid': os.getpid()}),
                              encoding='utf-8')
        with patch('credential_store.secrets.token_urlsafe', return_value=shared):
            code, out, err = self.cli(*self.provision_args('gamma'))
        self.assertEqual(1, code)
        self.assertIn('collides', err)
        self.assertEqual([], self.dpapi_files())
        self.assertEqual(0, self.binding_count())

    def test_invalid_state_file_refuses_before_any_write(self):
        self.state.write_text('{not json', encoding='utf-8')
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(1, code)
        self.assertFalse(self.credentials.exists())
        self.assertEqual(0, self.binding_count())

    def test_existing_non_private_directory_refused_and_nothing_written(self):
        self.credentials.mkdir()
        # Administrators is not the current account or SYSTEM: the ACL is not private.
        store._run_icacls([self.credentials, '/grant', '*S-1-5-32-544:(OI)(CI)R'])
        code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(1, code)
        self.assertEqual([], self.dpapi_files())
        self.assertFalse(self.bound('alpha'))

    def test_invalid_profile_refused_before_directory_is_created(self):
        code, out, err = self.cli(*self.provision_args('../bad'))
        self.assertEqual(1, code)
        self.assertFalse(self.credentials.exists())

    def test_orphan_file_is_reported_and_not_deleted_or_bound(self):
        with patch.object(Broker, 'provision_session_credential',
                          side_effect=sqlite3.OperationalError('locked')):
            code, out, err = self.cli(*self.provision_args('alpha'))
        self.assertEqual(1, code)
        self.assertIn('kept as an orphan', err)
        self.assertFalse(self.bound('alpha'))
        orphans = self.dpapi_files()
        self.assertEqual(1, len(orphans))
        orphan_path = self.credentials / orphans[0]
        before_bytes = orphan_path.read_bytes()
        before_rows = self.binding_count()

        code, out, err = self.cli('recover', '--credentials-dir', str(self.credentials),
                                  '--database', str(self.database))
        self.assertEqual(0, code, err)
        report = json.loads(out)
        self.assertFalse(report['changed'])
        self.assertEqual([{'file': orphans[0], 'status': 'orphan', 'profile': None}], report['files'])
        self.assertEqual(before_bytes, orphan_path.read_bytes())  # never deleted or rewritten
        self.assertEqual(before_rows, self.binding_count())
        self.assertFalse(self.bound('alpha'))

    def test_recover_reports_bound_and_unreadable_files_without_changes(self):
        self.assertEqual(0, self.cli(*self.provision_args('alpha'))[0])
        bad = self.credentials / 'credential-broken.dpapi'
        bad.write_bytes(b'AB-DPAPI-v1\x00not-a-valid-blob')  # synthetic corruption fixture
        code, out, err = self.cli('recover', '--credentials-dir', str(self.credentials),
                                  '--database', str(self.database))
        self.assertEqual(0, code, err)
        entries = json.loads(out)['files']
        bound = [e for e in entries if e['status'] == 'bound']
        self.assertEqual(1, len(bound))
        self.assertEqual('alpha', bound[0]['profile'])
        broken = [e for e in entries if e['file'] == 'credential-broken.dpapi']
        self.assertEqual('unreadable', broken[0]['status'])
        self.assertTrue(bad.exists())

    def test_failure_after_commit_reports_unsure_and_keeps_binding(self):
        broker = Broker(self.database)
        directory = self.make_dir()
        with patch.object(Broker, '_notify_messages', side_effect=sqlite3.OperationalError('late')):
            with self.assertRaises(store.ProvisionError) as error:
                broker.provision_profile('delta', directory, active_shared_token=None)
        self.assertTrue(error.exception.unsure)
        self.assertIn('unsure', str(error.exception))
        self.assertIn('Do not retry blindly', str(error.exception))
        self.assertTrue(self.bound('delta'))
        self.assertEqual(1, len(list(Path(directory).glob('*.dpapi'))))

    def test_cli_failure_after_commit_exits_unsure_and_says_do_not_retry(self):
        with patch.object(Broker, '_notify_messages', side_effect=sqlite3.OperationalError('late')):
            code, out, err = self.cli(*self.provision_args('delta'))
        self.assertEqual(3, code)
        self.assertIn('unsure', err)
        self.assertIn('do not retry blindly', err.lower())
        self.assertTrue(self.bound('delta'))

    def test_failure_before_commit_reports_not_bound_not_unsure(self):
        broker = Broker(self.database)
        directory = self.make_dir()
        with patch.object(Broker, 'provision_session_credential',
                          side_effect=sqlite3.OperationalError('locked')):
            with self.assertRaises(store.ProvisionError) as error:
                broker.provision_profile('epsilon', directory, active_shared_token=None)
        self.assertFalse(error.exception.unsure)
        self.assertEqual('Could not provision initial profile', str(error.exception))
        self.assertFalse(self.bound('epsilon'))
        self.assertEqual(1, len(list(Path(directory).glob('*.dpapi'))))

    def test_check_reports_bound_state_without_secret(self):
        self.assertEqual(0, self.cli(*self.provision_args('alpha'))[0])
        code, out, err = self.cli('check', 'alpha', '--database', str(self.database))
        self.assertEqual(0, code, err)
        result = json.loads(out)
        self.assertEqual({'bound': True, 'id': 'alpha', 'opted_in': False}, {k: result[k] for k in ('bound', 'id', 'opted_in')})
        self.assertIn('NOT proof', result['note'])
        self.assertIn('thread-identity', result['note'])
        code, out, err = self.cli('check', 'nobody', '--database', str(self.database))
        result = json.loads(out)
        self.assertEqual({'bound': False, 'id': 'nobody', 'opted_in': False}, {k: result[k] for k in ('bound', 'id', 'opted_in')})

    def test_missing_database_is_refused_and_not_created(self):
        missing = self.root / 'absent.sqlite3'
        code, out, err = self.cli('check', 'alpha', '--database', str(missing))
        self.assertEqual(1, code)
        self.assertFalse(missing.exists())


if __name__ == '__main__':
    unittest.main()
