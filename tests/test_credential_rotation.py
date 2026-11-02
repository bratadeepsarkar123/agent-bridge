"""Credential rotation rehearsal using real DPAPI, private ACLs, and temp state only."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import bridge_backup
import credential_store as store
import provision_profile
from broker import Broker


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LIVE_CREDENTIALS = Path(r'C:\Users\brata\Downloads\claude\agent-bridge-experiment\credentials')
POWERSHELL = Path(os.environ.get('SystemRoot', r'C:\Windows')) / 'System32' / 'WindowsPowerShell' / 'v1.0' / 'powershell.exe'


class SimulatedCrash(RuntimeError):
    pass


@unittest.skipUnless(sys.platform == 'win32', 'Real user-bound DPAPI and NTFS ACLs require Windows')
class CredentialRotationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.database = self.root / 'broker.sqlite3'
        self.state = self.root / 'broker-session.json'
        self.credentials = self.root / 'credentials'
        window_start = datetime.now(timezone.utc) - timedelta(minutes=1)
        window_end = window_start + timedelta(hours=2)
        self.restart_window = window_start.isoformat() + '/' + window_end.isoformat()
        self.assert_safe_paths(self.root, self.database, self.state, self.credentials)
        store.prepare_credentials_directory(self.credentials)
        self.addCleanup(self.reset_temp_acl)
        self.broker = Broker(self.database)
        self.assert_safe_paths(self.database, self.credentials)

    def reset_temp_acl(self):
        if self.root.exists():
            try:
                store._run_icacls([self.root, '/reset', '/T', '/C'])
            except store.CredentialStoreError:
                pass

    def assert_safe_paths(self, *paths):
        forbidden = (REPOSITORY_ROOT.resolve(), LIVE_CREDENTIALS.resolve())
        for path in paths:
            candidate = Path(path).resolve()
            lowered = str(candidate).casefold()
            for base in forbidden:
                base_lowered = str(base).casefold()
                self.assertFalse(lowered == base_lowered or
                                 lowered.startswith(base_lowered + os.sep.casefold()),
                                 'test path resolved under a protected live location')

    def add_bound_profile(self, profile='alpha'):
        self.broker.register({'id': profile, 'kind': 'claude', 'opt_in': True})
        result = store.provision_initial_profile(self.broker, profile, self.credentials)
        path = Path(result['credential_file'])
        self.assert_safe_paths(path)
        token = store.read_generation(path).decode('ascii')
        return token, path

    def cli(self, *args):
        from contextlib import redirect_stderr, redirect_stdout
        import io
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = provision_profile.main(list(args))
        return code, out.getvalue(), err.getvalue()

    def start_args(self, profile='alpha'):
        return ['rotate-start', profile, '--credentials-dir', str(self.credentials),
                '--database', str(self.database), '--state', str(self.state), '--client-stopped',
                '--restart-window', self.restart_window, '--retention-days', '30']

    def status_args(self, profile='alpha'):
        return ['rotate-status', profile, '--credentials-dir', str(self.credentials),
                '--database', str(self.database)]

    def resolve_args(self, profile='alpha', outcome='verified', *extra):
        return ['rotate-resolve', profile, '--credentials-dir', str(self.credentials),
                '--database', str(self.database), '--outcome', outcome, *extra]

    def table_snapshot(self):
        with sqlite3.connect(self.database) as db:
            result = {}
            for table in ('session_credentials', 'credential_rotations', 'retired_credentials', 'sessions'):
                result[table] = tuple(tuple(row) for row in db.execute(
                    'SELECT * FROM ' + table + ' ORDER BY rowid'))
        return self.database.read_bytes(), result

    def file_snapshot(self):
        paths = sorted(p for p in self.credentials.rglob('*') if p.is_file())
        self.assert_safe_paths(*paths)
        return tuple((p.relative_to(self.credentials).as_posix(), p.read_bytes()) for p in paths)

    def test_01_happy_rotation_retires_old_hash_and_file(self):
        old_token, old_path = self.add_bound_profile()
        code, out, err = self.cli(*self.start_args())
        self.assertEqual(0, code, err)
        started = json.loads(out)
        self.assertEqual('committed', started['state'])
        new_path = self.credentials / started['new_file']
        self.assert_safe_paths(new_path)
        new_token = store.read_generation(new_path).decode('ascii')
        self.assertNotIn(new_token, out + err)
        self.assertEqual(hashlib.sha256(new_token.encode()).hexdigest(),
                         self.broker.binding_for_profile('alpha'))
        self.assertNotEqual(old_token, new_token)
        committed_inventory = store.inventory_credential_files(self.broker, self.credentials)
        self.assertIn({'file': new_path.name, 'status': 'rotating', 'profile': 'alpha'},
                      committed_inventory)
        with sqlite3.connect(self.database) as db:
            self.assertIsNotNone(db.execute(
                'SELECT 1 FROM retired_credentials WHERE token_sha256=?',
                (hashlib.sha256(old_token.encode()).hexdigest(),)).fetchone())
        with self.assertRaises(PermissionError):
            with self.broker.connect() as db:
                self.broker._authorize(db, 'alpha', old_token)

        code, _, err = self.cli(*self.resolve_args('alpha', 'client-relaunched',
                                                   '--client-restart-passed'))
        self.assertEqual(0, code, err)
        code, out, err = self.cli(*self.resolve_args('alpha', 'verified', '--client-check-passed'))
        self.assertEqual(0, code, err)
        self.assertEqual('verified', json.loads(out)['state'])
        self.assertFalse(old_path.exists())
        retired_path = self.credentials / 'retired' / old_path.name
        self.assertTrue(retired_path.is_file())
        self.assert_safe_paths(retired_path)
        inventory = store.inventory_credential_files(self.broker, self.credentials)
        self.assertIn({'file': 'retired/' + old_path.name, 'status': 'retired', 'profile': 'alpha'}, inventory)
        retired = self.broker.retired_credential(hashlib.sha256(old_token.encode()).hexdigest())
        self.assertEqual(30, retired['retention_days'])
        self.assertGreater(retired['retain_until'], retired['retired_at'])

    def test_02_crash_boundaries_report_state_and_never_generate_a_second_token(self):
        cases = [
            ('prepared', 'prepared', 'resolve-abandoned', 0),
            ('file_written', 'file_written', 'resolve-abandoned', 1),
            ('committed', 'committed', 'relaunch-client', 1),
        ]
        for boundary, expected_state, expected_next, new_file_count in cases:
            with self.subTest(boundary=boundary):
                profile = 'p-' + uuid.uuid4().hex[:12]
                _, _ = self.add_bound_profile(profile)
                generated = []
                real_generate = store.secrets.token_urlsafe

                def generate(length):
                    generated.append(True)
                    return real_generate(length)

                def crash(at):
                    if at == boundary:
                        raise SimulatedCrash(at)

                with patch.object(store.secrets, 'token_urlsafe', side_effect=generate):
                    with self.assertRaises(SimulatedCrash):
                        store.rotate_profile_credential(
                            self.broker, profile, self.credentials,
                            restart_window=self.restart_window, retention_days=30,
                            _failure_hook=crash)
                    self.assertEqual(new_file_count, len(generated))
                    code, out, err = self.cli(*self.status_args(profile))
                    self.assertEqual(0, code, err)
                    status = json.loads(out)
                    self.assertEqual(expected_state, status['rotation']['state'])
                    self.assertEqual(expected_next, status['next_step'])
                    if boundary == 'file_written':
                        rotating_files = [entry for entry in status['files']
                                          if entry['status'] == 'rotating']
                        self.assertEqual(1, len(rotating_files))
                    self.assertEqual(new_file_count, len(generated))
                row = self.broker.rotation_for_profile(profile)
                if expected_state == 'committed':
                    self.broker.mark_rotation_client_relaunched(row['id'])
                    store.verify_and_retire_old_generation(self.broker, profile, self.credentials,
                                                           client_check_passed=True)
                else:
                    code, _, err = self.cli(*self.resolve_args(profile, 'abandoned'))
                    self.assertEqual(0, code, err)

        # Step 4 to 5: the launch has been recorded, but verification has not.
        profile = 'after-launch'
        self.add_bound_profile(profile)
        started = store.rotate_profile_credential(self.broker, profile, self.credentials,
                                                  restart_window=self.restart_window,
                                                  retention_days=30)
        mark_relaunched = self.broker.mark_rotation_client_relaunched
        with patch.object(self.broker, 'mark_rotation_client_relaunched', side_effect=lambda ident: (
                mark_relaunched(ident), (_ for _ in ()).throw(SimulatedCrash()))[0]):
            with self.assertRaises(SimulatedCrash):
                self.broker.mark_rotation_client_relaunched(started['rotation_id'])
        code, out, err = self.cli(*self.status_args(profile))
        self.assertEqual(0, code, err)
        self.assertEqual('verify-client', json.loads(out)['next_step'])

        # Step 5 to 6: verified in the journal; the old file remains until retirement.
        rotation = self.broker.rotation_for_profile(profile)
        before = self.file_snapshot()
        with patch.object(self.broker, 'mark_rotation_verified', side_effect=lambda ident: (
                Broker(self.database).mark_rotation_verified(ident), (_ for _ in ()).throw(SimulatedCrash()))[0]):
            with self.assertRaises(SimulatedCrash):
                self.broker.mark_rotation_verified(rotation['id'])
        self.assertEqual('retire-old-file',
                         json.loads(self.cli(*self.status_args(profile))[1])['next_step'])
        self.assertEqual(before, self.file_snapshot())
        store.verify_and_retire_old_generation(self.broker, profile, self.credentials,
                                               client_check_passed=True)

    def test_03_commit_exception_uses_read_back_and_unsure_never_changes_files(self):
        old_token, _ = self.add_bound_profile()
        real_commit = self.broker.commit_credential_rotation
        def committed_then_raise(*args):
            real_commit(*args)
            raise RuntimeError('post-commit')

        with patch.object(Broker, 'commit_credential_rotation', side_effect=committed_then_raise):
            code, out, err = self.cli(*self.start_args())
        self.assertEqual(0, code, err)
        self.assertEqual('committed', json.loads(out)['state'])
        self.assertNotEqual(hashlib.sha256(old_token.encode()).hexdigest(),
                            self.broker.binding_for_profile('alpha'))
        alpha_rotation = self.broker.rotation_for_profile('alpha')
        self.broker.mark_rotation_client_relaunched(alpha_rotation['id'])
        store.verify_and_retire_old_generation(self.broker, 'alpha', self.credentials,
                                               client_check_passed=True)

        # Old hash still bound: mark abandoned and keep the generated orphan.
        old_again, _ = self.add_bound_profile('beta')
        files_before_beta = len(list(self.credentials.glob('credential-*.dpapi')))
        with patch.object(Broker, 'commit_credential_rotation', side_effect=RuntimeError('before commit')):
            code, _, _ = self.cli(*self.start_args('beta'))
        self.assertEqual(1, code)
        self.assertEqual(hashlib.sha256(old_again.encode()).hexdigest(),
                         self.broker.binding_for_profile('beta'))
        self.assertEqual('abandoned', self.broker.rotation_for_profile('beta')['state'])
        self.assertEqual(files_before_beta + 1,
                         sum(1 for _ in self.credentials.glob('credential-*.dpapi')))

        # Neither old nor new hash is bound: code 3 and no post-read-back file change.
        self.add_bound_profile('gamma')
        snapshot_at_commit = []

        def remove_binding_then_raise(rotation_id, token):
            snapshot_at_commit.append(self.file_snapshot())
            with sqlite3.connect(self.database) as db:
                db.execute('DELETE FROM session_credentials WHERE session_id=?', ('gamma',))
            raise RuntimeError('ambiguous')

        with patch.object(Broker, 'commit_credential_rotation', side_effect=remove_binding_then_raise):
            code, _, err = self.cli(*self.start_args('gamma'))
        self.assertEqual(3, code)
        self.assertIn('Do not retry blindly', err)
        self.assertEqual(snapshot_at_commit[0], self.file_snapshot())

    def test_04_cas_mismatch_refuses_without_retiring_or_rewriting_files(self):
        old_token, _ = self.add_bound_profile()
        actual_commit = self.broker.commit_credential_rotation
        other_token = store.secrets.token_urlsafe(32)

        def change_then_commit(rotation_id, generated):
            self.broker.provision_session_credential('alpha', other_token, replace=True)
            return actual_commit(rotation_id, generated)

        with patch.object(Broker, 'commit_credential_rotation', side_effect=change_then_commit):
            code, _, err = self.cli(*self.start_args())
        self.assertEqual(1, code)
        self.assertIn('binding changed', err)
        self.assertEqual(hashlib.sha256(other_token.encode()).hexdigest(),
                         self.broker.binding_for_profile('alpha'))
        self.assertIsNone(self.broker.retired_credential(hashlib.sha256(old_token.encode()).hexdigest()))
        self.assertEqual('abandoned', self.broker.rotation_for_profile('alpha')['state'])

    def test_05_provisioning_and_rotation_refuse_a_retired_token(self):
        retired_token, _ = self.add_bound_profile()
        started = store.rotate_profile_credential(self.broker, 'alpha', self.credentials,
                                                  restart_window=self.restart_window,
                                                  retention_days=30)
        self.broker.mark_rotation_client_relaunched(started['rotation_id'])
        store.verify_and_retire_old_generation(self.broker, 'alpha', self.credentials,
                                               client_check_passed=True)
        with self.assertRaisesRegex(ValueError, 'retired credential'):
            self.broker.provision_session_credential('other', retired_token, replace=False)
        with self.assertRaisesRegex(ValueError, 'retired credential'):
            self.broker.load_shared_token(retired_token)

        self.broker.register({'id': 'new-profile', 'kind': 'claude', 'opt_in': True})
        before = self.file_snapshot()
        with patch.object(store.secrets, 'token_urlsafe', return_value=retired_token):
            with self.assertRaises(store.ProvisionError):
                store.provision_initial_profile(self.broker, 'new-profile', self.credentials)
        self.assertEqual(before, self.file_snapshot())

        before = self.file_snapshot()
        with patch.object(store.secrets, 'token_urlsafe', return_value=retired_token):
            with self.assertRaises(store.ProvisionError):
                store.rotate_profile_credential(self.broker, 'alpha', self.credentials,
                                                restart_window=self.restart_window,
                                                retention_days=30)
        self.assertEqual(before, self.file_snapshot())
        alpha_rotation = self.broker.rotation_for_profile('alpha')
        self.broker.abandon_credential_rotation(alpha_rotation['id'])

        _, beta_path = self.add_bound_profile('beta')
        beta_hash = self.broker.binding_for_profile('beta')
        rotation_id = uuid.uuid4().hex
        new_name = 'credential-' + uuid.uuid4().hex + '.dpapi'
        self.broker.begin_credential_rotation(rotation_id, 'beta', beta_hash, beta_path.name,
                                              new_name, self.restart_window, 30)
        retired_hash = hashlib.sha256(retired_token.encode()).hexdigest()
        self.broker.mark_rotation_file_written(rotation_id, retired_hash)
        with self.assertRaisesRegex(ValueError, 'retired credential'):
            self.broker.commit_credential_rotation(rotation_id, retired_token)
        self.assertEqual(beta_hash, self.broker.binding_for_profile('beta'))

    def test_06_truncated_generation_is_kept_unreadable_and_never_bound(self):
        self.add_bound_profile()

        def partial(path, _token):
            Path(path).write_bytes(b'partial-generation')
            raise store.CredentialStoreError('Could not persist credential generation')

        with patch.object(store, 'create_generation', side_effect=partial):
            with self.assertRaises(store.CredentialStoreError):
                store.rotate_profile_credential(self.broker, 'alpha', self.credentials,
                                                restart_window=self.restart_window,
                                                retention_days=30)
        status = self.broker.rotation_for_profile('alpha')
        self.assertEqual('prepared', status['state'])
        self.assertEqual(status['old_sha256'], self.broker.binding_for_profile('alpha'))
        inventory = store.inventory_credential_files(self.broker, self.credentials)
        self.assertIn({'file': status['new_file'], 'status': 'unreadable', 'profile': None}, inventory)
        self.assertTrue((self.credentials / status['new_file']).exists())

    def test_07_status_and_recover_are_read_only(self):
        self.add_bound_profile()
        with self.assertRaises(SimulatedCrash):
            store.rotate_profile_credential(self.broker, 'alpha', self.credentials,
                                            restart_window=self.restart_window,
                                            retention_days=30,
                                            _failure_hook=lambda at: (_ for _ in ()).throw(SimulatedCrash())
                                            if at == 'file_written' else None)
        before_files, before_rows = self.file_snapshot(), self.table_snapshot()
        status_code, _, status_err = self.cli(*self.status_args())
        recover_code, _, recover_err = self.cli('recover', '--credentials-dir', str(self.credentials),
                                                '--database', str(self.database))
        self.assertEqual(0, status_code, status_err)
        self.assertEqual(0, recover_code, recover_err)
        self.assertEqual(before_files, self.file_snapshot())
        self.assertEqual(before_rows, self.table_snapshot())

    def test_08_pre_rotation_backup_is_refused_after_its_hash_is_retired(self):
        old_token, _ = self.add_bound_profile()
        state = self.root / 'broker-session.json'
        backup = self.root / 'pre-rotation-set'
        self.assert_safe_paths(state, backup)
        manifest = bridge_backup.make_set(self.database, state, self.credentials, backup)
        started = store.rotate_profile_credential(self.broker, 'alpha', self.credentials,
                                                  restart_window=self.restart_window,
                                                  retention_days=30)
        self.broker.mark_rotation_client_relaunched(started['rotation_id'])
        store.verify_and_retire_old_generation(self.broker, 'alpha', self.credentials,
                                               client_check_passed=True)
        backup_db = backup / bridge_backup.DATABASE
        backup_creds = backup / bridge_backup.CREDENTIALS
        with self.assertRaisesRegex(bridge_backup.BundleError, 'retired credential warning'):
            bridge_backup.verify_pairing(backup_db, backup_creds, retired_database=self.database)
        target_db, target_creds = self.root / 'restore.sqlite3', self.root / 'restore-creds'
        with self.assertRaisesRegex(bridge_backup.BundleError, 'retired credential warning'):
            bridge_backup.restore_set(backup, target_db, target_creds,
                                      expected_database_sha256=manifest['database']['sha256'],
                                      retired_database=self.database)
        self.assertFalse(target_db.exists())
        self.assertFalse(target_creds.exists())
        self.assertNotEqual(self.broker.binding_for_profile('alpha'),
                            hashlib.sha256(old_token.encode()).hexdigest())

    def test_09_rotated_client_launch_uses_only_new_generation(self):
        if not POWERSHELL.exists():
            self.skipTest('Windows PowerShell is unavailable')
        session = str(uuid.uuid4())
        self.add_bound_profile(session)
        started = store.rotate_profile_credential(self.broker, session, self.credentials,
                                                  restart_window=self.restart_window,
                                                  retention_days=30)
        old_file = self.credentials / self.broker.rotation_for_profile(session)['old_file']
        new_file = self.credentials / started['new_file']
        fake_python = self.root / 'fake_client.py'
        fake_capture = self.root / 'fake-client-read.json'
        self.assert_safe_paths(old_file, new_file, fake_python, fake_capture)
        fake_python.write_text(
            'import json, os\n'
            'from pathlib import Path\n'
            'import credential_store\n'
            'p = Path(os.environ["BRIDGE_SESSION_CREDENTIAL_FILE"])\n'
            'credential_store.read_generation(p)\n'
            'Path(os.environ["ROTATION_FAKE_CAPTURE"]).write_text(json.dumps({"read": [p.name], "format": os.environ.get("BRIDGE_SESSION_CREDENTIAL_FORMAT")}))\n',
            encoding='utf-8')
        launcher = self.root / 'launch-claude.ps1'
        shutil.copy2(REPOSITORY_ROOT / 'launch-claude.ps1', launcher)
        for module in REPOSITORY_ROOT.glob('*.py'):
            shutil.copy2(module, self.root / module.name)
        (self.root / 'codex_client.py').write_text('import sys\nsys.exit(0)\n', encoding='utf-8')
        script = launcher.read_text(encoding='utf-8')
        real_call = "& 'C:\\Users\\brata\\.local\\bin\\claude.exe' @launchArgs"
        fake_call = "& $python -B (Join-Path $PSScriptRoot 'fake_client.py')"
        self.assertIn(real_call, script)
        process_query = ('$existing = Get-CimInstance Win32_Process -Filter "name = \'claude.exe\'" |\n'
                         '    Where-Object { $_.CommandLine -like "*$ClaudeSessionId*" }')
        self.assertIn(process_query, script)
        script = script.replace(real_call, fake_call).replace(process_query, '$existing = $null')
        launcher.write_text(script, encoding='utf-8')
        self.database = self.root / 'broker.sqlite3'
        self.broker = Broker(self.database)
        env = os.environ.copy()
        env['ROTATION_FAKE_CAPTURE'] = str(fake_capture)
        command = [str(POWERSHELL), '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(launcher),
                   '-RotatedSessionId', session, '-CredentialsDir', str(self.credentials),
                   '-RestartWindow', self.restart_window]
        result = subprocess.run(command, capture_output=True, text=True, timeout=60,
                                cwd=str(self.root), env=env)
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(fake_capture.read_text(encoding='utf-8'))
        self.assertEqual([new_file.name], report['read'])
        self.assertEqual('dpapi', report['format'])
        self.assertTrue(old_file.is_file())
        self.assertTrue(new_file.is_file())
        self.assertEqual([], [p for p in report['read'] if p == old_file.name])
        self.assertIsNotNone(self.broker.rotation_for_profile(session)['client_relaunched_at'])
        self.assertNotIn(store.read_generation(new_file).decode('ascii'), result.stdout + result.stderr)

    def test_10_cli_exit_codes_are_zero_one_two_and_three(self):
        self.add_bound_profile()
        code, _, err = self.cli(*self.status_args())
        self.assertEqual(0, code, err)
        code, _, _ = self.cli('rotate-status', '../invalid', '--credentials-dir', str(self.credentials),
                              '--database', str(self.database))
        self.assertEqual(1, code)
        code, _, _ = self.cli()
        self.assertEqual(2, code)
        with patch.object(store, 'rotate_profile_credential',
                          side_effect=store.ProvisionError('Rotation outcome is unsure. Do not retry blindly.',
                                                           unsure=True)):
            code, _, _ = self.cli(*self.start_args())
        self.assertEqual(3, code)


if __name__ == '__main__':
    unittest.main()
