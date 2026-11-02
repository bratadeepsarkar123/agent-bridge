"""One environment credential per process: refusal with fake environments only.

No broker, lock file, network call, or credential file is touched. The environment
is replaced with a dictionary, and native_worker's lock is patched out.
"""
import contextlib
import io
import os
from unittest.mock import patch
import unittest

from codex_client import SESSION_CREDENTIAL_FILE_ENV, ClientError, require_single_environment_profile
import native_worker

A = '123e4567-e89b-12d3-a456-426614174000'
B = '223e4567-e89b-12d3-a456-426614174000'
FAKE_CREDENTIAL_PATH = r'C:\temp\fake-credential.dpapi'


class RequireSingleEnvironmentProfileTests(unittest.TestCase):
    def test_one_profile_is_allowed_with_or_without_environment_credential(self):
        require_single_environment_profile([A], {})
        require_single_environment_profile([A], {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH})

    def test_repeated_same_profile_counts_once(self):
        require_single_environment_profile([A, A], {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH})

    def test_several_profiles_refused_when_environment_credential_is_set(self):
        with self.assertRaises(ClientError) as error:
            require_single_environment_profile([A, B], {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH})
        self.assertEqual('environment_credential_shared', error.exception.code)
        self.assertIn('multi_dispatcher.py', error.exception.message)
        self.assertIn('--credential SESSION_UUID=PATH', error.exception.message)
        self.assertNotIn(FAKE_CREDENTIAL_PATH, error.exception.message)

    def test_several_profiles_refused_without_environment_credential_for_one_profile_clients(self):
        with self.assertRaises(ClientError) as error:
            require_single_environment_profile([A, B], {})
        self.assertEqual('multiple_profiles_unsupported', error.exception.code)
        self.assertIn('multi_dispatcher.py', error.exception.message)

    def test_empty_environment_value_counts_as_no_environment_credential(self):
        with self.assertRaises(ClientError) as error:
            require_single_environment_profile([A, B], {SESSION_CREDENTIAL_FILE_ENV: ''})
        self.assertEqual('multiple_profiles_unsupported', error.exception.code)

    def test_default_reads_the_real_environment_only_when_none_is_given(self):
        with patch.dict(os.environ, {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH}, clear=False):
            with self.assertRaises(ClientError) as error:
                require_single_environment_profile([A, B])
        self.assertEqual('environment_credential_shared', error.exception.code)


class NativeWorkerMainRefusalTests(unittest.TestCase):
    def run_main(self, argv, environ):
        err = io.StringIO()
        with patch.dict(os.environ, environ, clear=True), patch.object(native_worker, 'acquire') as acquire:
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as exit_info:
                    native_worker.main(argv)
        return exit_info.exception.code, err.getvalue(), acquire

    def test_two_recipients_with_environment_credential_refused_before_lock(self):
        code, err, acquire = self.run_main(['--recipient', A, '--recipient', B],
                                           {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH})
        self.assertEqual(2, code)
        self.assertIn('environment credential', err)
        self.assertIn('multi_dispatcher.py', err)
        self.assertNotIn(FAKE_CREDENTIAL_PATH, err)
        acquire.assert_not_called()

    def test_two_recipients_without_environment_credential_refused_before_lock(self):
        code, err, acquire = self.run_main(['--recipient', A, '--recipient', B], {})
        self.assertEqual(2, code)
        self.assertIn('one profile per process', err)
        acquire.assert_not_called()

    def test_single_recipient_with_environment_credential_reaches_the_lock(self):
        with patch.dict(os.environ, {SESSION_CREDENTIAL_FILE_ENV: FAKE_CREDENTIAL_PATH}, clear=True), \
                patch.object(native_worker, 'acquire', side_effect=RuntimeError('lock held (test)')) as acquire:
            with self.assertRaises(SystemExit) as exit_info:
                native_worker.main(['--recipient', A])
        acquire.assert_called_once()
        self.assertEqual('lock held (test)', exit_info.exception.code)


if __name__ == '__main__':
    unittest.main()
