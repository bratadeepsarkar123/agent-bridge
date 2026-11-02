import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import adapter_scoped_guard as guard  # noqa: E402

A = '11111111-1111-4111-8111-111111111111'
B = '22222222-2222-4222-8222-222222222222'


class ScopedCredentialTests(unittest.TestCase):
    def test_unset_variable_is_not_scoped(self):
        self.assertFalse(guard.scoped_credential_set({}))

    def test_present_variable_is_scoped_even_when_empty(self):
        # claude_adapter treats any present value (including empty) as a scoped credential.
        self.assertTrue(guard.scoped_credential_set({guard.SCOPED_CREDENTIAL_FILE_ENV: ''}))
        self.assertTrue(guard.scoped_credential_set({guard.SCOPED_CREDENTIAL_FILE_ENV: 'x.dpapi'}))


class SwitchAllowedTests(unittest.TestCase):
    def test_same_or_missing_live_id_never_refused(self):
        scoped = {guard.SCOPED_CREDENTIAL_FILE_ENV: 'x.dpapi'}
        self.assertTrue(guard.switch_allowed(A, A, scoped))
        self.assertTrue(guard.switch_allowed(A, '', scoped))
        self.assertTrue(guard.switch_allowed(A, None, scoped))

    def test_switch_refused_with_scoped_credential(self):
        self.assertFalse(guard.switch_allowed(A, B, {guard.SCOPED_CREDENTIAL_FILE_ENV: 'x.dpapi'}))

    def test_switch_allowed_without_scoped_credential(self):
        self.assertTrue(guard.switch_allowed(A, B, {}))


class SwitchRefusalLogTests(unittest.TestCase):
    def test_refusal_is_written_once_per_live_id_and_keeps_current_id(self):
        stream = io.StringIO()
        log = guard.SwitchRefusalLog(stream)
        self.assertEqual(A, log.refuse(A, B))
        self.assertEqual(A, log.refuse(A, B))
        self.assertEqual(A, log.refuse(A, B))
        text = stream.getvalue()
        self.assertEqual(1, text.count('refused switch to ' + B))
        self.assertNotIn('token', text.lower())


if __name__ == '__main__':
    unittest.main()
