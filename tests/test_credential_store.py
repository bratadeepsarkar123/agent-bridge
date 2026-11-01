"""Synthetic secrets only; real DPAPI tests run on Windows in temporary files."""
from contextlib import closing, redirect_stderr, redirect_stdout
import ctypes
import hashlib
import io
from pathlib import Path
import secrets
import sqlite3
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, mock_open, patch

import credential_store as store
from broker import Broker


def token():
    return secrets.token_urlsafe(32).encode('ascii')


class CodecValidationTests(unittest.TestCase):
    def test_non_windows_fails_closed_without_native_loading_or_disk_writes(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(store.sys, 'platform', 'linux'), \
                patch.object(store, '_load_api') as load:
            path = Path(directory) / 'generation'
            operations = [lambda: store.protect_token(token()), lambda: store.unprotect_token(b'anything'),
                          lambda: store.create_generation(path, token()), lambda: store.read_generation(path),
                          lambda: store.provision_initial_profile(Mock(), 'configured', directory)]
            for operation in operations:
                with self.assertRaises(store.CredentialStoreError):
                    operation()
            load.assert_not_called()
            self.assertFalse(path.exists())

    def test_invalid_tokens_and_oversized_blobs_never_reach_native_code(self):
        with patch.object(store.sys, 'platform', 'win32'), patch.object(store, '_load_api') as load:
            for value in (None, 'x' * 43, bytearray(b'x' * 43), b'', b'x' * 31, b'x' * 513,
                          b'x' * 1000000, b' ' * 43, b'x' * 42 + b'\n', b'\xff' * 43):
                with self.assertRaises(store.CredentialStoreError):
                    store.protect_token(value)
            for value in (None, '', bytearray(b'bad'), b'', store._FILE_MAGIC,
                          b'x' * 100, store._FILE_MAGIC + b'x' * store.MAX_FILE_BYTES):
                with self.assertRaises(store.CredentialStoreError):
                    store.unprotect_token(value)
            load.assert_not_called()

    def fake_api(self, output, *, success=True, reported_size=None, free_failure=False):
        allocation = ctypes.create_string_buffer(output)
        calls, freed = [], []
        def operation(incoming, description, entropy, reserved, prompt, flags, outgoing):
            self.assertIsNone(description)
            self.assertIsNone(entropy)
            self.assertIsNone(reserved)
            self.assertIsNone(prompt)
            self.assertEqual(1, flags)  # UI forbidden only; never machine scope.
            calls.append(ctypes.cast(incoming, ctypes.POINTER(store._DataBlob)).contents)
            target = ctypes.cast(outgoing, ctypes.POINTER(store._DataBlob)).contents
            target.cbData = len(output) if reported_size is None else reported_size
            target.pbData = ctypes.cast(allocation, ctypes.POINTER(ctypes.c_ubyte))
            return success
        def free(pointer):
            freed.append(pointer.value)
            return pointer.value if free_failure else None
        api = SimpleNamespace(CryptProtectData=operation, CryptUnprotectData=operation)
        return (api, SimpleNamespace(LocalFree=free)), allocation, calls, freed

    def test_ui_forbidden_and_localfree_on_success(self):
        value = token()
        api, allocation, calls, freed = self.fake_api(b'encrypted')
        with patch.object(store.sys, 'platform', 'win32'), patch.object(store, '_load_api', return_value=api):
            self.assertEqual(store._FILE_MAGIC + b'encrypted', store.protect_token(value))
        self.assertEqual(1, len(calls))
        self.assertEqual(1, len(freed))
        self.assertEqual(b'\0' * len(b'encrypted'), allocation.raw[:-1])

    def test_localfree_on_native_failure_invalid_size_and_validation_failure(self):
        for output, success, size in [(b'native output', False, None), (b'x', True, 0),
                                       (b'x', True, store.MAX_CIPHERTEXT_BYTES + 1),
                                       (b'invalid token envelope', True, None)]:
            api, allocation, calls, freed = self.fake_api(output, success=success, reported_size=size)
            with patch.object(store.sys, 'platform', 'win32'), \
                    patch.object(store, '_load_api', return_value=api):
                with self.assertRaises(store.CredentialStoreError):
                    store.unprotect_token(store._FILE_MAGIC + b'synthetic')
            self.assertEqual(1, len(freed))

    def test_localfree_failure_does_not_return_credential(self):
        api, allocation, calls, freed = self.fake_api(b'encrypted', free_failure=True)
        with patch.object(store.sys, 'platform', 'win32'), patch.object(store, '_load_api', return_value=api):
            with self.assertRaises(store.CredentialStoreError):
                store.protect_token(token())
        self.assertEqual(1, len(freed))

    def test_decrypted_token_envelope_and_digest_are_strict(self):
        value = token()
        invalid = [store._PAYLOAD_MAGIC + value + b'\0' * 32,
                   store._PAYLOAD_MAGIC + b'x' * 31 + hashlib.sha256(b'x' * 31).digest(),
                   store._PAYLOAD_MAGIC + b'x' * 513 + hashlib.sha256(b'x' * 513).digest(),
                   b'wrong version' + value + hashlib.sha256(value).digest()]
        with patch.object(store.sys, 'platform', 'win32'):
            for payload in invalid:
                with patch.object(store, '_transform', return_value=payload), \
                        self.assertRaises(store.CredentialStoreError):
                    store.unprotect_token(store._FILE_MAGIC + b'synthetic')

    def test_bounded_read_even_if_file_grows_after_stat(self):
        opener = mock_open(read_data=store._FILE_MAGIC + b'x' * store.MAX_FILE_BYTES)
        info = SimpleNamespace(st_mode=0o100600, st_size=1)
        with patch.object(store.sys, 'platform', 'win32'), patch('builtins.open', opener), \
                patch.object(store.os, 'fstat', return_value=info), patch.object(store, '_load_api') as load:
            with self.assertRaises(store.CredentialStoreError):
                store.read_generation('synthetic-path')
            opener().read.assert_called_once_with(store.MAX_FILE_BYTES + 1)
            load.assert_not_called()

    def test_native_exception_is_sanitized_and_still_releases_output(self):
        value = token()
        api, allocation, calls, freed = self.fake_api(b'allocated')
        original = api[0].CryptProtectData
        def fail(*args):
            original(*args)
            raise OSError(value.decode())
        api[0].CryptProtectData = fail
        with patch.object(store.sys, 'platform', 'win32'), patch.object(store, '_load_api', return_value=api):
            with self.assertRaises(store.CredentialStoreError) as error:
                store.protect_token(value)
        self.assertEqual('DPAPI operation failed', str(error.exception))
        self.assertEqual(1, len(freed))

    def test_missing_native_output_fails_closed(self):
        free = Mock(return_value=None)
        api = (SimpleNamespace(CryptProtectData=Mock(return_value=1)), SimpleNamespace(LocalFree=free))
        with patch.object(store.sys, 'platform', 'win32'), patch.object(store, '_load_api', return_value=api):
            with self.assertRaises(store.CredentialStoreError):
                store.protect_token(token())
        free.assert_not_called()


@unittest.skipUnless(sys.platform == 'win32', 'Real user-bound DPAPI requires Windows')
class WindowsCredentialStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'generation.dpapi'
        self.secret = token()

    def test_explicit_initial_profile_file_verification_and_hash_only_binding(self):
        broker = Broker(Path(self.temp.name) / 'isolated.sqlite3')
        with patch.object(store.secrets, 'token_urlsafe', return_value=self.secret.decode()), \
                redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()) as err:
            result = store.provision_initial_profile(broker, 'configured-profile', self.temp.name)
        self.assertEqual('', out.getvalue() + err.getvalue())
        self.assertEqual({'id', 'protected', 'credential_file'}, set(result))
        self.assertEqual('configured-profile', result['id'])
        self.assertTrue(result['protected'])
        self.assertNotIn(self.secret.decode(), str(result))
        path = Path(result['credential_file'])
        self.assertEqual(self.secret, store.read_generation(path))
        self.assertNotIn(self.secret, path.read_bytes())
        self.assertTrue(broker.credential_is_valid(self.secret.decode()))
        self.assertEqual([], broker.sessions()['sessions'])
        with closing(broker.connect()) as db:
            self.assertNotIn(self.secret.decode(), '\n'.join(db.iterdump()))
        previous = path.read_bytes()
        with self.assertRaises(store.CredentialStoreError):
            store.provision_initial_profile(broker, 'configured-profile', self.temp.name)
        self.assertEqual(previous, path.read_bytes())
        self.assertTrue(broker.credential_is_valid(self.secret.decode()))
        self.assertEqual(2, len(list(Path(self.temp.name).glob('*.dpapi'))))

    def test_initial_profile_verification_failure_never_binds(self):
        broker = Broker(Path(self.temp.name) / 'isolated.sqlite3')
        for invalid in (token(), b'short'):
            with patch.object(store, 'read_generation', return_value=invalid), \
                    patch.object(broker, 'provision_session_credential') as bind:
                with self.assertRaises(store.CredentialStoreError):
                    store.provision_initial_profile(broker, 'unbound', self.temp.name)
                bind.assert_not_called()
        with patch.object(store, 'read_generation', side_effect=store.CredentialStoreError('Invalid encrypted credential')), \
                patch.object(broker, 'provision_session_credential') as bind:
            with self.assertRaises(store.CredentialStoreError):
                store.provision_initial_profile(broker, 'unbound', self.temp.name)
            bind.assert_not_called()
        self.assertEqual(3, len(list(Path(self.temp.name).glob('*.dpapi'))))
        with closing(broker.connect()) as db:
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0])

    def test_initial_profile_bad_input_and_create_failure_do_not_bind(self):
        broker = Broker(Path(self.temp.name) / 'isolated.sqlite3')
        for profile, directory in [('../invalid', self.temp.name), ('unbound', Path(self.temp.name) / 'missing')]:
            with self.assertRaises(store.CredentialStoreError):
                store.provision_initial_profile(broker, profile, directory)
        with patch.object(store, 'create_generation', side_effect=store.GenerationExistsError('Credential generation already exists')), \
                patch.object(broker, 'provision_session_credential') as bind:
            with self.assertRaises(store.GenerationExistsError):
                store.provision_initial_profile(broker, 'unbound', self.temp.name)
            bind.assert_not_called()
        self.assertEqual([], list(Path(self.temp.name).glob('*.dpapi')))
        with closing(broker.connect()) as db:
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0])

    def test_initial_profile_binding_failure_leaves_only_verified_encrypted_orphan(self):
        broker = Broker(Path(self.temp.name) / 'isolated.sqlite3')
        with patch.object(broker, 'provision_session_credential', side_effect=sqlite3.OperationalError(self.secret.decode())):
            with self.assertRaises(store.CredentialStoreError) as error:
                store.provision_initial_profile(broker, 'unbound', self.temp.name)
        self.assertEqual('Could not provision initial profile', str(error.exception))
        files = list(Path(self.temp.name).glob('*.dpapi'))
        self.assertEqual(1, len(files))
        value = store.read_generation(files[0])
        self.assertNotIn(value, files[0].read_bytes())
        with closing(broker.connect()) as db:
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0])

    def test_initial_profile_concurrent_provisioners_leave_one_binding_and_orphan(self):
        from concurrent.futures import ThreadPoolExecutor
        db_path = Path(self.temp.name) / 'isolated.sqlite3'
        brokers = [Broker(db_path), Broker(db_path)]
        gate = threading.Barrier(2)
        original_read = store.read_generation
        def verified_together(path):
            value = original_read(path)
            gate.wait(timeout=3)  # Both files verified before either binding.
            return value
        def attempt(index):
            try:
                return store.provision_initial_profile(brokers[index], 'one-profile', self.temp.name)
            except store.CredentialStoreError:
                return None
        with patch.object(store, 'read_generation', side_effect=verified_together), \
                ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, range(2)))
        winners = [result for result in results if result is not None]
        self.assertEqual(1, len(winners))
        files = list(Path(self.temp.name).glob('*.dpapi'))
        self.assertEqual(2, len(files))
        values = [store.read_generation(path) for path in files]
        self.assertNotEqual(values[0], values[1])
        for path, value in zip(files, values):
            self.assertNotIn(value, path.read_bytes())
            self.assertEqual(path == Path(winners[0]['credential_file']),
                             brokers[0].credential_is_valid(value.decode()))
        with closing(brokers[0].connect()) as db:
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0])

    def test_real_roundtrip_including_token_size_boundaries(self):
        for size in (32, 43, 512):
            value = secrets.token_urlsafe(512).encode('ascii')[:size]
            blob = store.protect_token(value)
            self.assertNotIn(value, blob)
            self.assertEqual(value, store.unprotect_token(blob))

    def test_create_only_encrypted_file_roundtrip_and_no_output(self):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            self.assertIsNone(store.create_generation(self.path, self.secret))
            self.assertEqual(self.secret, store.read_generation(self.path))
        self.assertEqual('', out.getvalue() + err.getvalue())
        encrypted = self.path.read_bytes()
        self.assertNotIn(self.secret, encrypted)
        self.assertEqual([self.path], list(Path(self.temp.name).iterdir()))
        with self.assertRaises(store.GenerationExistsError):
            store.create_generation(self.path, token())
        self.assertEqual(encrypted, self.path.read_bytes())

    def test_corruption_truncation_and_plaintext_fail_closed(self):
        blob = store.protect_token(self.secret)
        damaged = bytearray(blob)
        damaged[-1] ^= 0xff
        for value in (bytes(damaged), blob[:-1], blob[:len(store._FILE_MAGIC) + 1],
                      b'wrong version' + blob, self.secret):
            self.path.write_bytes(value)  # Synthetic corruption fixture only.
            with self.assertRaises(store.CredentialStoreError) as error:
                store.read_generation(self.path)
            self.assertNotIn(self.secret.decode(), str(error.exception))

    def test_bounded_read_rejects_oversized_and_missing_files(self):
        self.path.write_bytes(b'x' * (store.MAX_FILE_BYTES + 1))
        with patch.object(store, '_transform') as transform:
            with self.assertRaises(store.CredentialStoreError):
                store.read_generation(self.path)
            transform.assert_not_called()
        with self.assertRaises(store.CredentialStoreError):
            store.read_generation(self.path.with_name('missing'))
        self.path.write_bytes(b'')
        with self.assertRaises(store.CredentialStoreError):
            store.read_generation(self.path)

    def test_failure_does_not_create_plaintext_or_fallback(self):
        with patch.object(store, '_load_api', side_effect=store.CredentialStoreError('DPAPI unavailable')):
            with self.assertRaises(store.CredentialStoreError):
                store.create_generation(self.path, self.secret)
        self.assertFalse(self.path.exists())
        with patch.object(store.os, 'fsync', side_effect=OSError(self.secret.decode())):
            with self.assertRaises(store.CredentialStoreError) as error:
                store.create_generation(self.path, self.secret)
        self.assertNotIn(self.secret.decode(), str(error.exception))
        self.assertNotIn(self.secret, self.path.read_bytes())
        with self.assertRaises(store.GenerationExistsError):
            store.create_generation(self.path, token())

    def test_exclusive_create_under_contention(self):
        import concurrent.futures
        def attempt():
            try:
                store.create_generation(self.path, self.secret)
                return 'created'
            except store.GenerationExistsError:
                return 'exists'
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: attempt(), range(4)))
        self.assertEqual(1, results.count('created'))
        self.assertEqual(3, results.count('exists'))
        self.assertEqual(self.secret, store.read_generation(self.path))


if __name__ == '__main__':
    unittest.main()
