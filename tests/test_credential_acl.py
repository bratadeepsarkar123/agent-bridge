"""Windows ACL enforcement for credential directories and files.

Windows-only. Every path is a TemporaryDirectory path; no live broker, database,
or credential is touched. Principals are read through real icacls output.
"""
import os
from pathlib import Path
import tempfile
import unittest

import bridge_backup
import credential_store
from broker import Broker


@unittest.skipUnless(os.name == 'nt', 'Windows ACLs and DPAPI are Windows-only')
class CredentialAclTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.addCleanup(self.folder.cleanup)

    def test_private_directory_and_file_verify(self):
        directory = self.root / 'private'
        directory.mkdir()
        credential_store.apply_private_acl(directory, directory=True)
        credential_store.verify_private_acl(directory)
        file_path = directory / 'generation.dpapi'
        file_path.write_bytes(b'placeholder')
        credential_store.apply_private_acl(file_path, directory=False)
        credential_store.verify_private_acl(file_path)

    def test_extra_principal_fails_verification(self):
        directory = self.root / 'shared'
        directory.mkdir()
        credential_store.apply_private_acl(directory, directory=True)
        # Administrators (S-1-5-32-544) is not the current account or SYSTEM.
        result = credential_store._run_icacls([directory, '/grant', '*S-1-5-32-544:(OI)(CI)R'])
        self.assertEqual(0, result.returncode)
        with self.assertRaises(credential_store.CredentialStoreError):
            credential_store.verify_private_acl(directory)

    def test_inherited_broad_acl_fails_verification(self):
        # A fresh temp directory inherits parent ACEs naming other principals.
        directory = self.root / 'inherited'
        directory.mkdir()
        with self.assertRaises(credential_store.CredentialStoreError):
            credential_store.verify_private_acl(directory)

    def test_provisioning_refuses_non_private_directory_and_writes_nothing(self):
        directory = self.root / 'inherited'
        directory.mkdir()
        broker = Broker(self.root / 'broker.sqlite3')
        with self.assertRaises(credential_store.CredentialStoreError):
            credential_store.provision_initial_profile(
                broker, '123e4567-e89b-12d3-a456-426614174000', directory)
        self.assertEqual([], list(directory.glob('*.dpapi')))

    def test_make_set_creates_private_set_and_credentials_directories(self):
        database = self.root / 'broker.sqlite3'
        state = self.root / 'broker-session.json'
        creds = self.root / 'creds'
        creds.mkdir()
        credential_store.apply_private_acl(creds, directory=True)
        broker = Broker(database)
        credential_store.provision_initial_profile(
            broker, '123e4567-e89b-12d3-a456-426614174000', creds)
        set_dir = self.root / 'set-1'
        bridge_backup.make_set(database, state, creds, set_dir)
        credential_store.verify_private_acl(set_dir)
        credential_store.verify_private_acl(set_dir / bridge_backup.CREDENTIALS)

    def test_restore_refuses_existing_credentials_directory(self):
        database = self.root / 'broker.sqlite3'
        state = self.root / 'broker-session.json'
        creds = self.root / 'creds'
        creds.mkdir()
        credential_store.apply_private_acl(creds, directory=True)
        broker = Broker(database)
        credential_store.provision_initial_profile(
            broker, '123e4567-e89b-12d3-a456-426614174000', creds)
        manifest = bridge_backup.make_set(database, state, creds, self.root / 'set-1')
        existing = self.root / 'already-here'
        existing.mkdir()
        with self.assertRaises(bridge_backup.BundleError):
            bridge_backup.restore_set(self.root / 'set-1', self.root / 'restored.sqlite3', existing,
                                      expected_database_sha256=manifest['database']['sha256'])
        self.assertFalse((self.root / 'restored.sqlite3').exists())


if __name__ == '__main__':
    unittest.main()
