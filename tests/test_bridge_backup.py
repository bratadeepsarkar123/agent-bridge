"""Paired backup set (database + DPAPI credentials): make, verify, restore to new paths.

Windows-only: uses real DPAPI under the current account. Every path is a
TemporaryDirectory path; no live broker, database, or credential is touched.
"""
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

import bridge_backup
import credential_store
from broker import Broker


@unittest.skipUnless(os.name == 'nt', 'DPAPI is Windows-only')
class PairedBackupSetTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.database = self.root / 'broker.sqlite3'
        self.state = self.root / 'broker-session.json'
        self.creds = self.root / 'creds'
        self.creds.mkdir()
        credential_store.apply_private_acl(self.creds, directory=True)
        self.broker = Broker(self.database)
        credential_store.provision_initial_profile(self.broker, 'alpha', self.creds)

    def tearDown(self):
        self.folder.cleanup()

    def make(self, name='set-1'):
        return bridge_backup.make_set(self.database, self.state, self.creds, self.root / name)

    def test_make_set_pairs_database_with_its_credential_file(self):
        manifest = self.make()
        self.assertEqual(1, manifest['bound_credentials'])
        self.assertEqual(1, len(manifest['credential_files']))
        self.assertTrue((self.root / 'set-1' / bridge_backup.MANIFEST).is_file())
        # The manifest never contains a secret or a credential hash.
        text = (self.root / 'set-1' / bridge_backup.MANIFEST).read_text(encoding='utf-8')
        self.assertNotIn('token_sha256', text)

    def test_make_set_refuses_missing_credential_and_leaves_no_usable_set(self):
        for path in self.creds.glob(bridge_backup.CREDENTIAL_GLOB):
            path.unlink()
        with self.assertRaises(bridge_backup.BundleError):
            self.make('set-missing')
        leftovers = [p for p in (self.root / 'set-missing').rglob('*') if p.is_file()] \
            if (self.root / 'set-missing').exists() else []
        self.assertEqual([], leftovers)

    def test_make_set_refuses_existing_directory(self):
        self.make('set-1')
        with self.assertRaises(bridge_backup.BundleError):
            self.make('set-1')

    def test_restore_with_confirmed_digest_restores_pair_to_new_paths(self):
        manifest = self.make()
        target_db = self.root / 'restored.sqlite3'
        target_creds = self.root / 'restored-creds'
        result = bridge_backup.restore_set(self.root / 'set-1', target_db, target_creds,
                                           expected_database_sha256=manifest['database']['sha256'])
        self.assertEqual(1, result['bound_credentials'])
        with sqlite3.connect(str(target_db)) as copy:
            self.assertEqual(1, copy.execute('SELECT COUNT(*) FROM session_credentials').fetchone()[0])
        self.assertEqual(1, bridge_backup.verify_pairing(target_db, target_creds))

    def test_restore_refuses_without_the_reviewed_digest(self):
        self.make()
        target_db = self.root / 'restored.sqlite3'
        with self.assertRaises(bridge_backup.BundleError):
            bridge_backup.restore_set(self.root / 'set-1', target_db, self.root / 'restored-creds',
                                      expected_database_sha256='0' * 64)
        self.assertFalse(target_db.exists())

    def test_restore_detects_a_tampered_credential_file(self):
        manifest = self.make()
        set_file = next((self.root / 'set-1' / bridge_backup.CREDENTIALS).glob(bridge_backup.CREDENTIAL_GLOB))
        set_file.write_bytes(set_file.read_bytes() + b'x')
        with self.assertRaises(bridge_backup.BundleError):
            bridge_backup.restore_set(self.root / 'set-1', self.root / 'restored.sqlite3',
                                      self.root / 'restored-creds',
                                      expected_database_sha256=manifest['database']['sha256'])

    def test_restore_never_replaces_an_existing_database(self):
        manifest = self.make()
        with self.assertRaises(bridge_backup.BundleError):
            bridge_backup.restore_set(self.root / 'set-1', self.database, self.root / 'restored-creds',
                                      expected_database_sha256=manifest['database']['sha256'])


if __name__ == '__main__':
    unittest.main()
