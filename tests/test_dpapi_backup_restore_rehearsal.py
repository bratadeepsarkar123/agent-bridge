"""Synthetic SQLite and real same-account Windows DPAPI restore rehearsal.

The broker database, backup, restored database, and both credential-file
directories are temporary. No live broker, configured database, or agent
session is read or contacted. On Windows, the copied credential is decrypted
with the current user's real DPAPI context.
"""

from contextlib import closing
import hashlib
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest

import credential_store
from broker import Broker


def backup_sqlite(source_path, destination_path):
    """Create a consistent SQLite backup through the database backup API."""
    with closing(sqlite3.connect(source_path)) as source, \
            closing(sqlite3.connect(destination_path)) as destination:
        source.backup(destination)


@unittest.skipUnless(sys.platform == "win32", "real user-bound DPAPI requires Windows")
class DpapiBackupRestoreRehearsalTests(unittest.TestCase):
    def test_restored_hash_and_copied_dpapi_file_authorize_opted_in_profile(self):
        with tempfile.TemporaryDirectory(prefix="bridge-dpapi-restore-") as folder:
            root = Path(folder)
            source_db = root / "synthetic-source.sqlite3"
            database_backup = root / "synthetic-backup.sqlite3"
            restored_db = root / "synthetic-restored.sqlite3"
            source_credentials = root / "source-credentials"
            restored_credentials = root / "restored-credentials"
            source_credentials.mkdir()
            restored_credentials.mkdir()
            # Provisioning refuses any directory whose ACL is not private.
            credential_store.apply_private_acl(source_credentials, directory=True)

            profile_id = "synthetic-dpapi-profile"
            source_broker = Broker(source_db)
            provisioned = credential_store.provision_initial_profile(
                source_broker,
                profile_id,
                source_credentials,
            )
            original_file = Path(provisioned["credential_file"])
            self.assertTrue(original_file.is_file())

            # Provisioning binds the hash but does not opt the profile in.
            original_token = credential_store.read_generation(original_file).decode("ascii")
            self.assertNotIn(original_token, str(provisioned))
            self.assertTrue(source_broker.credential_is_valid(original_token))
            with self.assertRaises(ValueError):
                source_broker.check_session(profile_id, credential=original_token)
            source_broker.register(
                {"id": profile_id, "kind": "codex", "opt_in": True},
                credential=original_token,
            )
            self.assertEqual(
                {"id": profile_id, "kind": "codex"},
                source_broker.check_session(profile_id, credential=original_token),
            )

            # Back up SQLite data and the DPAPI file as separate artifacts.
            backup_sqlite(source_db, database_backup)
            copied_file = restored_credentials / "restored-credential.dpapi"
            shutil.copy2(original_file, copied_file)
            self.assertNotEqual(original_file, copied_file)
            self.assertEqual(original_file.read_bytes(), copied_file.read_bytes())

            # Restore the database through SQLite's API, independently of the
            # file copy, then ask real DPAPI to decrypt the copied generation.
            backup_sqlite(database_backup, restored_db)
            restored_token = credential_store.read_generation(copied_file).decode("ascii")
            self.assertEqual(original_token, restored_token)
            self.assertNotIn(restored_token.encode("ascii"), copied_file.read_bytes())

            restored_broker = Broker(restored_db)
            expected_hash = hashlib.sha256(restored_token.encode("ascii")).hexdigest()
            with closing(restored_broker.connect()) as db:
                credential_row = db.execute(
                    "SELECT session_id, token_sha256 FROM session_credentials WHERE session_id=?",
                    (profile_id,),
                ).fetchone()
                session_row = db.execute(
                    "SELECT id, kind, opted_in FROM sessions WHERE id=?",
                    (profile_id,),
                ).fetchone()
                self.assertEqual((profile_id, expected_hash), tuple(credential_row))
                self.assertEqual((profile_id, "codex", 1), tuple(session_row))
                self.assertNotIn(restored_token, "\n".join(db.iterdump()))

            # Authentication succeeds only when the copied DPAPI credential is
            # paired with the restored hash and the profile's opt-in state.
            self.assertTrue(restored_broker.credential_is_valid(restored_token))
            self.assertEqual(
                {"id": profile_id, "kind": "codex"},
                restored_broker.check_session(profile_id, credential=restored_token),
            )
            with self.assertRaises(PermissionError):
                restored_broker.check_session(profile_id)
            with self.assertRaises(PermissionError):
                restored_broker.check_session(profile_id, credential="X" * 43)


if __name__ == "__main__":
    unittest.main()
