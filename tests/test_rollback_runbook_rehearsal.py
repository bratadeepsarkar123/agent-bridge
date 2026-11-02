"""Rehearses the data half of the ROLLOUT rollback runbook on temporary data only.

Steps exercised: stopped-state set made (checked copy + paired credentials),
set restored to NEW paths with the reviewed digest, restored credential accepted
by the current build, restored schema accepted by the startup guard.

NOT proven here: a compatible security-enforcing rollback BUILD. ROLLOUT.md
forbids restoring the old live build against an upgraded database; no such
build is available, so the bridge must stay stopped until one is.
"""
import os
from pathlib import Path
import tempfile
import unittest

import bridge_backup
import credential_store
from broker import Broker, require_current_schema_or_empty


@unittest.skipUnless(os.name == 'nt', 'DPAPI is Windows-only')
class RollbackRunbookRehearsalTests(unittest.TestCase):
    def test_restored_set_is_accepted_by_current_build_at_new_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database = root / 'live.sqlite3'
            state = root / 'broker-session.json'
            creds = root / 'creds'
            creds.mkdir()
            credential_store.apply_private_acl(creds, directory=True)
            live = Broker(database)
            credential_store.provision_initial_profile(live, 'alpha', creds)

            # 1. Stopped state: no discovery file, so the set is made.
            manifest = bridge_backup.make_set(database, state, creds, root / 'set-1')

            # 2. Restore to new paths with the digest a person reviewed.
            restored_db = root / 'restored.sqlite3'
            restored_creds = root / 'restored-creds'
            bridge_backup.restore_set(root / 'set-1', restored_db, restored_creds,
                                      expected_database_sha256=manifest['database']['sha256'],
                                      retired_database=database)

            # 3. The startup guard accepts the restored schema without migrating it.
            require_current_schema_or_empty(restored_db, require_existing=True)

            # 4. The restored credential authenticates against the restored database.
            restored = Broker(restored_db)
            credential_file = next(restored_creds.glob(bridge_backup.CREDENTIAL_GLOB))
            token = credential_store.read_generation(credential_file).decode('ascii')
            self.assertTrue(restored.credential_is_valid(token))

            # 5. The live database and its credential file are untouched.
            self.assertTrue(live.credential_is_valid(token))
            self.assertTrue(database.exists())


if __name__ == '__main__':
    unittest.main()
