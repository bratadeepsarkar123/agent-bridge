"""Checked stop-then-copy backup: copies only a verifiably stopped broker database."""
from pathlib import Path
import sqlite3
import tempfile
import unittest

import backup_tool
import broker


class CheckedBackupTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.database = self.root / 'broker.sqlite3'
        self.state = self.root / 'broker-session.json'
        broker.Broker(self.database)  # creates the current schema
        self.destination = self.root / 'backup-1.sqlite3'

    def tearDown(self):
        self.folder.cleanup()

    def test_copies_a_stopped_database_and_the_copy_is_readable(self):
        digest = backup_tool.checked_backup(self.database, self.state, self.destination)
        self.assertEqual(64, len(digest))
        with sqlite3.connect(str(self.destination)) as copy:
            self.assertEqual('ok', copy.execute('PRAGMA integrity_check').fetchone()[0])
        self.assertFalse(list(self.root.glob('*.partial')))

    def test_refuses_while_discovery_state_exists(self):
        self.state.write_text('{}', encoding='utf-8')
        with self.assertRaises(backup_tool.BackupRefused):
            backup_tool.checked_backup(self.database, self.state, self.destination)
        self.assertFalse(self.destination.exists())

    def test_refuses_while_a_stop_note_exists(self):
        broker.stop_marker_path(self.state).write_text('pending', encoding='utf-8')
        with self.assertRaises(backup_tool.BackupRefused):
            backup_tool.checked_backup(self.database, self.state, self.destination)
        self.assertFalse(self.destination.exists())

    def test_never_overwrites_an_existing_destination(self):
        self.destination.write_bytes(b'earlier backup')
        with self.assertRaises(backup_tool.BackupRefused):
            backup_tool.checked_backup(self.database, self.state, self.destination)
        self.assertEqual(b'earlier backup', self.destination.read_bytes())

    def test_refuses_when_database_is_missing(self):
        with self.assertRaises(backup_tool.BackupRefused):
            backup_tool.checked_backup(self.root / 'missing.sqlite3', self.state, self.destination)
        self.assertFalse(self.destination.exists())


if __name__ == '__main__':
    unittest.main()
