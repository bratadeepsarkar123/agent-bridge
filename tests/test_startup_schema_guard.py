"""CLI schema guard uses isolated synthetic files, never a running broker."""
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from broker import Broker, require_current_schema_or_empty


class StartupSchemaGuardTests(unittest.TestCase):
    def test_refuses_legacy_before_migrating_or_creating_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / 'old.sqlite3'
            state = root / 'state.json'
            with closing(sqlite3.connect(database)) as db, db:
                db.execute('CREATE TABLE sessions (id TEXT PRIMARY KEY, kind TEXT, updated REAL)')
                db.execute("INSERT INTO sessions VALUES ('example', 'codex', 1)")
            before = database.read_bytes()
            result = subprocess.run([sys.executable, '-B', 'broker.py', '--database', str(database),
                                     '--state', str(state)], capture_output=True, timeout=10)
            self.assertNotEqual(0, result.returncode)
            self.assertIn(b'Broker refused database', result.stderr)
            self.assertFalse(state.exists())
            self.assertEqual(before, database.read_bytes())

    def test_accepts_empty_or_current_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            require_current_schema_or_empty(root / 'missing.sqlite3')
            current = root / 'current.sqlite3'
            Broker(current)
            require_current_schema_or_empty(current)

    def test_rejects_incomplete_or_unreadable_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.sqlite3'
            path.write_bytes(b'not a sqlite file')
            with self.assertRaises(sqlite3.DatabaseError):
                require_current_schema_or_empty(path)

    def test_rejects_current_columns_without_message_uniqueness(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'missing-constraints.sqlite3'
            Broker(path)
            with closing(sqlite3.connect(path)) as db, db:
                db.execute('ALTER TABLE messages RENAME TO prior_messages')
                db.execute('CREATE TABLE messages AS SELECT * FROM prior_messages')
                db.execute('DROP TABLE prior_messages')
            with self.assertRaisesRegex(ValueError, 'uniqueness'):
                require_current_schema_or_empty(path)

    def test_recovery_refuses_missing_or_empty_database_before_state_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / 'missing.sqlite3'
            empty = root / 'empty.sqlite3'
            with closing(sqlite3.connect(empty)):
                pass
            for database in (missing, empty):
                result = subprocess.run([sys.executable, '-B', 'broker.py', '--recover',
                                         '--database', str(database), '--state', str(root / 'absent-state')],
                                        capture_output=True, timeout=10)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(b'Broker refused database', result.stderr)
            self.assertFalse(missing.exists())
