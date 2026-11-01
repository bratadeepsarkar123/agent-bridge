"""SQLite writer-fence rehearsal; does not prove no process holds handles."""
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


class WriterFenceTests(unittest.TestCase):
    def test_separate_writer_is_blocked_during_backup(self):
        for mode in ('DELETE', 'WAL'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                source_path = Path(directory) / 'source.sqlite3'
                backup_path = Path(directory) / 'backup.sqlite3'
                with closing(sqlite3.connect(source_path)) as fence:
                    self.assertEqual(mode.lower(), fence.execute('PRAGMA journal_mode=' + mode).fetchone()[0].lower())
                    fence.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)')
                    fence.execute("INSERT INTO messages VALUES (1, 'before')")
                    fence.commit()
                    fence.execute('BEGIN IMMEDIATE')
                    script = (
                        'import sqlite3,sys\n'
                        'db=sqlite3.connect(sys.argv[1],timeout=0.1)\n'
                        'try:\n'
                        ' db.execute("INSERT INTO messages VALUES (2, \'blocked\')"); db.commit()\n'
                        'except sqlite3.OperationalError as exc:\n'
                        ' print(str(exc)); sys.exit(7)\n'
                        'finally: db.close()\n'
                    )
                    attempt = subprocess.run([sys.executable, '-B', '-c', script, str(source_path)],
                                             capture_output=True, text=True, timeout=5)
                    self.assertEqual(7, attempt.returncode, attempt.stderr)
                    self.assertIn('locked', attempt.stdout.lower())
                    with closing(sqlite3.connect(source_path)) as reader, \
                            closing(sqlite3.connect(backup_path)) as destination:
                        reader.backup(destination)
                        self.assertEqual(1, destination.execute('SELECT COUNT(*) FROM messages').fetchone()[0])
                    fence.rollback()
                    fence.execute("INSERT INTO messages VALUES (2, 'after')")
                    fence.commit()
                    self.assertEqual(2, fence.execute('SELECT COUNT(*) FROM messages').fetchone()[0])
