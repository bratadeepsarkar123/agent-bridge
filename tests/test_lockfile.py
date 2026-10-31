import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lockfile import acquire, release


class LockTests(unittest.TestCase):
    def test_stale_lock_is_recovered_and_only_owner_releases(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'worker.json'
            path.write_text(json.dumps({'pid': 999999}), encoding='utf-8')
            with patch('lockfile._process_is_alive', return_value=False):
                acquire(path, {'recipient': 'test'})
            self.assertEqual(os.getpid(), json.loads(path.read_text())['pid'])
            release(path)
            self.assertFalse(path.exists())

    def test_live_or_invalid_lock_is_not_removed(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'worker.json'
            path.write_text(json.dumps({'pid': 42}), encoding='utf-8')
            with patch('lockfile._process_is_alive', return_value=True):
                with self.assertRaises(RuntimeError):
                    acquire(path, {})
            path.write_text('invalid', encoding='utf-8')
            with self.assertRaises(RuntimeError):
                acquire(path, {})
            self.assertTrue(path.exists())


if __name__ == '__main__':
    unittest.main()
