import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from service_guard import check_once, save_config


class ServiceGuardTests(unittest.TestCase):
    def test_healthy_service_is_not_restarted(self):
        service = {'script': 'native_worker.py', 'args': ['--recipient', 'thread'], 'pid': 123, 'creation': 99}
        popen = Mock()
        self.assertEqual([], check_once([service], identify=lambda pid: 99, popen=popen))
        popen.assert_not_called()

    def test_dead_service_restarts_only_allowed_script(self):
        service = {'script': 'watch_session.py', 'args': ['--pid', '123'], 'pid': 123, 'creation': 99}
        popen = Mock(return_value=SimpleNamespace(pid=456))
        with patch('status._process_is_alive', return_value=False):
            events = check_once([service], identify=lambda pid: 101 if pid == 456 else None, popen=popen)
        self.assertEqual('start_requested', events[0]['status'])
        self.assertEqual(456, service['pid'])
        self.assertIn('watch_session.py', popen.call_args.args[0][2])

    def test_unknown_identity_does_not_spawn_duplicate(self):
        service = {'script': 'supervisor.py', 'args': [], 'pid': 123, 'creation': 99}
        popen = Mock()
        with patch('status._process_is_alive', return_value=True):
            result = check_once([service], identify=lambda pid: None, popen=popen)
        self.assertEqual('identity_unknown', result[0]['status'])
        popen.assert_not_called()

    def test_missing_recorded_creation_does_not_spawn_duplicate(self):
        service = {'script': 'supervisor.py', 'args': [], 'pid': 123, 'creation': None}
        popen = Mock()
        result = check_once([service], identify=lambda pid: 99, popen=popen)
        self.assertEqual('identity_unknown', result[0]['status'])
        popen.assert_not_called()

    def test_unapproved_script_is_rejected(self):
        with self.assertRaises(ValueError):
            check_once([{'script': 'claude.exe', 'args': [], 'pid': 1, 'creation': 1}])

    def test_updated_service_identity_is_persisted(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'config.json'
            services = [{'script': 'supervisor.py', 'args': [], 'pid': 123, 'creation': 44}]
            save_config(path, services)
            self.assertEqual(services, json.loads(path.read_text(encoding='utf-8'))['services'])


if __name__ == '__main__':
    unittest.main()
