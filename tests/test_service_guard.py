import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import service_guard
from service_guard import check_once, run_loop, save_config, validate_service


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

    def test_multi_dispatcher_is_an_allowed_service(self):
        service = {
            'script': 'multi_dispatcher.py',
            'args': ['--recipient', 'thread-a', '--recipient', 'thread-b'],
            'pid': 123,
            'creation': 99,
        }
        validate_service(service)

    def test_dead_multi_dispatcher_is_restarted_with_configured_recipients(self):
        service = {
            'script': 'multi_dispatcher.py',
            'args': ['--recipient', 'thread-a', '--recipient', 'thread-b'],
            'pid': 123,
            'creation': 99,
        }
        popen = Mock(return_value=SimpleNamespace(pid=456))
        with patch('status._process_is_alive', return_value=False):
            events = check_once([service], identify=lambda pid: 101 if pid == 456 else None, popen=popen)

        command = popen.call_args.args[0]
        self.assertEqual('multi_dispatcher.py', Path(command[2]).name)
        self.assertEqual(service['args'], command[3:])
        self.assertEqual('start_requested', events[0]['status'])
        self.assertEqual(456, service['pid'])

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


class ServiceGuardStopRequestTests(unittest.TestCase):
    """Injected fakes and a temp folder only: no process is spawned or signalled."""

    def setUp(self):
        self._folder = tempfile.TemporaryDirectory()
        self.addCleanup(self._folder.cleanup)
        self.root = Path(self._folder.name)
        self.request = self.root / 'service-guard.stop-request'
        self.config = self.root / 'services.json'
        self.lines = []

    def service(self):
        return {'script': 'watch_session.py', 'args': ['--pid', '123'], 'pid': 123, 'creation': 99}

    def test_default_request_path_is_in_guard_root(self):
        self.assertEqual(service_guard.ROOT / 'service-guard.stop-request', service_guard.STOP_REQUEST)

    def test_request_present_before_first_pass_spawns_nothing_and_is_consumed(self):
        self.request.write_text('stop\n', encoding='utf-8')
        services = [self.service()]
        before = json.dumps(services)
        popen = Mock()
        pause = Mock()
        run_loop(services, self.config, 30, self.request,
                 check=lambda s: check_once(s, identify=lambda pid: None, popen=popen),
                 save=Mock(), say=self.lines.append, pause=pause)
        popen.assert_not_called()
        pause.assert_not_called()
        self.assertFalse(self.request.exists())
        self.assertEqual(before, json.dumps(services))  # children are neither touched nor re-recorded
        self.assertEqual(['{"status": "stop_requested"}'], self.lines)

    def test_request_during_sleep_is_seen_within_one_second_and_no_further_pass_runs(self):
        check = Mock(return_value=[])
        sleeps = []

        def pause(seconds):
            sleeps.append(seconds)
            self.request.write_text('stop\n', encoding='utf-8')

        run_loop([self.service()], self.config, 30, self.request, check=check, save=Mock(),
                 say=self.lines.append, pause=pause)
        self.assertEqual(1, check.call_count)
        self.assertEqual([1.0], sleeps)  # the 30 s interval is sliced into 1 s checks
        self.assertFalse(self.request.exists())

    def test_without_request_the_loop_keeps_running(self):
        calls = []

        def check(services):
            calls.append(1)
            if len(calls) == 3:
                self.request.write_text('stop\n', encoding='utf-8')
            return []

        run_loop([self.service()], self.config, 10, self.request, check=check, save=Mock(),
                 say=self.lines.append, pause=lambda seconds: None)
        self.assertEqual(3, len(calls))

    def test_guard_exits_without_saving_config_or_spawning_when_stopped(self):
        self.request.write_text('stop\n', encoding='utf-8')
        save = Mock()
        run_loop([self.service()], self.config, 10, self.request, check=Mock(return_value=[]),
                 save=save, say=self.lines.append, pause=lambda seconds: None)
        save.assert_not_called()
        self.assertFalse(self.config.exists())


if __name__ == '__main__':
    unittest.main()
