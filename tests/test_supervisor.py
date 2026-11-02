import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import supervisor


class SupervisorTests(unittest.TestCase):
    def test_status_does_not_restart_without_proof_of_crash(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'state.json'
            with patch.object(supervisor, 'STATE', state):
                self.assertEqual('stopped', supervisor.inspect_broker())
                state.write_text('bad json', encoding='utf-8')
                self.assertEqual('invalid', supervisor.inspect_broker())
                state.write_text(json.dumps({'pid': 12345}), encoding='utf-8')
                with patch.object(supervisor, '_process_is_alive', return_value=True):
                    self.assertEqual('running', supervisor.inspect_broker())
                with patch.object(supervisor, '_process_is_alive', return_value=False):
                    self.assertEqual('crashed', supervisor.inspect_broker())


class SupervisorStopRequestTests(unittest.TestCase):
    """Temp folder and injected fakes only: Popen and sleep are replaced; no broker or process is touched."""

    def setUp(self):
        self._folder = tempfile.TemporaryDirectory()
        self.addCleanup(self._folder.cleanup)
        root = Path(self._folder.name)
        self.root = root
        self.state = root / 'broker-session.json'
        self.marker = root / 'broker-session.json.stop-pending'
        self.request = root / 'supervisor.stop-request'
        self.lock = root / 'supervisor-session.json'
        for name, value in (('STATE', self.state), ('LOCK', self.lock), ('LOG', root / 'supervisor.log'),
                            ('STOP_REQUEST', self.request)):
            patcher = patch.object(supervisor, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.popen = Mock(return_value=Mock(poll=Mock(return_value=None)))
        popen_patch = patch.object(supervisor.subprocess, 'Popen', self.popen)
        popen_patch.start()
        self.addCleanup(popen_patch.stop)

    def write_crashed_state(self):
        self.state.write_text(json.dumps({'pid': 12345}), encoding='utf-8')

    def test_request_present_at_start_exits_without_recovery_or_marker_change(self):
        self.write_crashed_state()
        self.marker.write_text('{}', encoding='utf-8')
        self.request.write_text('stop\n', encoding='utf-8')
        before = self.state.read_bytes()
        with patch.object(supervisor, '_process_is_alive', return_value=False):
            supervisor.run()
        self.popen.assert_not_called()
        self.assertFalse(self.request.exists())
        self.assertFalse(self.lock.exists())  # lock released by the normal exit path
        self.assertEqual(before, self.state.read_bytes())
        self.assertEqual('{}', self.marker.read_text(encoding='utf-8'))
        self.assertIn('stop requested', (self.root / 'supervisor.log').read_text(encoding='utf-8'))

    def test_request_arriving_during_crash_inspection_prevents_launch(self):
        self.write_crashed_state()

        def inspect_and_request():
            self.request.write_text('stop\n', encoding='utf-8')
            return 'crashed'

        with patch.object(supervisor, 'inspect_broker', side_effect=inspect_and_request):
            supervisor.run()
        self.popen.assert_not_called()
        self.assertFalse(self.request.exists())

    def test_request_arriving_during_sleep_stops_within_one_slice(self):
        self.write_crashed_state()
        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)
            self.request.write_text('stop\n', encoding='utf-8')

        with patch.object(supervisor, '_process_is_alive', return_value=True), \
                patch.object(supervisor.time, 'sleep', side_effect=fake_sleep):
            supervisor.run()
        self.assertEqual([1.0], sleeps)
        self.popen.assert_not_called()

    def test_without_request_a_crashed_broker_is_still_recovered(self):
        self.write_crashed_state()

        def stop_after_first_recovery(seconds):
            self.request.write_text('stop\n', encoding='utf-8')

        with patch.object(supervisor, '_process_is_alive', return_value=False), \
                patch.object(supervisor.time, 'sleep', side_effect=stop_after_first_recovery):
            supervisor.run()
        self.assertEqual(1, self.popen.call_count)
        self.assertIn('broker_launcher.py', self.popen.call_args.args[0][2])
        self.assertIn('--recover', self.popen.call_args.args[0])


if __name__ == '__main__':
    unittest.main()
