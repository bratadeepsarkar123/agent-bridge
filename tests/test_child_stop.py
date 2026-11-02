import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import child_stop  # noqa: E402
import watch_errors  # noqa: E402


class RequestPathTests(unittest.TestCase):
    def test_request_name_uses_script_stem(self):
        root = Path('some-root')
        self.assertEqual(root / 'watch_errors.stop-request', child_stop.request_path('watch_errors.py', root))
        self.assertEqual(root / 'native_worker.stop-request', child_stop.request_path('native_worker.py', root))


class PauseTests(unittest.TestCase):
    def test_pause_returns_early_when_request_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            request = Path(tmp) / 'watch_errors.stop-request'
            request.write_text('stop\n', encoding='utf-8')
            sleeps = []
            child_stop.pause(30, request, sleep=sleeps.append)
            self.assertEqual([], sleeps)

    def test_pause_sleeps_in_one_second_slices(self):
        with tempfile.TemporaryDirectory() as tmp:
            sleeps = []
            child_stop.pause(2.5, Path(tmp) / 'missing.stop-request', sleep=sleeps.append)
            self.assertEqual([1.0, 1.0, 0.5], sleeps)


class WatchErrorsStopTests(unittest.TestCase):
    def test_watcher_exits_without_polling_when_request_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            transcript = Path(tmp) / 'session.jsonl'
            transcript.write_text('', encoding='utf-8')
            state = Path(tmp) / 'state.sqlite3'
            request = child_stop.request_path(watch_errors.__file__)
            request.write_text('stop\n', encoding='utf-8')
            try:
                with mock.patch.object(watch_errors, 'poll_once') as poll, \
                        mock.patch.object(sys, 'argv', ['watch_errors.py', '--transcript', str(transcript),
                                                        '--recipient', 'thread-x', '--state', str(state)]), \
                        mock.patch('builtins.print') as printed:
                    watch_errors.main()
                poll.assert_not_called()
                printed.assert_any_call(json.dumps({'status': 'stop_requested'}), flush=True)
                self.assertFalse(request.exists(), 'the request must be consumed on stop')
            finally:
                child_stop.consume(request)


class ChildContractTests(unittest.TestCase):
    """Every child that service_stop can stop must poll its own request file."""

    CHILDREN = ['native_worker.py', 'watch_session.py', 'watch_errors.py', 'multi_dispatcher.py']

    def test_each_child_checks_and_consumes_its_request(self):
        root = Path(__file__).resolve().parents[1]
        for script in self.CHILDREN:
            source = (root / script).read_text(encoding='utf-8')
            self.assertIn('child_stop.request_path(__file__)', source, script)
            self.assertIn('child_stop.consume(', source, script)

    def test_children_never_use_taskkill_or_force(self):
        root = Path(__file__).resolve().parents[1]
        for script in self.CHILDREN:
            source = (root / script).read_text(encoding='utf-8')
            self.assertNotIn('taskkill', source, script)
            self.assertNotIn('TerminateProcess', source, script)


if __name__ == '__main__':
    unittest.main()
