"""Isolated stop fences: synthetic files, mocked services, owned subprocesses only."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import broker
import supervisor


ROOT = Path(__file__).resolve().parents[1]


class StopRecoveryFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / 'state.json'
        # Current legacy discovery: no instance UUID or new mandatory fields.
        self.raw = json.dumps({'pid': 12345, 'port': 12345,
                               'token': 'synthetic-' + 'x' * 40}).encode()
        self.state.write_bytes(self.raw)

    def publish(self):
        return broker.publish_stop_inhibit(self.state, self.raw)

    def test_publish_matches_legacy_discovery_without_exposing_bearer(self):
        marker = self.publish()
        self.assertEqual(marker, broker.stop_marker_path(self.state).read_bytes())
        self.assertNotIn(json.loads(self.raw)['token'].encode(), marker)
        with self.assertRaises(RuntimeError):
            broker.require_recovery_allowed(self.state)
        with patch.object(supervisor, 'STATE', self.state), \
                patch.object(supervisor, '_process_is_alive', return_value=False) as alive:
            self.assertEqual('stop_pending', supervisor.inspect_broker())
            alive.assert_not_called()

    def test_stale_malformed_empty_and_foreign_markers_all_inhibit(self):
        marker = broker.stop_marker_path(self.state)
        for raw in (b'', b'{', b'{"version":999}', b'{"state_sha256":"foreign"}'):
            marker.write_bytes(raw)
            with self.assertRaises(RuntimeError):
                broker.require_recovery_allowed(self.state)
            with self.assertRaises(RuntimeError):
                self.publish()
            self.assertEqual(raw, marker.read_bytes())

    def test_clean_only_exact_owned_marker_and_discovery(self):
        owned = self.publish()
        self.state.write_bytes(self.raw + b' ')
        with self.assertRaises(RuntimeError):
            broker.complete_stop(self.state, self.raw, owned)
        self.assertTrue(broker.stop_marker_path(self.state).exists())
        self.state.write_bytes(self.raw)
        broker.stop_marker_path(self.state).write_bytes(b'foreign')
        with self.assertRaises(RuntimeError):
            broker.complete_stop(self.state, self.raw, owned)
        self.assertTrue(self.state.exists())
        broker.stop_marker_path(self.state).write_bytes(owned)
        broker.complete_stop(self.state, self.raw, owned)
        self.assertFalse(self.state.exists())
        self.assertFalse(broker.stop_marker_path(self.state).exists())
        self.assertTrue(self.state.with_name('state.json.recovery-lock').exists())

    def test_failed_flush_or_publication_never_returns_success(self):
        for target in ('fsync', 'link'):
            with patch.object(broker.os, target, side_effect=OSError('synthetic failure')):
                with self.assertRaises(OSError):
                    self.publish()
            self.assertEqual(self.raw, self.state.read_bytes())
            self.assertFalse(broker.stop_marker_path(self.state).exists())

    def test_failure_after_publication_leaves_inhibit(self):
        with patch.object(broker, '_sync_directory', side_effect=OSError('synthetic failure')):
            with self.assertRaises(OSError):
                self.publish()
        with self.assertRaises(RuntimeError):
            broker.require_recovery_allowed(self.state)

    def test_cleanup_failure_after_discovery_removal_keeps_marker(self):
        owned = self.publish()
        with patch.object(broker, '_sync_directory', side_effect=OSError('synthetic failure')):
            with self.assertRaises(OSError):
                broker.complete_stop(self.state, self.raw, owned)
        self.assertFalse(self.state.exists())
        with self.assertRaises(RuntimeError):
            broker.require_recovery_allowed(self.state)
        with patch.object(supervisor, 'STATE', self.state):
            self.assertEqual('stop_pending', supervisor.inspect_broker())

    def test_check_to_launch_race_is_rechecked_by_broker_before_setup(self):
        with patch.object(supervisor, 'STATE', self.state), \
                patch.object(supervisor, '_process_is_alive', return_value=False):
            self.assertEqual('crashed', supervisor.inspect_broker())
        self.publish()  # Stop begins after supervisor's decision.
        with patch.object(sys, 'argv', ['broker.py', '--recover', '--state', str(self.state)]), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server') as prepare:
            with self.assertRaises(SystemExit):
                broker.main()
            prepare.assert_not_called()

    def test_supervisor_rechecks_inhibit_before_launch(self):
        with patch.object(supervisor, 'acquire'), patch.object(supervisor, 'release'), \
                patch.object(supervisor, 'log'), \
                patch.object(supervisor, 'inspect_broker', return_value='crashed'), \
                patch.object(supervisor, 'require_recovery_allowed', side_effect=RuntimeError('pending')), \
                patch.object(supervisor.subprocess, 'Popen') as launch, \
                patch.object(supervisor.time, 'sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                supervisor.run()
            launch.assert_not_called()

    @patch.object(broker, 'FENCE_RETRY_BASE', 0.0)
    def test_failed_inhibit_write_keeps_serving_and_retries_before_drain(self):
        real_publish = broker.publish_stop_inhibit
        attempts = []
        server = Mock()
        server.active_handlers = 0
        server.wait_for_drain.return_value = True
        serve_passes = []
        def fail_twice_then_publish(*args):
            attempts.append(1)
            if len(attempts) <= 2:
                raise OSError('synthetic disk failure')
            return real_publish(*args)
        def serve_then_interrupt():
            serve_passes.append(len(attempts))
            if not attempts:
                raise KeyboardInterrupt()
        server.handle_request.side_effect = serve_then_interrupt
        with patch.object(sys, 'argv', ['broker.py', '--state', str(self.state)]), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server', return_value=(server, self.raw)), \
                patch.object(broker, 'publish_stop_inhibit', side_effect=fail_twice_then_publish), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                broker.main()
        # One serving pass in the main loop, then one pass after each of the two
        # failed writes; the third write succeeded and drain began.
        self.assertEqual(3, len(attempts))
        self.assertEqual(3, server.handle_request.call_count)
        server.begin_drain.assert_called_once()
        self.assertFalse(self.state.exists())
        self.assertFalse(broker.stop_marker_path(self.state).exists())

    @patch.object(broker, 'FENCE_RETRY_BASE', 0.0)
    def test_unpublishable_inhibit_never_drains_while_still_serving(self):
        server = Mock()
        serve_calls = []
        class Stop(Exception):
            pass
        def serve_then_stop():
            serve_calls.append(1)
            if len(serve_calls) >= 3:
                raise Stop()
        server.handle_request.side_effect = serve_then_stop
        with patch.object(sys, 'argv', ['broker.py', '--state', str(self.state)]), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server', return_value=(server, self.raw)), \
                patch.object(broker, 'publish_stop_inhibit', side_effect=OSError('synthetic disk failure')), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(Stop):
                broker.main()
        # Still serving on every retry pass: no drain, no discovery removal.
        # (Stop is raised on the third pass and again on the retry after it.)
        self.assertGreaterEqual(len(serve_calls), 3)
        server.begin_drain.assert_not_called()
        self.assertEqual(self.raw, self.state.read_bytes())
        self.assertFalse(broker.stop_marker_path(self.state).exists())

    def test_refused_inhibit_still_exits_with_rollout_blocker(self):
        server = Mock()
        server.handle_request.side_effect = KeyboardInterrupt()
        with patch.object(sys, 'argv', ['broker.py', '--state', str(self.state)]), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server', return_value=(server, self.raw)), \
                patch.object(broker, 'publish_stop_inhibit', side_effect=RuntimeError('refused')), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                broker.main()
        server.begin_drain.assert_not_called()
        self.assertIn('ROLLOUT BLOCKER', str(error.exception))
        self.assertEqual(self.raw, self.state.read_bytes())
        # After process death with no inhibit, a supervisor may auto-recover.
        with patch.object(supervisor, 'STATE', self.state), \
                patch.object(supervisor, '_process_is_alive', return_value=False):
            self.assertEqual('crashed', supervisor.inspect_broker())

    def test_recovery_gate_blocks_other_process_without_stale_lock_cleanup(self):
        script = ('import sys; from pathlib import Path; from broker import recovery_gate; '
                  '\nwith recovery_gate(Path(sys.argv[1])): pass')
        with broker.recovery_gate(self.state):
            result = subprocess.run([sys.executable, '-B', '-c', script, str(self.state)],
                                    cwd=ROOT, capture_output=True, timeout=5)
            self.assertNotEqual(0, result.returncode)
        result = subprocess.run([sys.executable, '-B', '-c', script, str(self.state)],
                                cwd=ROOT, capture_output=True, timeout=5)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_process_death_releases_gate_but_keeps_published_inhibit(self):
        script = ('import os,sys; from pathlib import Path; '
                  'from broker import publish_stop_inhibit,recovery_gate; '
                  'p=Path(sys.argv[1]); publish_stop_inhibit(p,p.read_bytes()); '
                  '\nwith recovery_gate(p): os._exit(7)')
        result = subprocess.run([sys.executable, '-B', '-c', script, str(self.state)],
                                cwd=ROOT, capture_output=True, timeout=5)
        self.assertEqual(7, result.returncode, result.stderr)
        with broker.recovery_gate(self.state):
            with self.assertRaises(RuntimeError):
                broker.require_recovery_allowed(self.state)

    def exercise_main(self, *, drained=True, print_failure=False, finish_after_deadline=False):
        server = Mock()
        server.active_handlers = 0 if drained else 1
        server.wait_for_drain.return_value = drained
        if finish_after_deadline:
            def deadline(_timeout):
                server.active_handlers = 0
                return False
            server.wait_for_drain.side_effect = deadline
        server.handle_request.side_effect = lambda: threading.Event().wait(.01)
        marker_seen = []
        def begin():
            marker_seen.append(broker.stop_marker_path(self.state).is_file())
        server.begin_drain.side_effect = begin
        argv = ['broker.py', '--control-stdin', '--state', str(self.state), '--drain-timeout', '.1']
        with patch.object(sys, 'argv', argv), patch.object(sys, 'stdin', io.StringIO('')), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server', return_value=(server, self.raw)), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            if print_failure:
                original_print = print
                def fail_ready(*args, **kwargs):
                    if args and 'listening' in str(args[0]):
                        raise BrokenPipeError('synthetic stdout failure')
                    original_print(*args, **kwargs)
                with patch('builtins.print', side_effect=fail_ready):
                    with self.assertRaises(BrokenPipeError):
                        broker.main()
            elif drained:
                broker.main()
            else:
                with self.assertRaises(SystemExit) as error:
                    broker.main()
                self.assertEqual(2, error.exception.code)
        self.assertTrue(marker_seen)
        self.assertTrue(all(marker_seen), 'drain preceded durable inhibit')
        return server

    def test_eof_persists_before_drain_and_success_cleans(self):
        self.exercise_main()
        self.assertFalse(self.state.exists())
        self.assertFalse(broker.stop_marker_path(self.state).exists())

    def test_failed_drain_retains_fence_after_handlers_eventually_exit(self):
        self.exercise_main(drained=False)
        self.assertTrue(self.state.exists())
        with patch.object(supervisor, 'STATE', self.state), \
                patch.object(supervisor, '_process_is_alive', return_value=False):
            self.assertEqual('stop_pending', supervisor.inspect_broker())

    def test_controller_deadline_is_not_followed_by_second_drain_wait(self):
        server = self.exercise_main(drained=False, finish_after_deadline=True)
        server.wait_for_drain.assert_called_once_with(.1)
        self.assertEqual(0, server.active_handlers)
        self.assertTrue(self.state.exists())
        self.assertTrue(broker.stop_marker_path(self.state).exists())

    def test_startup_stdout_failure_with_eof_does_not_join_blocked_control(self):
        self.exercise_main(print_failure=True)

    @patch.object(broker, 'FENCE_RETRY_BASE', 0.0)
    def test_control_stop_retries_failed_inhibit_instead_of_dropping_stop(self):
        real_publish = broker.publish_stop_inhibit
        attempts = []
        def fail_once_then_publish(*args):
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError('synthetic disk failure')
            return real_publish(*args)
        server = Mock()
        server.active_handlers = 0
        server.wait_for_drain.return_value = True
        server.handle_request.side_effect = lambda: threading.Event().wait(.01)
        argv = ['broker.py', '--control-stdin', '--state', str(self.state), '--drain-timeout', '1']
        with patch.object(sys, 'argv', argv), patch.object(sys, 'stdin', io.StringIO('stop\n')), \
                patch.object(broker, 'require_current_schema_or_empty'), \
                patch.object(broker, '_prepare_server', return_value=(server, self.raw)), \
                patch.object(broker, 'publish_stop_inhibit', side_effect=fail_once_then_publish), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            broker.main()
        # The first write failed, the control thread retried, and only then drained.
        self.assertEqual(2, len(attempts))
        server.begin_drain.assert_called()
        self.assertFalse(self.state.exists())
        self.assertFalse(broker.stop_marker_path(self.state).exists())

    def test_retry_delay_doubles_from_base_and_is_capped(self):
        self.assertEqual(1.0, broker.fence_retry_delay(1))
        self.assertEqual(2.0, broker.fence_retry_delay(2))
        self.assertEqual(16.0, broker.fence_retry_delay(5))
        self.assertEqual(30.0, broker.fence_retry_delay(6))
        self.assertEqual(30.0, broker.fence_retry_delay(10_000))

    def test_fence_retry_log_limits_messages_and_alerts_once(self):
        log = broker.FenceRetryLog('shutdown')
        err = io.StringIO()
        with redirect_stderr(err):
            delays = [log.failed(OSError('synthetic')) for _ in range(60)]
        lines = err.getvalue().splitlines()
        # Three burst messages within the same minute, then exactly one alert at the threshold.
        self.assertEqual(broker.FENCE_LOG_BURST + 1, len(lines))
        self.assertEqual(1, sum(line.startswith('ALERT') for line in lines))
        self.assertEqual(30.0, delays[-1])
        # A later failure, once the interval has passed, is logged again.
        log.last_logged -= broker.FENCE_LOG_INTERVAL + 1
        with redirect_stderr(err):
            log.failed(OSError('synthetic'))
        self.assertEqual(broker.FENCE_LOG_BURST + 2, len(err.getvalue().splitlines()))


if __name__ == '__main__':
    unittest.main()
