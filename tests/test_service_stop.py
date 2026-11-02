import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import service_stop
from service_stop import Controls, Stopper, main


GUARD_PID = 4001
SUPERVISOR_PID = 4002
BROKER_PID = 4003
CHILD_A_PID = 5001
CHILD_B_PID = 5002
REQUEST_PIDS = {service_stop.GUARD_STOP_REQUEST: GUARD_PID,
                service_stop.SUPERVISOR_STOP_REQUEST: SUPERVISOR_PID}


class FakeWorld:
    """Fake process table and control callables. No real process is touched."""

    def __init__(self, root):
        self.root = Path(root)
        self.running = set()
        self.commands = {}
        self.exits_on_request = set()
        self.drain_codes = [0]
        self.calls = []
        self.started = []
        self.start_exit_code = None
        self.request_error = None
        self.sleeps = []

    def add_process(self, pid, command, exits=True):
        self.running.add(pid)
        self.commands[pid] = command
        if exits:
            self.exits_on_request.add(pid)

    def write_lock(self, name, pid):
        (self.root / name).write_text(json.dumps({'pid': pid}), encoding='utf-8')

    def _request_exit(self, pid):
        self.calls.append(('exit', pid))
        if pid in self.exits_on_request:
            self.running.discard(pid)

    def _write_stop_request(self, path):
        """Fake a stop-request file. A process that consumes it exits and removes the file."""
        self.calls.append(('request', path.name))
        if self.request_error is not None:
            raise self.request_error
        if not path.exists():
            path.write_text('stop\n', encoding='utf-8')
        pid = REQUEST_PIDS[path.name]
        if pid in self.exits_on_request and pid in self.running:
            self.running.discard(pid)
            path.unlink(missing_ok=True)

    def _drain(self, root, timeout):
        self.calls.append(('drain', timeout))
        code = self.drain_codes.pop(0) if self.drain_codes else 0
        if code == 0:
            self.running.discard(BROKER_PID)
            (self.root / 'broker-session.json').unlink(missing_ok=True)
        return code

    def _start(self, args, root):
        script = Path(args[2]).name if len(args) > 2 else ''
        self.calls.append(('start', script))
        self.started.append(script)
        child = SimpleNamespace(pid=9000 + len(self.started), poll=lambda: self.start_exit_code)
        if script == 'broker_launcher.py':
            (self.root / 'broker-session.json').write_text(json.dumps({'pid': BROKER_PID}), encoding='utf-8')
        return child

    def controls(self):
        return Controls(
            read_pid=service_stop._default_read_pid,
            is_alive=lambda pid: pid in self.running,
            command_line=lambda pid: self.commands.get(pid),
            request_exit=self._request_exit,
            write_stop_request=self._write_stop_request,
            drain_broker=self._drain,
            start_process=self._start,
            sleep=lambda seconds: self.sleeps.append(seconds),
        )


def healthy_world(root):
    """Guard, supervisor, broker and two children are running, with lock and discovery files."""
    world = FakeWorld(root)
    world.add_process(GUARD_PID, 'python -u service_guard.py --config cfg.json')
    world.add_process(SUPERVISOR_PID, 'python -B supervisor.py')
    world.add_process(BROKER_PID, 'python -B broker.py --control-stdin')
    world.add_process(CHILD_A_PID, 'python -u native_worker.py --recipient thread-a')
    world.add_process(CHILD_B_PID, 'python -u watch_session.py --pid 1')
    world.write_lock('service-guard.json', GUARD_PID)
    world.write_lock('supervisor-session.json', SUPERVISOR_PID)
    (Path(root) / 'broker-session.json.recovery-lock').write_text('', encoding='utf-8')  # kept by design
    (Path(root) / 'broker-session.json').write_text(json.dumps({'pid': BROKER_PID}), encoding='utf-8')
    return world


def write_services(root):
    config = Path(root) / 'services.json'
    config.write_text(json.dumps({'services': [
        {'script': 'native_worker.py', 'args': ['--recipient', 'thread-a'], 'pid': CHILD_A_PID, 'creation': None},
        {'script': 'watch_session.py', 'args': ['--pid', '1'], 'pid': CHILD_B_PID, 'creation': None},
    ]}), encoding='utf-8')
    return config


class ServiceStopTests(unittest.TestCase):
    def setUp(self):
        self._folder = tempfile.TemporaryDirectory()
        self.root = Path(self._folder.name)
        self.lines = []

    def tearDown(self):
        self._folder.cleanup()

    def say(self, text):
        self.lines.append(text)

    def test_correct_order_guard_supervisor_drain_children_cleanup(self):
        world = healthy_world(self.root)
        config = write_services(self.root)
        stopper = Stopper(self.root, world.controls(), services_config=config, say=self.say)
        self.assertTrue(stopper.run())
        self.assertEqual([('request', 'service-guard.stop-request'), ('request', 'supervisor.stop-request'),
                          ('drain', 120.0), ('exit', CHILD_A_PID), ('exit', CHILD_B_PID)], world.calls)
        self.assertEqual(['ok', 'ok', 'ok', 'ok', 'ok'], [step.status for step in stopper.steps])
        self.assertIn('Stop confirmed', self.lines[-1])

    def test_dry_run_acts_on_nothing(self):
        world = healthy_world(self.root)
        config = write_services(self.root)

        def forbidden(*args, **kwargs):
            raise AssertionError('dry run must not act')

        controls = Controls(
            read_pid=service_stop._default_read_pid,
            is_alive=lambda pid: pid in world.running,
            command_line=lambda pid: world.commands.get(pid),
            request_exit=forbidden,
            write_stop_request=forbidden,
            drain_broker=forbidden,
            start_process=forbidden,
            sleep=forbidden,
        )
        before = sorted(path.name for path in self.root.iterdir())
        result = main(['restart', '--dry-run', '--root', str(self.root),
                       '--services-config', str(config)], controls=controls, say=self.say)
        self.assertEqual(0, result)
        self.assertEqual(before, sorted(path.name for path in self.root.iterdir()))
        self.assertEqual([], world.calls)
        self.assertTrue(any('Dry run' in line for line in self.lines))
        self.assertTrue(any('start' in line for line in self.lines))

    def test_guard_failure_stops_before_supervisor(self):
        world = healthy_world(self.root)
        world.exits_on_request.discard(GUARD_PID)  # guard does not exit on request
        stopper = Stopper(self.root, world.controls(), exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual('failed', stopper.steps[0].status)
        self.assertEqual([('request', 'service-guard.stop-request')], world.calls)
        self.assertTrue(any('[not run] 2. supervisor' in line for line in self.lines))
        self.assertIn('Stop FAILED at step 1 (guard)', self.lines[-1])

    def test_guard_identity_mismatch_refuses_to_signal(self):
        world = healthy_world(self.root)
        world.commands[GUARD_PID] = 'some-other-program.exe --flag'
        stopper = Stopper(self.root, world.controls(), exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual([], world.calls)
        self.assertIn('refusing to signal', stopper.steps[0].detail)

    def test_drain_refusal_stops_and_reports_without_retry(self):
        world = healthy_world(self.root)
        world.drain_codes = [2, 0, 0]
        config = write_services(self.root)
        stopper = Stopper(self.root, world.controls(), services_config=config, say=self.say)
        self.assertFalse(stopper.run())
        drains = [call for call in world.calls if call[0] == 'drain']
        self.assertEqual(1, len(drains))
        self.assertNotIn(('exit', CHILD_A_PID), world.calls)
        self.assertEqual('failed', stopper.steps[2].status)
        self.assertIn('refused by broker_stop (exit 2)', stopper.steps[2].detail)
        self.assertIn('Stop FAILED at step 3 (broker drain)', self.lines[-1])

    def test_drain_timeout_is_not_retried(self):
        world = healthy_world(self.root)
        world.drain_codes = [1, 0]
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual(1, len([call for call in world.calls if call[0] == 'drain']))
        self.assertIn('timed out', stopper.steps[2].detail)

    def test_child_that_does_not_exit_stops_the_run(self):
        world = healthy_world(self.root)
        world.exits_on_request.discard(CHILD_A_PID)
        config = write_services(self.root)
        stopper = Stopper(self.root, world.controls(), services_config=config, exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        self.assertNotIn(('exit', CHILD_B_PID), world.calls)
        self.assertEqual('failed', stopper.steps[3].status)

    def test_remaining_children_are_stopped_in_config_order(self):
        world = healthy_world(self.root)
        config = write_services(self.root)
        stopper = Stopper(self.root, world.controls(), services_config=config, say=self.say)
        self.assertTrue(stopper.run())
        child_exits = [call for call in world.calls if call[0] == 'exit' and call[1] in (CHILD_A_PID, CHILD_B_PID)]
        self.assertEqual([('exit', CHILD_A_PID), ('exit', CHILD_B_PID)], child_exits)

    def test_cleanup_fails_when_a_live_lock_remains(self):
        world = healthy_world(self.root)
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertTrue(stopper.run())
        # A lock whose owner is still alive after the stop must fail the cleanup check.
        world.write_lock('supervisor-session.json', SUPERVISOR_PID)
        world.running.add(SUPERVISOR_PID)
        cleanup = stopper._cleanup()
        self.assertEqual('failed', cleanup.status)
        self.assertIn('supervisor pid 4002 is still running', cleanup.detail)

    def test_cleanup_fails_when_discovery_or_stop_marker_remains(self):
        world = healthy_world(self.root)
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertTrue(stopper.run())
        (self.root / 'broker-session.json').write_text(json.dumps({'pid': BROKER_PID}), encoding='utf-8')
        (self.root / 'broker-session.json.stop-pending').write_text('{}', encoding='utf-8')
        cleanup = stopper._cleanup()
        self.assertEqual('failed', cleanup.status)
        self.assertIn('broker-session.json is still present', cleanup.detail)
        self.assertIn('stop-pending', cleanup.detail)

    def test_stale_lock_from_a_dead_pid_is_explained_not_failed(self):
        world = healthy_world(self.root)
        world.write_lock('service-guard.json', 4999)  # pid 4999 is not in the process table
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertTrue(stopper.run())
        self.assertIn('stale', stopper.steps[-1].detail)

    def test_unreadable_lock_is_refused(self):
        world = healthy_world(self.root)
        (self.root / 'service-guard.json').write_text('not json', encoding='utf-8')
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual([], world.calls)
        self.assertEqual('failed', stopper.steps[0].status)

    def test_restart_starts_only_after_confirmed_stop(self):
        world = healthy_world(self.root)
        world.exits_on_request.discard(GUARD_PID)
        result = main(['restart', '--root', str(self.root), '--exit-timeout', '1'],
                      controls=world.controls(), say=self.say)
        self.assertEqual(1, result)
        self.assertEqual([], world.started)
        self.assertTrue(any('Stop FAILED' in line for line in self.lines))

    def test_restart_starts_broker_supervisor_guard_after_confirmed_stop(self):
        world = healthy_world(self.root)
        config = write_services(self.root)
        result = main(['restart', '--root', str(self.root), '--services-config', str(config)],
                      controls=world.controls(), say=self.say)
        self.assertEqual(0, result)
        self.assertEqual(['broker_launcher.py', 'supervisor.py', 'service_guard.py'], world.started)
        self.assertIn('Restart finished.', self.lines)

    def test_start_failure_is_reported_and_stops_the_start_sequence(self):
        world = healthy_world(self.root)
        world.start_exit_code = 1  # every launched process reports an exit at once
        result = main(['restart', '--root', str(self.root)], controls=world.controls(), say=self.say)
        self.assertEqual(1, result)
        # Broker start succeeds (discovery appears); the supervisor start then fails and the guard is never started.
        self.assertEqual(['broker_launcher.py', 'supervisor.py'], world.started)
        self.assertIn('Restart FAILED', self.lines[-1])

    def test_restart_without_config_reports_guard_not_started(self):
        world = healthy_world(self.root)
        stopper_ok = main(['restart', '--root', str(self.root)], controls=world.controls(), say=self.say)
        self.assertEqual(0, stopper_ok)
        self.assertTrue(any('guard was NOT started' in line for line in self.lines))

    def test_guard_timeout_is_bounded_reported_and_never_force_killed(self):
        world = healthy_world(self.root)
        world.exits_on_request.discard(GUARD_PID)
        stopper = Stopper(self.root, world.controls(), exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        step = stopper.steps[0]
        self.assertEqual('failed', step.status)
        self.assertIn('did not exit within 1 s', step.detail)
        self.assertIn('not force-killed', step.detail)
        # Only the request file was used: no exit or kill-style call for the guard.
        self.assertFalse(any(call[0] == 'exit' and call[1] == GUARD_PID for call in world.calls))
        self.assertIn(GUARD_PID, world.running)
        self.assertTrue((self.root / 'service-guard.stop-request').exists())
        self.assertLessEqual(sum(world.sleeps), 1.5)
        self.assertEqual([('request', 'service-guard.stop-request')], world.calls)
        self.assertNotIn(('drain', 120.0), world.calls)

    def test_supervisor_timeout_stops_run_before_broker_drain(self):
        world = healthy_world(self.root)
        world.exits_on_request.discard(SUPERVISOR_PID)
        stopper = Stopper(self.root, world.controls(), exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual('ok', stopper.steps[0].status)
        self.assertEqual('failed', stopper.steps[1].status)
        self.assertIn('did not exit within 1 s', stopper.steps[1].detail)
        self.assertIn(SUPERVISOR_PID, world.running)
        self.assertTrue((self.root / 'supervisor.stop-request').exists())
        self.assertNotIn(('exit', SUPERVISOR_PID), world.calls)
        self.assertNotIn(('drain', 120.0), world.calls)
        self.assertIn('[not run] 3. broker drain', self.lines)

    def test_request_write_error_is_reported_and_nothing_else_runs(self):
        world = healthy_world(self.root)
        world.request_error = PermissionError('denied')
        stopper = Stopper(self.root, world.controls(), exit_timeout=1, say=self.say)
        self.assertFalse(stopper.run())
        self.assertEqual('failed', stopper.steps[0].status)
        self.assertIn('could not make a stop-request file', stopper.steps[0].detail)
        self.assertIn('PermissionError', stopper.steps[0].detail)
        self.assertEqual([('request', 'service-guard.stop-request')], world.calls)
        self.assertEqual([], world.sleeps)

    def test_clean_stop_consumes_request_files(self):
        world = healthy_world(self.root)
        config = write_services(self.root)
        stopper = Stopper(self.root, world.controls(), services_config=config, say=self.say)
        self.assertTrue(stopper.run())
        self.assertFalse((self.root / 'service-guard.stop-request').exists())
        self.assertFalse((self.root / 'supervisor.stop-request').exists())

    def test_leftover_guard_request_fails_cleanup(self):
        world = healthy_world(self.root)
        stopper = Stopper(self.root, world.controls(), say=self.say)
        self.assertTrue(stopper.run())
        (self.root / 'service-guard.stop-request').write_text('stop\n', encoding='utf-8')
        cleanup = stopper._cleanup()
        self.assertEqual('failed', cleanup.status)
        self.assertIn('service-guard.stop-request is still present', cleanup.detail)

    def test_default_request_write_is_exclusive_and_idempotent(self):
        path = self.root / 'supervisor.stop-request'
        service_stop._default_write_stop_request(path)
        service_stop._default_write_stop_request(path)  # second call: already requested, no error
        self.assertEqual('stop\n', path.read_text(encoding='utf-8'))
        self.assertEqual(['supervisor.stop-request'], [p.name for p in self.root.iterdir()])

    def test_request_names_match_the_guard_and_supervisor_modules(self):
        import service_guard
        import supervisor
        self.assertEqual(service_guard.STOP_REQUEST.name, service_stop.GUARD_STOP_REQUEST)
        self.assertEqual(supervisor.STOP_REQUEST.name, service_stop.SUPERVISOR_STOP_REQUEST)


if __name__ == '__main__':
    unittest.main()
