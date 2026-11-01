"""Real synthetic pipe children + temporary journals only; no configured services."""
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import lifecycle_intent_prototype as life


ROOT = Path(__file__).resolve().parents[1]


class LifecycleIntentPrototypeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='lifecycle-intent-tests-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / 'journal'
        self.coordinator = life.LifecyclePrototype.create(self.directory)
        self.addCleanup(self.coordinator.close_test_children)

    def start(self, **kwargs):
        target = self.coordinator.start_initial(**kwargs)
        return self.coordinator._owned[-1], target

    def assert_denied(self, callback):
        with self.assertRaises((life.LifecycleError, OSError)):
            callback()

    def test_missing_legacy_policy_and_empty_directory_never_authorize_launch(self):
        for name in ('missing', 'empty'):
            directory = self.root / name
            if name == 'empty':
                directory.mkdir()
            with patch.object(life.subprocess, 'Popen') as launch:
                self.assert_denied(lambda: life.LifecyclePrototype(directory))
                launch.assert_not_called()
        with self.assertRaises(FileExistsError):
            life.LifecyclePrototype.create(self.directory)

    def test_pending_intent_is_committed_before_stop_command(self):
        child, target = self.start()
        original_send = child.send
        observed = []
        def send(command):
            if command['op'] == 'stop':
                # Called while coordinator owns the gate: inspect through the
                # same journal connection, not a second lock acquisition.
                record = self.coordinator.journal.load()[0]
                self.assertEqual(('pending', 'stop', target),
                                 (record['state'], record['action'], record['target']))
                observed.append(record['operation'])
            original_send(command)
        with patch.object(child, 'send', side_effect=send):
            self.assertEqual('stopped', self.coordinator.request('stop'))
        self.assertEqual(1, len(observed))
        self.assertEqual(0, child.process.poll())
        self.assertEqual(observed[0], self.coordinator.snapshot()['operation'])
        for entry in self.directory.glob('*.json'):
            self.assertNotIn(child.token, entry.read_text())

    def test_stop_remains_stopped_across_coordinator_restart_no_auto_restart(self):
        self.start()
        self.coordinator.request('stop')
        with patch.object(life.subprocess, 'Popen') as launch:
            restored = life.LifecyclePrototype(self.directory)
            record = restored.snapshot()
            self.assertEqual(('stopped', 'stop'), (record['state'], record['action']))
            self.assert_denied(restored.start_initial)
            self.assert_denied(lambda: restored.restart_explicitly(record['operation']))
            self.assert_denied(lambda: restored.request('restart'))
            launch.assert_not_called()

    def test_explicit_restart_requires_exit_and_launch_claim_before_replacement(self):
        old, target = self.start()
        self.assertEqual('restart_ready', self.coordinator.request('restart'))
        record = self.coordinator.snapshot()
        self.assertEqual(0, old.process.poll())
        self.assert_denied(lambda: self.coordinator.restart_explicitly('wrong-operation'))
        real_factory = life._OwnedTestChild
        def launch_child(**kwargs):
            claimed = self.coordinator.journal.load()[0]
            self.assertEqual(('launch_claimed', 'restart'), (claimed['state'], claimed['action']))
            self.assertEqual(record['operation'], claimed['operation'])
            self.assertEqual(0, old.process.poll())
            return real_factory(**kwargs)
        with patch.object(life, '_OwnedTestChild', side_effect=launch_child):
            replacement = self.coordinator.restart_explicitly(record['operation'])
        self.assertNotEqual(target['instance_sha256'], replacement['instance_sha256'])
        self.assertEqual('running', self.coordinator.snapshot()['state'])
        self.assertEqual('stopped', self.coordinator.request('stop'))

    def test_completed_restart_is_not_replayed_by_new_coordinator_without_handle(self):
        self.start()
        self.coordinator.request('restart')
        record = self.coordinator.snapshot()
        restored = life.LifecyclePrototype(self.directory)
        with patch.object(life.subprocess, 'Popen') as launch:
            self.assert_denied(lambda: restored.restart_explicitly(record['operation']))
            launch.assert_not_called()
        self.assertEqual('restart_ready', restored.snapshot()['state'])

    def test_real_child_marker_failure_keeps_intent_despite_process_exit(self):
        child, _ = self.start(marker_failure=True)
        with self.assertRaisesRegex(life.LifecycleError, 'nonzero'):
            self.coordinator.request('restart')
        self.assertEqual(3, child.process.poll())
        pending = self.coordinator.snapshot()
        self.assertEqual(('pending', 'restart'), (pending['state'], pending['action']))
        self.assert_denied(lambda: self.coordinator.finish(pending['operation']))
        with patch.object(life.subprocess, 'Popen') as launch:
            restored = life.LifecyclePrototype(self.directory)
            self.assertEqual(pending, restored.snapshot())
            self.assert_denied(lambda: restored.finish(pending['operation']))
            self.assert_denied(lambda: restored.restart_explicitly(pending['operation']))
            launch.assert_not_called()

    def test_journal_fsync_link_and_directory_failures_refuse_stop(self):
        # Separate coordinators preserve each failed journal as evidence.
        for failure in ('fsync', 'link', 'directory'):
            with self.subTest(failure=failure):
                coordinator = life.LifecyclePrototype.create(self.root / ('failure-' + failure))
                self.addCleanup(coordinator.close_test_children)
                coordinator.start_initial()
                child = coordinator._owned[-1]
                sent = []
                original = child.send
                def send(command):
                    sent.append(command['op'])
                    return original(command)
                failing_patch = (patch.object(life, '_sync_directory', side_effect=OSError('disk failure'))
                                 if failure == 'directory' else
                                 patch.object(life.os, failure, side_effect=OSError('disk failure')))
                with failing_patch, patch.object(child, 'send', side_effect=send):
                    self.assert_denied(lambda: coordinator.request('stop'))
                self.assertNotIn('stop', sent)
                self.assertIsNone(child.process.poll())
                self.assertFalse(child.requested)
                self.assert_denied(lambda: life.LifecyclePrototype(coordinator.journal.directory))

    def test_short_write_failure_refuses_stop(self):
        child, _ = self.start()
        original_open = Path.open
        class ShortWriter:
            def __init__(self, stream):
                self.stream = stream
            def write(self, raw):
                return self.stream.write(raw[:len(raw) // 2])
        @contextmanager
        def partial_write(path, mode='r', *args, **kwargs):
            with original_open(path, mode, *args, **kwargs) as stream:
                yield ShortWriter(stream) if mode == 'xb' and path.name.startswith('pending-') else stream
        with patch.object(Path, 'open', partial_write), \
                patch.object(child, 'send', wraps=child.send) as send:
            self.assert_denied(lambda: self.coordinator.request('stop'))
            self.assertNotIn('stop', [call.args[0]['op'] for call in send.call_args_list])
        self.assertIsNone(child.process.poll())
        self.assertEqual(1, len(list(self.directory.glob('pending-*.tmp'))))
        self.assert_denied(self.coordinator.snapshot)

    def test_failed_stop_delivery_then_eof_stays_pending_despite_exact_owned_zero_exit(self):
        child, _ = self.start()
        original = child.send
        def fail_stop(command):
            if command['op'] == 'stop':
                raise BrokenPipeError('synthetic stop delivery failure')
            return original(command)
        with patch.object(child, 'send', side_effect=fail_stop):
            self.assert_denied(lambda: self.coordinator.request('stop'))
        pending = self.coordinator.snapshot()
        self.assertEqual('pending', pending['state'])
        self.assert_denied(lambda: self.coordinator.finish('wrong-operation'))
        self.assertEqual('pending', self.coordinator.finish(pending['operation']))
        restored = life.LifecyclePrototype(self.directory)
        self.assert_denied(lambda: restored.finish(pending['operation']))
        # Separately prove exit through the ORIGINAL retained process handle.
        # This is test-only EOF cleanup, no force-kill or PID attachment.
        child.process.stdin.close()
        self.assertEqual(0, child.process.wait(timeout=5))
        self.assertEqual('pending', self.coordinator.finish(pending['operation']))
        self.assertEqual(('pending', 'stop'), (self.coordinator.snapshot()['state'],
                                              self.coordinator.snapshot()['action']))
        self.assert_denied(lambda: self.coordinator.restart_explicitly(pending['operation']))

    def test_stop_bytes_delivered_then_send_failure_requires_ack_and_exit(self):
        for mode in ('normal', 'omit'):
            with self.subTest(mode=mode):
                coordinator = life.LifecyclePrototype.create(self.root / ('delivered-' + mode))
                self.addCleanup(coordinator.close_test_children)
                coordinator.start_initial(stop_delay=.5, ack_mode=mode)
                child = coordinator._owned[-1]
                original_send = child.send
                def deliver_then_fail(command):
                    original_send(command)  # Real pipe write AND flush first.
                    if command['op'] == 'stop':
                        raise BrokenPipeError('synthetic failure AFTER delivery')
                with patch.object(child, 'send', side_effect=deliver_then_fail):
                    self.assert_denied(lambda: coordinator.request('restart'))
                pending = coordinator.snapshot()
                self.assertEqual('pending', pending['state'])
                self.assertTrue(child.requested)
                self.assertEqual('pending', coordinator.finish(pending['operation']))
                self.assert_denied(lambda: coordinator.restart_explicitly(pending['operation']))
                self.assertEqual(0, child.process.wait(timeout=5))
                if mode == 'normal':
                    self.assertEqual('restart_ready', coordinator.finish(pending['operation']))
                else:
                    self.assertEqual('pending', coordinator.finish(pending['operation']))
                    self.assert_denied(lambda: coordinator.restart_explicitly(pending['operation']))

    def test_delayed_reader_ack_does_not_cache_missing_proof_or_complete_on_exit(self):
        child, _ = self.start()
        acknowledgement_arrived = threading.Event()
        release_reader = threading.Event()
        original_put = child.messages.put
        def delayed_put(value, *args, **kwargs):
            if isinstance(value, dict) and value.get('event') == 'stop_complete':
                acknowledgement_arrived.set()
                if not release_reader.wait(10):
                    raise RuntimeError('test did not release acknowledgement reader')
            return original_put(value, *args, **kwargs)
        try:
            with patch.object(child.messages, 'put', side_effect=delayed_put):
                self.assertEqual('pending', self.coordinator.request('restart'))
                self.assertTrue(acknowledgement_arrived.wait(2))
                self.assertEqual(0, child.process.poll())
                pending = self.coordinator.snapshot()
                self.assertEqual('pending', self.coordinator.finish(pending['operation']))
                self.assertFalse(child._completion_checked)
                self.assert_denied(lambda: self.coordinator.restart_explicitly(pending['operation']))
                if os.name == 'nt':
                    self.assertFalse(child.process._handle.closed)
                release_reader.set()
                child.reader.join(timeout=2)
                self.assertFalse(child.reader.is_alive())
                self.assertEqual('restart_ready', self.coordinator.finish(pending['operation']))
        finally:
            release_reader.set()

    def test_real_delayed_timeout_requires_ack_and_exit_before_restart_ready(self):
        child, _ = self.start(stop_delay=.5)
        self.assertEqual('pending', self.coordinator.request('restart', timeout=.01))
        operation = self.coordinator.snapshot()['operation']
        self.assertIsNone(child.process.poll())
        self.assertEqual('pending', self.coordinator.finish(operation))
        self.assert_denied(lambda: self.coordinator.restart_explicitly(operation))
        self.assertEqual(0, child.process.wait(timeout=5))
        self.assertEqual('restart_ready', self.coordinator.finish(operation))
        self.assertTrue(child.completion_verified(operation))
        self.coordinator.restart_explicitly(operation)
        self.assertEqual('stopped', self.coordinator.request('stop'))

    def test_missing_wrong_operation_or_wrong_identity_ack_never_completes(self):
        for mode in ('omit', 'wrong_operation', 'wrong_identity'):
            with self.subTest(mode=mode):
                coordinator = life.LifecyclePrototype.create(self.root / ('ack-' + mode))
                self.addCleanup(coordinator.close_test_children)
                coordinator.start_initial(ack_mode=mode)
                child = coordinator._owned[-1]
                self.assertEqual('pending', coordinator.request('restart'))
                self.assertEqual(0, child.process.poll())
                record = coordinator.snapshot()
                self.assertEqual('pending', coordinator.finish(record['operation']))
                with patch.object(life.subprocess, 'Popen') as launch:
                    self.assert_denied(lambda: coordinator.restart_explicitly(record['operation']))
                    launch.assert_not_called()
                self.assertEqual('pending', coordinator.snapshot()['state'])

    def test_failed_restart_delivery_then_idle_lease_zero_exit_remains_pending(self):
        child, _ = self.start(idle_lease=.5)
        original = child.send
        def fail_stop(command):
            if command['op'] == 'stop':
                raise BrokenPipeError('synthetic undelivered restart')
            return original(command)
        with patch.object(child, 'send', side_effect=fail_stop):
            self.assert_denied(lambda: self.coordinator.request('restart'))
        record = self.coordinator.snapshot()
        self.assertEqual(0, child.process.wait(timeout=5))
        self.assertEqual('pending', self.coordinator.finish(record['operation']))
        self.assert_denied(lambda: self.coordinator.restart_explicitly(record['operation']))
        self.assertEqual(record, self.coordinator.snapshot())

    def test_exact_completion_ack_followed_by_nonzero_exit_does_not_authorize_restart(self):
        child, _ = self.start(ack_mode='fail_after_ack')
        with self.assertRaisesRegex(life.LifecycleError, 'nonzero'):
            self.coordinator.request('restart')
        record = self.coordinator.snapshot()
        acknowledgement = child.messages.get(timeout=2)
        self.assertEqual({'event': 'stop_complete', 'operation': record['operation'],
                          'identity': child.identity}, acknowledgement)
        self.assertEqual(4, child.process.poll())
        self.assert_denied(lambda: self.coordinator.finish(record['operation']))
        self.assert_denied(lambda: self.coordinator.restart_explicitly(record['operation']))
        self.assertEqual('pending', self.coordinator.snapshot()['state'])

    def test_handshake_failure_reaps_only_just_spawned_child_and_retains_inventory(self):
        other = life.LifecyclePrototype.create(self.root / 'unrelated')
        self.addCleanup(other.close_test_children)
        other.start_initial()
        with patch.object(life._OwnedTestChild, 'receive', return_value={'wrong': 'handshake'}):
            with self.assertRaisesRegex(life.LifecycleError, 'startup identity'):
                self.coordinator.start_initial()
        self.assertEqual(1, len(self.coordinator._owned))
        failed = self.coordinator._owned[0]
        self.assertIsNotNone(failed.process.poll())
        self.assertTrue(failed.process.stdin.closed)
        self.assertTrue(failed.process.stdout.closed)
        self.assertFalse(failed.reader.is_alive())
        self.assertIsNone(other._owned[0].process.poll())
        self.assertFalse(other._owned[0].requested)
        self.assertEqual('launch_claimed', self.coordinator.snapshot()['state'])

    @unittest.skipUnless(os.name == 'nt', 'retained Windows handle creation lookup')
    def test_creation_time_lookup_failure_closes_owned_child_before_handshake(self):
        with patch.object(life, 'creation_time', side_effect=OSError('synthetic lookup failure')):
            self.assert_denied(self.coordinator.start_initial)
        self.assertEqual(1, len(self.coordinator._owned))
        child = self.coordinator._owned[0]
        self.assertIsNotNone(child.process.poll())
        self.assertTrue(child.process.stdin.closed)
        self.assertTrue(child.process.stdout.closed)
        self.assertIsNone(child.reader)
        self.assertEqual('launch_claimed', self.coordinator.snapshot()['state'])

    def test_construction_cleanup_failure_retains_exact_owner_for_retry(self):
        real_close = life._OwnedTestChild.close
        with patch.object(life._OwnedTestChild, 'receive', return_value={'wrong': 'handshake'}), \
                patch.object(life._OwnedTestChild, 'close', side_effect=OSError('synthetic cleanup failure')):
            with self.assertRaises(life.ChildConstructionError) as failure:
                self.coordinator.start_initial()
        self.assertEqual(1, len(self.coordinator._owned))
        self.assertIs(failure.exception.child, self.coordinator._owned[0])
        real_close(failure.exception.child)
        self.assertIsNotNone(failure.exception.child.process.poll())
        self.assertTrue(failure.exception.child.process.stdout.closed)

    def test_teardown_attempts_all_children_and_aggregates_multiple_failures(self):
        first, _ = self.start()
        self.coordinator.request('restart')
        self.coordinator.restart_explicitly(self.coordinator.snapshot()['operation'])
        second = self.coordinator._owned[-1]
        self.coordinator.request('restart')
        self.coordinator.restart_explicitly(self.coordinator.snapshot()['operation'])
        third = self.coordinator._owned[-1]
        first_failure, second_failure = OSError('first test failure'), RuntimeError('second test failure')
        with patch.object(first, 'close', side_effect=first_failure) as first_close, \
                patch.object(second, 'close', side_effect=second_failure) as second_close, \
                patch.object(third, 'close', wraps=third.close) as third_close:
            with self.assertRaises(life.TestTeardownError) as failure:
                self.coordinator.close_test_children()
            first_close.assert_called_once()
            second_close.assert_called_once()
            third_close.assert_called_once()
        self.assertEqual(((first, first_failure), (second, second_failure)), failure.exception.failures)
        self.assertTrue(third._teardown_complete)
        self.assertIsNotNone(third.process.poll())
        if os.name == 'nt':
            self.assertFalse(first.process._handle.closed)
            self.assertFalse(second.process._handle.closed)
            self.assertTrue(third.process._handle.closed)
        self.coordinator.close_test_children()  # Retry retained failed owners.
        self.assertTrue(all(child._teardown_complete for child in self.coordinator._owned))

    @unittest.skipUnless(os.name == 'nt', 'Windows retained Popen handles')
    def test_final_teardown_releases_handle_but_reconciliation_preserves_it(self):
        child, _ = self.start(stop_delay=.2)
        self.assertEqual('pending', self.coordinator.request('restart', timeout=.01))
        operation = self.coordinator.snapshot()['operation']
        self.assertFalse(child.process._handle.closed)
        self.assertEqual(0, child.process.wait(timeout=5))
        self.assertFalse(child.process._handle.closed)
        self.assertEqual(child.creation, life.creation_time(int(child.process._handle)))
        self.assertEqual('restart_ready', self.coordinator.finish(operation))
        self.assertFalse(child.process._handle.closed)
        self.coordinator.close_test_children()
        self.assertTrue(child.process._handle.closed)
        self.assertTrue(child._teardown_complete)
        with patch.object(life.subprocess, 'Popen') as launch:
            self.assert_denied(lambda: self.coordinator.restart_explicitly(operation))
            launch.assert_not_called()
        self.assert_denied(child.verify)
        self.coordinator.close_test_children()  # No double-close or reused handle.

    @unittest.skipUnless(os.name == 'nt', 'Windows retained Popen handles')
    def test_failed_final_teardown_retains_handle_for_cleanup_retry(self):
        child, _ = self.start()
        with patch.object(child.process, 'wait', side_effect=subprocess.TimeoutExpired('owned test', 12)):
            with self.assertRaises(life.TestTeardownError) as error:
                self.coordinator.close_test_children()
        self.assertIs(child, error.exception.failures[0][0])
        self.assertFalse(child._teardown_complete)
        self.assertFalse(child.process._handle.closed)
        self.coordinator.close_test_children()
        self.assertTrue(child.process._handle.closed)

    def test_identity_mismatch_never_sends_stop_or_changes_policy(self):
        child, _ = self.start()
        previous = self.coordinator.snapshot()
        real_identity = child.identity
        child.identity = dict(real_identity, pid=real_identity['pid'] + 1)
        with patch.object(child, 'send', wraps=child.send) as send:
            self.assert_denied(lambda: self.coordinator.request('stop'))
            send.assert_not_called()
        self.assertEqual(previous, self.coordinator.snapshot())
        child.identity = real_identity

    def test_unexpected_zero_exit_is_not_restart_authority(self):
        child, _ = self.start()
        child.process.stdin.close()
        self.assertEqual(0, child.process.wait(timeout=5))
        with patch.object(life.subprocess, 'Popen') as launch:
            self.assert_denied(lambda: self.coordinator.request('restart'))
            restored = life.LifecyclePrototype(self.directory)
            self.assertEqual('running', restored.snapshot()['state'])
            self.assert_denied(restored.start_initial)
            launch.assert_not_called()

    def test_incomplete_restart_launch_claim_prevents_duplicate_launch(self):
        self.start()
        self.coordinator.request('restart')
        operation = self.coordinator.snapshot()['operation']
        with patch.object(life, '_OwnedTestChild', side_effect=OSError('synthetic launch failure')):
            self.assert_denied(lambda: self.coordinator.restart_explicitly(operation))
        self.assertEqual('launch_claimed', self.coordinator.snapshot()['state'])
        restored = life.LifecyclePrototype(self.directory)
        with patch.object(life.subprocess, 'Popen') as launch:
            self.assert_denied(lambda: restored.restart_explicitly(operation))
            self.assert_denied(restored.start_initial)
            launch.assert_not_called()

    def test_launch_acknowledgement_write_failure_does_not_relaunch(self):
        original_append = self.coordinator.journal.append
        def fail_running(payload, **kwargs):
            if payload['state'] == 'running':
                raise OSError('synthetic acknowledgement failure')
            return original_append(payload, **kwargs)
        with patch.object(self.coordinator.journal, 'append', side_effect=fail_running):
            self.assert_denied(self.coordinator.start_initial)
        self.assertEqual(1, len(self.coordinator._owned))
        self.assertIsNone(self.coordinator._owned[0].process.poll())
        self.assertEqual('launch_claimed', self.coordinator.snapshot()['state'])
        with patch.object(life.subprocess, 'Popen') as launch:
            restored = life.LifecyclePrototype(self.directory)
            self.assert_denied(restored.start_initial)
            launch.assert_not_called()

    def test_malformed_or_incomplete_policy_is_sticky_without_ttl(self):
        (self.directory / '00000001.json').write_bytes(b'{')
        for _ in range(2):
            self.assert_denied(lambda: life.LifecyclePrototype(self.directory))
        self.assertEqual(b'{', (self.directory / '00000001.json').read_bytes())

    def test_gate_excludes_other_coordinator_process(self):
        script = 'import sys; from lifecycle_intent_prototype import LifecyclePrototype; LifecyclePrototype(sys.argv[1])'
        with self.coordinator.journal.gate():
            result = subprocess.run([sys.executable, '-B', '-c', script, str(self.directory)],
                                    cwd=ROOT, capture_output=True, timeout=5)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual('empty', self.coordinator.snapshot()['state'])

    def test_real_coordinator_crash_preserves_intent_and_does_not_adopt_pid(self):
        directory = self.root / 'crashed-coordinator'
        script = '''
import os, sys
from lifecycle_intent_prototype import LifecyclePrototype
c = LifecyclePrototype.create(sys.argv[1])
c.start_initial()
child = c._owned[-1]
original = child.send
def fail_stop(command):
    if command['op'] == 'stop':
        raise BrokenPipeError('test only')
    return original(command)
child.send = fail_stop
try:
    c.request('restart')
except BrokenPipeError:
    os._exit(7)
os._exit(9)
'''
        result = subprocess.run([sys.executable, '-B', '-c', script, str(directory)],
                                cwd=ROOT, capture_output=True, timeout=10)
        self.assertEqual(7, result.returncode, result.stderr)
        with patch.object(life.subprocess, 'Popen') as launch:
            restored = life.LifecyclePrototype(directory)
            record = restored.snapshot()
            self.assertEqual(('pending', 'restart'), (record['state'], record['action']))
            self.assert_denied(lambda: restored.finish(record['operation']))
            self.assert_denied(lambda: restored.restart_explicitly(record['operation']))
            launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
