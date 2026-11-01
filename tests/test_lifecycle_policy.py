"""Temporary snapshots, real shared recovery gates, and cooperating test processes.

No broker main, listener, configured data, process discovery or live services.
Controller evidence here is synthetic; these tests do not verify handle/writer
quiescence. Effects are counters/files only, never service starts or stops.
"""
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from broker import recovery_gate
import lifecycle_policy as policy


ROOT = Path(__file__).resolve().parents[1]


def identity():
    return str(uuid.uuid4())


class LifecyclePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='lifecycle-policy-tests-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / 'policy.json'
        self.gate_state = self.root / 'synthetic-state.json'
        self.policy_id, self.service_id = identity(), identity()
        self.store = self.new_store()

    def new_store(self, **kwargs):
        args = dict(policy_id=self.policy_id, service_id=self.service_id,
                    recovery_gate=lambda: recovery_gate(self.gate_state))
        args.update(kwargs)
        return policy.LifecyclePolicy(self.path, **args)

    def read(self):
        with self.store.locked() as locked:
            return locked.read()

    def bootstrap(self):
        with self.store.locked() as locked:
            return locked.bootstrap()

    def running(self):
        self.bootstrap()
        operation, generation = identity(), identity()
        with self.store.locked() as locked:
            claimed = locked.claim_launch(0, operation_id=operation, generation_id=generation,
                                          effect=lambda: None)
            return locked.mark_running(claimed['revision'], operation_id=operation, generation_id=generation)

    def pending(self, action='restart'):
        running = self.running()
        with self.store.locked() as locked:
            return locked.request(running['revision'], action=action, operation_id=identity(),
                                  generation_id=running['generation_id'], effect=lambda: None)

    @staticmethod
    def proof(record):
        return policy.StopEvidence(record['operation_id'], record['generation_id'], True, True, 0)

    def test_missing_policy_denies_then_explicit_bootstrap_cannot_reset(self):
        with self.store.locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.read()
            with self.assertRaises(policy.PolicyError):
                locked.bootstrap()  # Failed session cannot silently initialize.
        self.assertFalse(self.path.exists())
        initial = self.bootstrap()
        self.assertEqual((0, 'stopped', None), (initial['revision'], initial['state'], initial['action']))
        with self.store.locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.bootstrap()
        self.assertEqual(initial, self.read())

    def test_relative_policy_path_is_fixed_when_working_directory_changes(self):
        previous = Path.cwd()
        other = self.root / 'other'
        other.mkdir()
        try:
            os.chdir(self.root)
            store = policy.LifecyclePolicy(
                Path('policy.json'), policy_id=self.policy_id, service_id=self.service_id,
                recovery_gate=lambda: recovery_gate(self.gate_state))
            os.chdir(other)
            with store.locked() as locked:
                locked.bootstrap()
            self.assertTrue(self.path.is_file())
            self.assertFalse((other / 'policy.json').exists())
        finally:
            os.chdir(previous)

    def test_initial_launch_claim_is_committed_before_effect_under_same_gate(self):
        self.bootstrap()
        op, gen = identity(), identity()
        observed = []
        with self.store.locked() as locked:
            def launch():
                observed.append(locked.read())
                script = ('import sys; from pathlib import Path; from broker import recovery_gate; '
                          '\nwith recovery_gate(Path(sys.argv[1])): pass')
                result = subprocess.run([sys.executable, '-B', '-c', script, str(self.gate_state)],
                                        cwd=ROOT, capture_output=True, timeout=5)
                self.assertNotEqual(0, result.returncode)
            claimed = locked.claim_launch(0, operation_id=op, generation_id=gen, effect=launch)
        self.assertEqual([claimed], observed)
        self.assertEqual(('launch_claimed', 'start', op, gen),
                         tuple(claimed[k] for k in ('state', 'action', 'operation_id', 'generation_id')))
        effect = Mock()
        with self.store.locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.claim_launch(1, operation_id=identity(), generation_id=identity(), effect=effect)
        effect.assert_not_called()

    def test_stop_intent_precedes_effect_and_remains_stopped_without_auto_restart(self):
        running = self.running()
        observed = []
        with self.store.locked() as locked:
            pending = locked.request(2, action='stop', operation_id=identity(),
                                     generation_id=running['generation_id'],
                                     effect=lambda: observed.append(locked.read()))
            self.assertEqual([pending], observed)
            stopped = locked.complete(3, evidence=self.proof(pending))
        effect = Mock()
        with self.new_store().locked() as locked:
            self.assertEqual(stopped, locked.read())
            with self.assertRaises(policy.PolicyError):
                locked.claim_launch(4, operation_id=identity(), generation_id=identity(), effect=effect)
        effect.assert_not_called()

    def test_restart_needs_exact_proof_and_distinct_generation_before_claim(self):
        pending = self.pending()
        proof = self.proof(pending)
        with self.store.locked() as locked:
            ready = locked.complete(3, evidence=proof)
        self.assertEqual('restart_ready', ready['state'])
        new_generation = identity()
        observed = []
        with self.new_store().locked() as locked:
            claimed = locked.claim_launch(4, operation_id=pending['operation_id'], generation_id=new_generation,
                                          completion=proof, effect=lambda: observed.append(locked.read()))
            running = locked.mark_running(5, operation_id=pending['operation_id'], generation_id=new_generation)
        self.assertEqual([claimed], observed)
        self.assertEqual(pending['generation_id'], claimed['previous_generation_id'])
        self.assertEqual('running', running['state'])
        self.assertEqual(6, running['revision'])

    def test_dead_pid_missing_ack_wrong_operation_generation_and_nonzero_are_not_proof(self):
        pending = self.pending()
        good = self.proof(pending)
        invalid = [None, {'pid': 1234, 'dead': True}, replace(good, acknowledged=False),
                   replace(good, owned_exit_verified=False), replace(good, exit_code=1),
                   replace(good, exit_code=False), replace(good, acknowledged=1),
                   replace(good, operation_id=identity()), replace(good, generation_id=identity())]
        for evidence in invalid:
            with self.subTest(evidence=evidence), self.store.locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.complete(3, evidence=evidence)
            self.assertEqual(pending, self.read())

    def test_restart_ready_cannot_be_used_without_fresh_controller_evidence(self):
        pending = self.pending()
        good = self.proof(pending)
        with self.store.locked() as locked:
            locked.complete(3, evidence=good)
        for op, gen, evidence in [(identity(), identity(), good),
                                   (pending['operation_id'], pending['generation_id'], good),
                                   (pending['operation_id'], identity(), None)]:
            effect = Mock()
            with self.new_store().locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.claim_launch(4, operation_id=op, generation_id=gen,
                                        completion=evidence, effect=effect)
            effect.assert_not_called()

    def test_pending_is_sticky_across_reconstruction_and_invalid_transition_has_no_effect(self):
        pending = self.pending('stop')
        effect = Mock()
        for _ in range(2):
            with self.new_store().locked() as locked:
                self.assertEqual(pending, locked.read())
                with self.assertRaises(policy.PolicyError):
                    locked.request(3, action='restart', operation_id=identity(),
                                   generation_id=pending['generation_id'], effect=effect)
        effect.assert_not_called()
        self.assertEqual(pending, self.read())

    def test_revision_and_identity_validation_refuse_stale_or_omitted_identity(self):
        running = self.running()
        for revision, op, gen in [(1, identity(), running['generation_id']),
                                   (True, identity(), running['generation_id']),
                                   (-1, identity(), running['generation_id']),
                                   (2, None, running['generation_id']),
                                   (2, identity(), None), (2, identity(), identity()),
                                   (2, running['operation_id'], running['generation_id'])]:
            effect = Mock()
            with self.store.locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.request(revision, action='stop', operation_id=op, generation_id=gen, effect=effect)
            effect.assert_not_called()
        self.assertEqual(running, self.read())

    def test_mark_running_requires_exact_launch_operation_and_generation(self):
        self.bootstrap()
        op, gen = identity(), identity()
        with self.store.locked() as locked:
            locked.claim_launch(0, operation_id=op, generation_id=gen, effect=lambda: None)
        for wrong_op, wrong_gen in [(None, gen), (op, None), (identity(), gen), (op, identity())]:
            with self.store.locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.mark_running(1, operation_id=wrong_op, generation_id=wrong_gen)
        self.assertEqual('launch_claimed', self.read()['state'])

    def test_unknown_corrupt_foreign_and_ambiguous_policy_fail_closed(self):
        initial = self.bootstrap()
        invalid = [dict(initial, version=999), dict(initial, policy_id=identity()),
                   dict(initial, service_id=identity()), dict(initial, revision=True),
                   dict(initial, state='recover_anyway'), dict(initial, unknown=True),
                   dict(initial, revision=3, state='restart_ready', action='restart',
                        operation_id=identity(), generation_id=identity(), completion=None)]
        for record in invalid:
            self.path.write_bytes(policy._serialize(record))
            with self.subTest(record=record), self.store.locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.read()
        for raw in (b'', b'{', b'{}', b'x' * (policy.MAX_SNAPSHOT_BYTES + 1),
                    policy._serialize(initial).replace(b'"revision":0', b'"revision":1')):
            self.path.write_bytes(raw)
            with self.store.locked() as locked:
                with self.assertRaises(policy.PolicyError):
                    locked.read()

    def test_publication_failures_before_and_after_replace_never_invoke_action(self):
        for failure in ('fsync', 'link', 'replace_before', 'replace_after', 'directory_sync', 'witness_unlink'):
            with self.subTest(failure=failure):
                path = self.root / (failure + '.json')
                store = policy.LifecyclePolicy(path, policy_id=identity(), service_id=identity(),
                                               recovery_gate=lambda: recovery_gate(self.gate_state))
                with store.locked() as locked:
                    locked.bootstrap()
                original_replace = policy.os.replace
                original_unlink = Path.unlink
                def replace_then_fail(source, destination):
                    original_replace(source, destination)
                    raise OSError('synthetic failure after replacement')
                def fail_witness_unlink(path, *args, **kwargs):
                    if '.pending-' in path.name:
                        raise OSError('synthetic witness deletion failure')
                    return original_unlink(path, *args, **kwargs)
                if failure in ('fsync', 'link'):
                    injected = patch.object(policy.os, failure, side_effect=OSError('synthetic disk failure'))
                elif failure == 'replace_before':
                    injected = patch.object(policy.os, 'replace', side_effect=OSError('synthetic replace failure'))
                elif failure == 'replace_after':
                    injected = patch.object(policy.os, 'replace', side_effect=replace_then_fail)
                elif failure == 'directory_sync':
                    injected = patch.object(policy, '_sync_directory', side_effect=OSError('synthetic sync failure'))
                else:
                    injected = patch.object(Path, 'unlink', fail_witness_unlink)
                effect = Mock()
                with injected, store.locked() as locked:
                    with self.assertRaises(OSError):
                        locked.claim_launch(0, operation_id=identity(), generation_id=identity(), effect=effect)
                    with self.assertRaises(policy.PolicyError):
                        locked.read()  # Same session is poisoned after failure.
                effect.assert_not_called()
                with store.locked() as locked:
                    with self.assertRaises(policy.PolicyError):
                        locked.read()  # Other coordinator sees sticky witness.
                self.assertTrue(list(self.root.glob(path.name + '.pending-*')))

    def test_failed_stop_publication_and_failed_bootstrap_cannot_send_or_launch(self):
        running = self.running()
        effect = Mock()
        with patch.object(policy.os, 'fsync', side_effect=OSError('disk failure')), self.store.locked() as locked:
            with self.assertRaises(OSError):
                locked.request(2, action='stop', operation_id=identity(),
                               generation_id=running['generation_id'], effect=effect)
        effect.assert_not_called()
        other = policy.LifecyclePolicy(self.root / 'failed-bootstrap.json', policy_id=identity(),
                                      service_id=identity(), recovery_gate=lambda: recovery_gate(self.gate_state))
        with patch.object(policy.os, 'fsync', side_effect=OSError('disk failure')), other.locked() as locked:
            with self.assertRaises(OSError):
                locked.bootstrap()
        with other.locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.bootstrap()  # No implicit repair of partial initialization.

    def test_completion_replaced_then_failure_does_not_authorize_restart(self):
        pending = self.pending()
        proof = self.proof(pending)
        original_replace = policy.os.replace
        def replace_then_fail(source, destination):
            original_replace(source, destination)
            raise OSError('synthetic error after ready snapshot replacement')
        with patch.object(policy.os, 'replace', side_effect=replace_then_fail), self.store.locked() as locked:
            with self.assertRaises(OSError):
                locked.complete(3, evidence=proof)
        effect = Mock()
        with self.new_store().locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.claim_launch(4, operation_id=pending['operation_id'], generation_id=identity(),
                                    completion=proof, effect=effect)
        effect.assert_not_called()
        self.assertTrue(list(self.root.glob(self.path.name + '.pending-*')))

    def test_actual_short_write_leaves_uncertainty_artifact_and_never_invokes_effect(self):
        self.bootstrap()
        original_fdopen = policy.os.fdopen
        class ShortWriter:
            def __init__(self, stream):
                self.stream = stream
            def write(self, raw):
                return self.stream.write(raw[:len(raw) // 2])
        @contextmanager
        def short_fdopen(*args, **kwargs):
            with original_fdopen(*args, **kwargs) as stream:
                yield ShortWriter(stream)
        effect = Mock()
        with patch.object(policy.os, 'fdopen', short_fdopen), self.store.locked() as locked:
            with self.assertRaises(OSError):
                locked.claim_launch(0, operation_id=identity(), generation_id=identity(), effect=effect)
        effect.assert_not_called()
        with self.new_store().locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.read()

    def test_effect_error_leaves_claim_and_does_not_allow_retry(self):
        self.bootstrap()
        effect = Mock(side_effect=RuntimeError('synthetic launch outcome unknown'))
        with self.store.locked() as locked:
            with self.assertRaises(RuntimeError):
                locked.claim_launch(0, operation_id=identity(), generation_id=identity(), effect=effect)
            with self.assertRaises(policy.PolicyError):
                locked.read()
        effect.assert_called_once()
        claimed = self.read()
        again = Mock()
        with self.new_store().locked() as locked:
            with self.assertRaises(policy.PolicyError):
                locked.claim_launch(claimed['revision'], operation_id=identity(), generation_id=identity(), effect=again)
        again.assert_not_called()

    def test_session_cannot_be_used_after_shared_gate_is_released(self):
        self.bootstrap()
        with self.store.locked() as locked:
            locked.read()
        effect = Mock()
        with self.assertRaises(policy.PolicyError):
            locked.claim_launch(0, operation_id=identity(), generation_id=identity(), effect=effect)
        effect.assert_not_called()

    def test_two_cooperating_processes_cannot_both_claim_same_revision(self):
        running = self.running()
        script = '''
import sys,time
from pathlib import Path
from broker import recovery_gate
from lifecycle_policy import LifecyclePolicy
path,state,pid,sid,op,gen,output=sys.argv[1:]
p=LifecyclePolicy(path,policy_id=pid,service_id=sid,recovery_gate=lambda: recovery_gate(Path(state)))
print('ready',flush=True)
sys.stdin.readline()
try:
    with p.locked() as s:
        def effect():
            Path(output).write_text(op)
            time.sleep(.15)
        s.request(2,action='restart',operation_id=op,generation_id=gen,effect=effect)
except (OSError,RuntimeError):
    sys.exit(3)
'''
        children = []
        try:
            for index in range(2):
                operation = identity()
                output = self.root / ('effect-%d' % index)
                child = subprocess.Popen([sys.executable, '-B', '-u', '-c', script, str(self.path),
                    str(self.gate_state), self.policy_id, self.service_id, operation,
                    running['generation_id'], str(output)], cwd=ROOT, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                children.append(child)
                self.assertEqual('ready', child.stdout.readline().strip())
            for child in children:
                child.stdin.write('go\n')
                child.stdin.flush()
            for child in children:
                child.communicate(timeout=5)
            self.assertEqual([0, 3], sorted(c.returncode for c in children))
            effects = list(self.root.glob('effect-*'))
            self.assertEqual(1, len(effects))
            pending = self.read()
            self.assertEqual((3, 'pending', effects[0].read_text()),
                             (pending['revision'], pending['state'], pending['operation_id']))
        finally:
            for child in children:
                if child.stdin and not child.stdin.closed:
                    child.stdin.close()
                child.wait(timeout=5)
                child.stdout.close()
                child.stderr.close()


if __name__ == '__main__':
    unittest.main()
