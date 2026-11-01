"""Real children, private pipes and temporary databases; no live bridge access."""
import os
import unittest

from synthetic_stop import SyntheticStack


class SyntheticStopTests(unittest.TestCase):
    def test_real_stack_exits_in_order_and_is_idempotent(self):
        with SyntheticStack() as stack:
            children = [stack.launch(role) for role in ('broker', 'worker', 'supervisor', 'guard')]
            self.assertTrue(all(c.workspace.is_dir() for c in children))
            self.assertTrue((children[0].workspace / 'synthetic.sqlite3').is_file())
            if os.name == 'nt':
                self.assertTrue(all(type(c.creation) is int and c.creation > 0 for c in children))
            self.assertEqual([(r, 'stopped') for r in ('guard', 'supervisor', 'worker', 'broker')], stack.stop())
            self.assertTrue(all(c.process.poll() == 0 for c in children))
            self.assertTrue(all(not c.workspace.exists() for c in children))
            self.assertTrue(all(status == 'already_stopped' for _, status in stack.stop()))

    def test_identity_mismatch_preflight_leaves_all_running(self):
        with SyntheticStack() as stack:
            guard = stack.launch('guard')
            broker = stack.launch('broker')
            original = broker.identity
            for field, value in [('pid', guard.process.pid), ('creation', -1),
                                 ('token', 'wrong'), ('role', 'chat')]:
                broker.identity = {**original, field: value}
                with self.assertRaises(RuntimeError):
                    stack.stop()
                self.assertIsNone(guard.process.poll())
                self.assertIsNone(broker.process.poll())
                self.assertFalse(guard.requested)
            broker.identity = original
            stack.stop()

    def test_timeout_prevents_later_stop_then_retry_finishes(self):
        with SyntheticStack() as stack:
            guard = stack.launch('guard', delay=0.5)
            broker = stack.launch('broker')
            self.assertEqual([('guard', 'timeout')], stack.stop(timeout=0.05))
            self.assertIsNone(broker.process.poll())
            self.assertFalse(broker.requested)
            self.assertEqual([('guard', 'stopped'), ('broker', 'stopped')], stack.stop(timeout=3))
            self.assertEqual(0, guard.process.returncode)

    def test_other_stack_is_not_stopped(self):
        with SyntheticStack() as first, SyntheticStack() as second:
            own = first.launch('worker')
            unrelated = second.launch('worker')
            first.stop()
            self.assertEqual(0, own.process.poll())
            self.assertIsNone(unrelated.process.poll())
            self.assertFalse(unrelated.requested)
            second.stop()

    def test_child_rejects_bad_stop_identity(self):
        with SyntheticStack() as stack:
            child = stack.launch('worker')
            child._send({'op': 'stop', 'identity': {**child.identity, 'token': 'wrong'}})
            self.assertEqual({'error': 'identity mismatch'}, child._receive(3))
            self.assertIsNone(child.process.poll())
            stack.stop()

    def test_eof_cooperatively_cleans_up_without_signals(self):
        stack = SyntheticStack()
        child = stack.launch('broker')
        stack.close()
        self.assertEqual(0, child.process.poll())
        self.assertFalse(child.workspace.exists())


if __name__ == '__main__':
    unittest.main()
