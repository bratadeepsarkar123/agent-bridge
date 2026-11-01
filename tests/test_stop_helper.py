"""Pure simulation tests: no OS process inspection, launch, signals, or sleeps."""
from dataclasses import replace
import unittest
from unittest.mock import Mock

from stop_helper import ServiceIdentity, SimulatedRuntime, stop_services


class StopHelperTests(unittest.TestCase):
    def stack(self):
        runtime = SimulatedRuntime()
        services = [runtime.add(role, role, i, 100 + i)
                    for i, role in enumerate(('broker', 'worker', 'supervisor', 'guard'), 1)]
        return runtime, services

    def test_restart_authorities_exit_before_workers_and_broker(self):
        runtime, services = self.stack()
        result = stop_services(runtime, services)
        self.assertEqual(['guard', 'supervisor', 'worker', 'broker'],
                         [r['service'] for r in result])
        self.assertTrue(all(r['status'] == 'stopped' for r in result))
        self.assertEqual([('request_stop', name) for name in
                          ('guard', 'supervisor', 'worker', 'broker')],
                         [event for event in runtime.events if event[0] == 'request_stop'])
        for name in ('guard', 'supervisor', 'worker', 'broker'):
            offset = runtime.events.index(('request_stop', name))
            self.assertEqual(('wait', name, 5), runtime.events[offset + 1])
            self.assertEqual(('exited', name), runtime.events[offset + 2])

    def test_timeout_never_stops_later_dependencies(self):
        runtime = SimulatedRuntime()
        guard = runtime.add('guard', 'guard', 1, 1, exit_delay=float('inf'))
        broker = runtime.add('broker', 'broker', 2, 2)
        self.assertEqual([{'service': 'guard', 'status': 'timeout'}],
                         stop_services(runtime, [broker, guard], timeout=1))
        self.assertTrue(runtime._resolve(broker).running)
        self.assertFalse(runtime._resolve(broker).stop_requested)

    def test_retry_waits_without_duplicate_shutdown_request(self):
        runtime = SimulatedRuntime()
        service = runtime.add('worker', 'worker', 10, 20, exit_delay=3)
        self.assertEqual('timeout', stop_services(runtime, [service], timeout=2)[0]['status'])
        self.assertEqual('stopped', stop_services(runtime, [service], timeout=2)[0]['status'])
        self.assertEqual(1, runtime.events.count(('request_stop', 'worker')))
        self.assertEqual('already_stopped', stop_services(runtime, [service])[0]['status'])

    def test_all_identities_preflight_before_any_action(self):
        runtime, services = self.stack()
        for field, value in [('pid', 999), ('creation', 999), ('instance',
                             '00000000-0000-0000-0000-000000000000')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                stop_services(runtime, [*services[1:], replace(services[0], **{field: value})])
        self.assertEqual([], runtime.events)

    def test_reused_pid_with_same_creation_still_requires_instance_token(self):
        runtime = SimulatedRuntime()
        old = runtime.add('old', 'worker', 10, 20)
        stop_services(runtime, [old])
        new = runtime.add('replacement', 'worker', 10, 20)
        runtime.events.clear()
        with self.assertRaises(ValueError):
            stop_services(runtime, [old])
        self.assertEqual([], runtime.events)
        self.assertTrue(runtime._resolve(new).running)

    def test_missing_service_is_not_assumed_dead(self):
        runtime = SimulatedRuntime()
        identity = ServiceIdentity('absent', 'worker', 10, 20,
                                   '00000000-0000-0000-0000-000000000000')
        with self.assertRaises(ValueError):
            stop_services(runtime, [identity])
        self.assertEqual([], runtime.events)

    def test_omitted_running_service_blocks_entire_stop(self):
        runtime, services = self.stack()
        with self.assertRaises(ValueError):
            stop_services(runtime, services[:-1])
        self.assertEqual([], runtime.events)

    def test_duplicate_inventory_is_rejected(self):
        runtime, services = self.stack()
        with self.assertRaises(ValueError):
            stop_services(runtime, [*services, services[0]])
        self.assertEqual([], runtime.events)

    def test_non_simulated_backend_is_rejected_without_calls(self):
        backend = Mock()
        with self.assertRaises(ValueError):
            stop_services(backend, [])
        self.assertEqual([], backend.mock_calls)

    def test_invalid_identity_and_role_are_rejected(self):
        runtime, services = self.stack()
        for changes in ({'pid': True}, {'creation': None}, {'role': 'chat'},
                        {'instance': 'bad'}, {'name': ''}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                stop_services(runtime, [replace(services[0], **changes)])
        self.assertEqual([], runtime.events)

    def test_invalid_timeouts_have_no_effect(self):
        runtime, services = self.stack()
        for timeout in (True, 0, -1, 61, float('nan'), float('inf'), '1'):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                stop_services(runtime, services, timeout=timeout)
        self.assertEqual([], runtime.events)

    def test_separate_runtime_cannot_stop_another_inventory(self):
        runtime, services = self.stack()
        with self.assertRaises(ValueError):
            stop_services(SimulatedRuntime(), services)
        self.assertEqual([], runtime.events)

    def test_empty_simulation_is_a_noop(self):
        self.assertEqual([], stop_services(SimulatedRuntime(), []))


if __name__ == '__main__':
    unittest.main()
