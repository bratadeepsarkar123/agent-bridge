"""Isolated tests for a shared, explicitly opted-in app-server subscription."""

from __future__ import annotations

import queue
import threading
import time
import unittest
from unittest.mock import Mock

from codex_native import ProxyError, RPCRejected
from shared_subscription import SharedSubscription


THREAD_A = '123e4567-e89b-12d3-a456-426614174000'
THREAD_B = '223e4567-e89b-12d3-a456-426614174000'
THREAD_OTHER = '323e4567-e89b-12d3-a456-426614174000'
_CLOSED = object()


class FakeProxy:
    def __init__(self, request_errors=None, pre_response=None):
        self.request_errors = request_errors or {}
        self.pre_response = pre_response or {}
        self.requests = []
        self.incoming = queue.Queue()
        self.closed = threading.Event()
        self.close_count = 0
        self._close_lock = threading.Lock()

    def send_json(self, request):
        method = request['method']
        params = request['params']
        ident = request['id']
        self.requests.append((method, params, ident))
        error = self.request_errors.get(params.get('threadId'))
        if error is not None:
            if isinstance(error, RPCRejected):
                self.incoming.put({'id': ident, 'error': {'message': str(error)}})
            else:
                raise error
        if method == 'thread/resume':
            for event in self.pre_response.get(params['threadId'], []):
                self.incoming.put(event)
            if error is None:
                self.incoming.put({'id': ident, 'result': {'thread': {'id': params['threadId']}}})
            return
        raise AssertionError('unexpected app-server request: ' + method)

    def receive_json(self):
        event = self.incoming.get()
        if event is _CLOSED:
            raise ProxyError('fake proxy closed')
        if isinstance(event, BaseException):
            raise event
        return event

    def emit(self, event):
        self.incoming.put(event)

    def disconnect(self, error=None):
        self.incoming.put(error or ProxyError('fake transport disconnected'))

    def close(self):
        with self._close_lock:
            self.close_count += 1
            if not self.closed.is_set():
                self.closed.set()
                self.incoming.put(_CLOSED)


def wait_for(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class SharedSubscriptionTests(unittest.TestCase):
    def test_resumes_only_selected_ids_and_isolates_missing_rollout(self):
        early_event = {'method': 'thread/event', 'params': {'threadId': THREAD_A, 'early': True}}
        proxy = FakeProxy(
            {THREAD_B: RPCRejected('no rollout for selected thread')},
            {THREAD_A: [early_event]},
        )
        factory = Mock(return_value=proxy)
        received = []
        delivered = threading.Event()

        def callback(event):
            received.append(event)
            delivered.set()

        subscription = SharedSubscription(
            [THREAD_A, THREAD_B], proxy_factory=factory, reconnect_delay=0.01
        )
        try:
            subscription.connect(callback=callback)
            self.assertTrue(wait_for(lambda: subscription.member_statuses == {
                THREAD_A: 'subscribed', THREAD_B: 'unavailable'
            }), 'member-specific resume state did not settle')

            self.assertEqual(1, factory.call_count)
            self.assertEqual(
                [THREAD_A, THREAD_B],
                [params['threadId'] for method, params, _ in proxy.requests if method == 'thread/resume'],
            )
            self.assertTrue(all(params['excludeTurns'] is True for _, params, _ in proxy.requests))
            self.assertIn('RPCRejected', subscription.member_errors[THREAD_B])

            proxy.emit({'method': 'thread/event', 'params': {'threadId': THREAD_OTHER}})
            proxy.emit({'method': 'approval/request', 'id': 71,
                        'params': {'threadId': THREAD_A}})
            proxy.emit({'method': 'thread/event', 'params': {'threadId': THREAD_B}})
            proxy.emit({'method': 'thread/event', 'params': {'threadId': THREAD_A}})

            self.assertTrue(delivered.wait(1), 'selected notification was not delivered')
            self.assertTrue(wait_for(lambda: len(received) == 2), 'all selected notifications were not delivered')
            self.assertEqual([THREAD_A, THREAD_A],
                             [event['params']['threadId'] for event in received])
            self.assertTrue(received[0]['params']['early'])
            self.assertEqual(['thread/resume', 'thread/resume'],
                             [method for method, _, _ in proxy.requests])
            self.assertEqual('connected', subscription.status)
            self.assertIsNone(proxy.timeout)
        finally:
            subscription.close()

        self.assertTrue(proxy.closed.is_set())
        self.assertEqual('closed', subscription.status)

    def test_all_missing_rollouts_release_the_proxy(self):
        proxy = FakeProxy({THREAD_A: RPCRejected('no rollout')})
        factory = Mock(return_value=proxy)
        subscription = SharedSubscription([THREAD_A], proxy_factory=factory, reconnect_delay=0.01)
        try:
            subscription.connect()
            self.assertTrue(wait_for(lambda: subscription.status == 'unavailable'))
            self.assertTrue(wait_for(lambda: proxy.closed.is_set()))
            self.assertEqual(1, factory.call_count)
            self.assertEqual('unavailable', subscription.member_statuses[THREAD_A])
        finally:
            subscription.close()

    def test_partial_resume_transport_failure_retries_every_member_on_one_new_proxy(self):
        first = FakeProxy({THREAD_B: ProxyError('transport failed during resume')})
        second = FakeProxy()
        proxies = [first, second]
        created = []

        def factory():
            proxy = proxies[min(len(created), len(proxies) - 1)]
            created.append(proxy)
            return proxy

        delivered = threading.Event()
        received = []
        subscription = SharedSubscription(
            [THREAD_A, THREAD_B], proxy_factory=factory,
            reconnect_delay=0.01, max_reconnect_delay=0.02,
        )
        try:
            subscription.connect(callback=lambda event: (received.append(event), delivered.set()))
            self.assertTrue(wait_for(lambda: len(created) >= 2), 'connection was not retried')
            self.assertTrue(wait_for(lambda: subscription.member_statuses == {
                THREAD_A: 'subscribed', THREAD_B: 'subscribed'
            }), 'all opted-in members were not resumed after reconnect')

            self.assertEqual(1, first.close_count)
            for proxy in (first, second):
                self.assertEqual(
                    [THREAD_A, THREAD_B],
                    [params['threadId'] for method, params, _ in proxy.requests if method == 'thread/resume'],
                )
                self.assertTrue(all(method == 'thread/resume' for method, _, _ in proxy.requests))

            second.emit({'method': 'thread/turn/completed',
                         'params': {'threadId': THREAD_B, 'turn': {'id': 'turn'}}})
            self.assertTrue(delivered.wait(1), 'notification was not delivered after reconnect')
            self.assertEqual(THREAD_B, received[0]['params']['threadId'])
        finally:
            subscription.close()

    def test_receive_disconnect_reconnects_and_resumes_exact_same_members(self):
        first, second = FakeProxy(), FakeProxy()
        created = []

        def factory():
            proxy = first if not created else second
            created.append(proxy)
            return proxy

        subscription = SharedSubscription(
            [THREAD_A, THREAD_B], proxy_factory=factory, reconnect_delay=0.01
        )
        try:
            subscription.connect()
            self.assertTrue(wait_for(lambda: subscription.member_statuses == {
                THREAD_A: 'subscribed', THREAD_B: 'subscribed'
            }), 'initial members were not resumed')
            first.disconnect()
            self.assertTrue(wait_for(lambda: len(created) == 2), 'disconnected proxy was not replaced')
            self.assertTrue(wait_for(lambda: subscription.member_statuses == {
                THREAD_A: 'subscribed', THREAD_B: 'subscribed'
            }), 'reconnected proxy did not resume both members')

            self.assertEqual(1, first.close_count)
            self.assertEqual(2, len(created))
            self.assertEqual([THREAD_A, THREAD_B],
                             [params['threadId'] for _, params, _ in second.requests])
        finally:
            subscription.close()

    def test_close_during_proxy_creation_closes_late_proxy(self):
        entered = threading.Event()
        release = threading.Event()
        proxy = FakeProxy()

        def factory():
            entered.set()
            release.wait(2)
            return proxy

        subscription = SharedSubscription([THREAD_A], proxy_factory=factory)
        subscription.connect()
        self.assertTrue(entered.wait(1), 'proxy creation did not start')
        closer = threading.Thread(target=subscription.close)
        closer.start()
        release.set()
        closer.join(1)

        self.assertFalse(closer.is_alive(), 'close did not finish after proxy creation ended')
        self.assertTrue(proxy.closed.is_set())
        self.assertEqual(1, proxy.close_count)
        self.assertEqual('closed', subscription.status)
        self.assertEqual({THREAD_A: 'closed'}, subscription.member_statuses)

    def test_requires_unique_nonempty_opt_in_uuid_list_and_bounded_backoff(self):
        with self.assertRaises(ValueError):
            SharedSubscription([], proxy_factory=Mock())
        with self.assertRaises(ValueError):
            SharedSubscription([THREAD_A, THREAD_A], proxy_factory=Mock())
        with self.assertRaises(ValueError):
            SharedSubscription([THREAD_A], proxy_factory=Mock(),
                               reconnect_delay=0.2, max_reconnect_delay=0.1)
        with self.assertRaises(ValueError):
            SharedSubscription([THREAD_A], proxy_factory=Mock(), max_reconnect_delay=float('inf'))
        with self.assertRaises(ValueError):
            SharedSubscription([THREAD_A], proxy_factory=Mock(), reconnect_delay=float('nan'))


if __name__ == '__main__':
    unittest.main()
