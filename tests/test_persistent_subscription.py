"""Contract tests for an opt-in, reconnecting app-server subscription.

The app-server proxy is mocked throughout; these tests never attach to a live
Codex thread. The intended public contract is documented in the test calls:
``connect(resume=True, callback=...)`` explicitly requests ``thread/resume``.
"""

from __future__ import annotations

import queue
import threading
import unittest
from unittest.mock import Mock

from codex_native import ProxyError, RPCRejected
from persistent_subscription import PersistentSubscription


THREAD_ID = "123e4567-e89b-12d3-a456-426614174000"
_CLOSED = object()


class MockAppServerProxy:
    """Small in-memory stand-in for AppServerProxy's request/read/close API."""

    def __init__(self, *, request_error=None):
        self.request_error = request_error
        self.request = Mock(side_effect=request_error)
        self.request.return_value = {'thread': {'id': THREAD_ID}}
        self._incoming = queue.Queue()
        self.closed = threading.Event()
        self.close = Mock(side_effect=self._close)

    def receive_json(self):
        value = self._incoming.get()
        if value is _CLOSED:
            raise ProxyError("mock app-server connection closed")
        if isinstance(value, BaseException):
            raise value
        return value

    def emit(self, notification):
        self._incoming.put(notification)

    def disconnect(self, error=None):
        self._incoming.put(error or ProxyError("mock transport disconnected"))

    def _close(self):
        if not self.closed.is_set():
            self.closed.set()
            self._incoming.put(_CLOSED)


class PersistentSubscriptionTests(unittest.TestCase):
    def test_concurrent_connect_is_rejected_while_first_connection_is_opening(self):
        entered_factory = threading.Event()
        release_factory = threading.Event()
        proxy = MockAppServerProxy()

        def proxy_factory():
            entered_factory.set()
            release_factory.wait(1)
            return proxy

        subscription = PersistentSubscription(proxy_factory, THREAD_ID)
        connect_errors = []

        def connect_first():
            try:
                subscription.connect()
            except BaseException as exc:
                connect_errors.append(exc)

        first = threading.Thread(target=connect_first)
        first.start()
        self.assertTrue(entered_factory.wait(1), "first connect did not enter the factory")
        try:
            with self.assertRaisesRegex(RuntimeError, "started twice"):
                subscription.connect()
        finally:
            release_factory.set()
        first.join(1)
        self.assertFalse(first.is_alive(), "first connect did not finish")
        self.assertEqual([], connect_errors)
        self.assertEqual("connected_unsubscribed", subscription.status)
        subscription.close()
        proxy.close.assert_called_once_with()

    def test_close_during_connection_setup_closes_late_proxy_without_starting_reader(self):
        entered_factory = threading.Event()
        release_factory = threading.Event()
        proxy = MockAppServerProxy()

        def proxy_factory():
            entered_factory.set()
            release_factory.wait(1)
            return proxy

        subscription = PersistentSubscription(proxy_factory, THREAD_ID)
        connect_errors = []

        def connect():
            try:
                subscription.connect()
            except BaseException as exc:
                connect_errors.append(exc)

        connector = threading.Thread(target=connect)
        connector.start()
        self.assertTrue(entered_factory.wait(1), "connect did not enter the factory")
        subscription.close()
        release_factory.set()
        connector.join(1)

        self.assertFalse(connector.is_alive(), "connect did not observe close")
        self.assertEqual(1, len(connect_errors))
        self.assertIsInstance(connect_errors[0], RuntimeError)
        self.assertIn("closed while connecting", str(connect_errors[0]))
        proxy.close.assert_called_once_with()
        self.assertIsNone(subscription._thread)
        self.assertEqual("closed", subscription.status)

    def test_connect_holds_one_connection_and_does_not_resume_by_default(self):
        proxy = MockAppServerProxy()
        proxy_factory = Mock(return_value=proxy)
        received = []
        delivered = threading.Event()

        def callback(notification):
            received.append(notification)
            if len(received) == 2:
                delivered.set()

        subscription = PersistentSubscription(proxy_factory, THREAD_ID)
        try:
            subscription.connect(callback=callback)
            first_event = {
                "method": "thread/turn/completed",
                "params": {"threadId": THREAD_ID, "turn": {"id": "turn-1"}},
            }
            second_event = {
                "method": "thread/turn/completed",
                "params": {"threadId": THREAD_ID, "turn": {"id": "turn-2"}},
            }
            proxy.emit({"method": "thread/status/changed", "params": {"threadId": "other"}})
            proxy.emit(first_event)
            proxy.emit(second_event)

            self.assertTrue(delivered.wait(1), "notifications were not delivered on the held connection")
            self.assertEqual([first_event, second_event], received)
            proxy_factory.assert_called_once_with()
            proxy.request.assert_not_called()
            proxy.close.assert_not_called()
        finally:
            subscription.close()

        proxy.close.assert_called_once_with()

    def test_resume_subscription_requires_explicit_opt_in_and_uses_requested_thread(self):
        proxy = MockAppServerProxy()
        proxy_factory = Mock(return_value=proxy)
        callback = Mock()
        subscription = PersistentSubscription(proxy_factory, THREAD_ID)

        try:
            subscription.connect(resume=True, callback=callback)

            proxy.request.assert_called_once()
            method, params = proxy.request.call_args.args[:2]
            self.assertEqual("thread/resume", method)
            self.assertEqual(THREAD_ID, params["threadId"])
            self.assertNotIn("thread/start", [call.args[0] for call in proxy.request.call_args_list])
            proxy.close.assert_not_called()
        finally:
            subscription.close()

    def test_transport_disconnect_reconnects_and_resumes_the_same_explicit_thread(self):
        first = MockAppServerProxy()
        second = MockAppServerProxy()
        proxy_factory = Mock(side_effect=[first, second])
        received = threading.Event()
        callback = Mock(side_effect=lambda _notification: received.set())
        subscription = PersistentSubscription(proxy_factory, THREAD_ID)

        try:
            subscription.connect(resume=True, callback=callback)
            first.disconnect()
            event = {
                "method": "thread/turn/completed",
                "params": {"threadId": THREAD_ID, "turn": {"id": "turn-2"}},
            }
            second.emit(event)

            self.assertTrue(received.wait(2), "subscription did not reconnect and dispatch")
            self.assertEqual(2, proxy_factory.call_count)
            self.assertEqual("thread/resume", first.request.call_args.args[0])
            self.assertEqual("thread/resume", second.request.call_args.args[0])
            self.assertEqual(
                [THREAD_ID, THREAD_ID],
                [
                    first.request.call_args.args[1]["threadId"],
                    second.request.call_args.args[1]["threadId"],
                ],
            )
            callback.assert_called_once_with(event)
        finally:
            subscription.close()

    def test_missing_rollout_refuses_resume_without_start_fallback(self):
        proxy = MockAppServerProxy(
            request_error=RPCRejected("thread/resume failed: no rollout found")
        )
        proxy_factory = Mock(return_value=proxy)
        subscription = PersistentSubscription(proxy_factory, THREAD_ID)

        with self.assertRaisesRegex(RPCRejected, "no rollout found"):
            subscription.connect(resume=True)

        proxy.request.assert_called_once()
        self.assertEqual("thread/resume", proxy.request.call_args.args[0])
        self.assertEqual(THREAD_ID, proxy.request.call_args.args[1]["threadId"])
        proxy.close.assert_called_once_with()
        self.assertNotIn("thread/start", [call.args[0] for call in proxy.request.call_args_list])


if __name__ == "__main__":
    unittest.main()
