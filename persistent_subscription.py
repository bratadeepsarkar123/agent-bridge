"""Opt-in holder for a Codex app-server thread subscription.

The owner must explicitly permit resumption. This module never creates a thread,
starts a turn, acknowledges messages, or handles approval requests.
"""
import threading

from codex_native import ProxyError, RPCRejected, TargetUnavailable
from codex_queue import canonical_uuid


class PersistentSubscription:
    def __init__(self, proxy_factory, thread_id, reconnect_delay=0.25):
        self.proxy_factory = proxy_factory
        self.thread_id = canonical_uuid(thread_id, 'thread_id')
        if reconnect_delay <= 0:
            raise ValueError('reconnect_delay must be positive')
        self.reconnect_delay = reconnect_delay
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._proxy = None
        self._thread = None
        self._starting = False
        self._resume = False
        self._callback = None
        self.status = 'disconnected'
        self.last_error = None

    def _open(self):
        proxy = self.proxy_factory()
        try:
            if self._resume:
                # This is the explicit opt-in path; it may load a saved thread.
                result = proxy.request('thread/resume', {'threadId': self.thread_id, 'excludeTurns': True}, 7)
                thread = result.get('thread') if isinstance(result, dict) else None
                if not isinstance(thread, dict) or thread.get('id') != self.thread_id:
                    raise TargetUnavailable('resumed thread identity could not be verified')
            # Idle is normal for a notification subscriber; wait without polling.
            proxy.timeout = None
            return proxy
        except BaseException:
            proxy.close()
            raise

    def connect(self, *, resume=False, callback=None):
        with self._lock:
            if self._thread is not None or self._starting or self._stop.is_set():
                raise RuntimeError('subscription cannot be started twice')
            self._starting = True
            self._resume = bool(resume)
            self._callback = callback
        try:
            proxy = self._open()
        except BaseException as exc:
            with self._lock:
                self._starting = False
                if not self._stop.is_set():
                    self.status = 'unavailable'
                    self.last_error = exc
            raise
        with self._lock:
            self._starting = False
            if self._stop.is_set():
                cancelled = True
            else:
                cancelled = False
                self._proxy = proxy
                self.status = 'subscribed' if self._resume else 'connected_unsubscribed'
                self._thread = threading.Thread(target=self._run, name='codex-subscription', daemon=True)
                # Start while holding the state lock so close() cannot try to
                # join a Thread object in the gap before Thread.start().
                self._thread.start()
        if cancelled:
            proxy.close()
            raise RuntimeError('subscription was closed while connecting')
        return self

    def _set_status(self, status, error=None):
        with self._lock:
            if self._stop.is_set():
                return
            self.status = status
            if error is not None:
                self.last_error = error

    def _take_proxy(self):
        with self._lock:
            proxy = self._proxy
            self._proxy = None
            return proxy

    def _run(self):
        while not self._stop.is_set():
            with self._lock:
                proxy = self._proxy
            if proxy is None:
                if self._stop.wait(self.reconnect_delay):
                    break
                try:
                    proxy = self._open()
                except (RPCRejected, TargetUnavailable) as exc:
                    # A missing rollout cannot be recovered by creating a new chat.
                    self._set_status('unavailable', exc)
                    break
                except (ProxyError, OSError) as exc:
                    self._set_status('disconnected', exc)
                    continue
                with self._lock:
                    if self._stop.is_set():
                        close_proxy = True
                    else:
                        close_proxy = False
                        self._proxy = proxy
                        self.status = 'subscribed' if self._resume else 'connected_unsubscribed'
                if close_proxy:
                    proxy.close()
                    break
            try:
                event = proxy.receive_json()
                # This holder never answers server requests or approvals.
                params = event.get('params') if isinstance(event, dict) else None
                if (self._callback is not None and isinstance(event, dict) and 'method' in event
                        and 'id' not in event and isinstance(params, dict)
                        and params.get('threadId') == self.thread_id):
                    try:
                        self._callback(event)
                    except Exception:
                        pass
            except (ProxyError, OSError) as exc:
                self._set_status('disconnected', exc)
                old = self._take_proxy()
                if old is not None:
                    old.close()

    def close(self):
        with self._lock:
            self._stop.set()
            proxy = self._proxy
            self._proxy = None
            thread = self._thread
            self.status = 'closed'
        if proxy is not None:
            proxy.close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
