"""Exercise incomplete HTTP requests during shutdown of an owned broker.

Each test starts only its own loopback broker with a temporary SQLite database.
The harness adds observation prints around DrainingHTTPServer methods; it does
not change broker behavior. No configured broker state or live service is used.
"""

import json
import os
import queue
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HARNESS = r'''
import broker
import os
import time
from pathlib import Path

server_type = broker.DrainingHTTPServer
original_thread = server_type.process_request_thread
original_admit = server_type.admit
original_begin_drain = server_type.begin_drain
original_wait_for_drain = server_type.wait_for_drain
original_sessions = broker.Broker.sessions

def observed_sessions(self):
    release_path = os.environ.get('BRIDGE_TEST_RELEASE_OPERATION')
    if release_path:
        print('OPERATION_ENTERED', flush=True)
        deadline = time.monotonic() + 30
        while not Path(release_path).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError('test did not release admitted operation')
            time.sleep(.01)
        print('OPERATION_RELEASED', flush=True)
    return original_sessions(self)

def observed_process_request_thread(self, request, client_address):
    print('HANDLER_STARTED', flush=True)
    return original_thread(self, request, client_address)

def observed_admit(self, request):
    result = original_admit(self, request)
    if result:
        print('ADMITTED', flush=True)
    return result

def observed_begin_drain(self):
    original_begin_drain(self)
    print('DRAIN_STARTED', flush=True)

def observed_wait_for_drain(self, timeout):
    print('DRAIN_WAIT active={}'.format(self.active_handlers), flush=True)
    result = original_wait_for_drain(self, timeout)
    print('DRAIN_RESULT {} active={}'.format(result, self.active_handlers), flush=True)
    return result

server_type.process_request_thread = observed_process_request_thread
server_type.admit = observed_admit
server_type.begin_drain = observed_begin_drain
server_type.wait_for_drain = observed_wait_for_drain
broker.Broker.sessions = observed_sessions
broker.main()
'''


class BrokerSlowShutdownTests(unittest.TestCase):
    def start_broker(self, root, *, request_timeout, drain_timeout, release_operation=None):
        state_path = root / "broker-state.json"
        database_path = root / "synthetic-broker.sqlite3"
        env = os.environ.copy()
        env.pop("BRIDGE_TEST_RELEASE_OPERATION", None)
        if release_operation is not None:
            env["BRIDGE_TEST_RELEASE_OPERATION"] = str(release_operation)
        process = subprocess.Popen(
            [
                sys.executable, "-B", "-u", "-c", HARNESS,
                "--control-stdin",
                "--database", str(database_path),
                "--state", str(state_path),
                "--request-timeout", str(request_timeout),
                "--drain-timeout", str(drain_timeout),
            ],
            cwd=ROOT,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        stdout_lines = queue.Queue()
        stderr_lines = queue.Queue()

        def collect(stream, target):
            try:
                for line in stream:
                    target.put(line.rstrip())
            finally:
                target.put(None)

        threading.Thread(target=collect, args=(process.stdout, stdout_lines), daemon=True).start()
        threading.Thread(target=collect, args=(process.stderr, stderr_lines), daemon=True).start()
        try:
            self.wait_for_line(stdout_lines, "listening", timeout=10)
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except BaseException:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
            raise
        return process, state_path, database_path, state, stdout_lines, stderr_lines

    def wait_for_line(self, lines, fragment, *, timeout):
        observed = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                break
            if line is None:
                break
            observed.append(line)
            if fragment in line:
                return line
        self.fail("timed out waiting for {!r}; observed {}".format(fragment, observed))

    @staticmethod
    def send_pipe_stop(process):
        process.stdin.write("stop\n")
        process.stdin.flush()

    @staticmethod
    def connect_and_send_partial(port, request):
        client = socket.create_connection(("127.0.0.1", port), timeout=5)
        client.sendall(request)
        return client

    def wait_for_peer_close(self, client, *, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            client.settimeout(min(0.1, max(0.01, deadline - time.monotonic())))
            try:
                data = client.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if not data:
                return
        self.fail("broker did not close the incomplete request before the deadline")

    @staticmethod
    def cleanup_process(process, client=None):
        if client is not None:
            try:
                client.close()
            except OSError:
                pass
        if process.poll() is None:
            process.terminate()  # This is exclusively the test-owned subprocess.
            process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            try:
                stream.close()
            except OSError:
                pass

    def test_pipe_stop_drains_incomplete_headers_before_deadline(self):
        with tempfile.TemporaryDirectory(prefix="bridge-slow-header-stop-") as folder:
            root = Path(folder)
            (process, state_path, _database_path, state,
             stdout_lines, _stderr_lines) = self.start_broker(
                root, request_timeout=1, drain_timeout=4,
            )
            client = None
            try:
                client = self.connect_and_send_partial(
                    state["port"],
                    b"POST /v1/register HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Slow:",
                )
                self.wait_for_line(stdout_lines, "HANDLER_STARTED", timeout=5)
                self.send_pipe_stop(process)

                self.assertIn("DRAIN_WAIT active=1", self.wait_for_line(
                    stdout_lines, "DRAIN_WAIT", timeout=5))
                self.assertIn("DRAIN_RESULT True active=0", self.wait_for_line(
                    stdout_lines, "DRAIN_RESULT", timeout=5))
                self.assertEqual(0, process.wait(timeout=5))
                self.assertFalse(state_path.exists(), "clean drain should remove broker state")
                self.wait_for_peer_close(client, timeout=2)
            finally:
                self.cleanup_process(process, client)

    def test_incomplete_headers_expire_on_request_inactivity_deadline(self):
        with tempfile.TemporaryDirectory(prefix="bridge-slow-header-timeout-") as folder:
            root = Path(folder)
            (process, state_path, _database_path, state,
             stdout_lines, _stderr_lines) = self.start_broker(
                root, request_timeout=1, drain_timeout=3,
            )
            client = None
            try:
                client = self.connect_and_send_partial(
                    state["port"],
                    b"GET /v1/sessions HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Slow:",
                )
                self.wait_for_line(stdout_lines, "HANDLER_STARTED", timeout=5)
                started = time.monotonic()
                self.wait_for_peer_close(client, timeout=5)
                elapsed = time.monotonic() - started
                self.assertGreaterEqual(elapsed, 0.5, "connection closed before inactivity timeout")
                self.assertLess(elapsed, 4, "connection exceeded inactivity timeout by too much")

                self.send_pipe_stop(process)
                self.assertIn("DRAIN_RESULT True active=0", self.wait_for_line(
                    stdout_lines, "DRAIN_RESULT", timeout=5))
                self.assertEqual(0, process.wait(timeout=5))
                self.assertFalse(state_path.exists(), "clean drain should remove broker state")
            finally:
                self.cleanup_process(process, client)

    def test_incomplete_post_body_hits_drain_deadline_without_false_clean(self):
        with tempfile.TemporaryDirectory(prefix="bridge-slow-body-drain-") as folder:
            root = Path(folder)
            (process, state_path, database_path, state,
             stdout_lines, stderr_lines) = self.start_broker(
                root, request_timeout=8, drain_timeout=2,
            )
            client = None
            try:
                body = json.dumps({
                    "id": "synthetic-partial-body-session",
                    "kind": "codex",
                    "opt_in": True,
                }).encode("utf-8")
                self.assertLess(len(body), 100, "fixture must be valid JSON shorter than Content-Length")
                request = (
                    b"POST /v1/register HTTP/1.1\r\n"
                    b"Host: 127.0.0.1\r\n"
                    + ("Authorization: Bearer {}\r\n".format(state["token"])).encode("ascii")
                    + b"Content-Type: application/json\r\n"
                    + b"Content-Length: 100\r\n"
                    + b"Connection: close\r\n\r\n"
                    + body
                )
                client = self.connect_and_send_partial(state["port"], request)
                self.wait_for_line(stdout_lines, "HANDLER_STARTED", timeout=5)
                self.assertIn("ADMITTED", self.wait_for_line(stdout_lines, "ADMITTED", timeout=5))

                self.send_pipe_stop(process)
                wait_line = self.wait_for_line(stdout_lines, "DRAIN_WAIT", timeout=5)
                self.assertEqual("DRAIN_WAIT active=1", wait_line,
                                 "incomplete POST body must remain tracked as active work")
                self.assertIsNone(process.poll(), "broker reported completion with body handler active")
                self.assertTrue(state_path.exists(), "state must remain while admitted work is active")
                time.sleep(0.25)
                self.assertIsNone(process.poll(), "broker exited clean before the drain deadline")
                self.assertTrue(state_path.exists(), "state disappeared before the drain result")

                self.assertIn("DRAIN_RESULT False", self.wait_for_line(
                    stdout_lines, "DRAIN_RESULT", timeout=5))
                self.assertEqual(2, process.wait(timeout=8),
                                 "drain deadline with active body work must not be clean")
                self.assertTrue(state_path.exists(), "failed drain must retain broker state")
                self.assertIn("Broker drain failed", self.wait_for_line(
                    stderr_lines, "Broker drain failed", timeout=3))
                with closing(sqlite3.connect(database_path)) as db:
                    registration = db.execute(
                        "SELECT id FROM sessions WHERE id=?",
                        ("synthetic-partial-body-session",),
                    ).fetchone()
                    self.assertIsNone(
                        registration,
                        "incomplete Content-Length framing must not mutate registration",
                    )
            finally:
                self.cleanup_process(process, client)

    def test_post_drain_trickling_header_has_hard_deadline_and_no_mutation(self):
        with tempfile.TemporaryDirectory(prefix="bridge-post-drain-header-") as folder:
            root = Path(folder)
            release = root / "release-admitted-operation"
            (process, state_path, database_path, state,
             stdout_lines, _stderr_lines) = self.start_broker(
                root,
                request_timeout=2,
                drain_timeout=12,
                release_operation=release,
            )
            admitted_client = None
            slow_client = None
            stop_trickling = threading.Event()
            trickle_errors = []
            trickle_count = [0]
            trickle_lock = threading.Lock()
            trickler = None
            try:
                admitted_client = self.connect_and_send_partial(
                    state["port"],
                    (
                        "GET /v1/sessions HTTP/1.1\r\n"
                        "Host: 127.0.0.1\r\n"
                        "Authorization: Bearer {}\r\n"
                        "Connection: close\r\n\r\n"
                    ).format(state["token"]).encode("ascii"),
                )
                self.wait_for_line(stdout_lines, "OPERATION_ENTERED", timeout=5)

                self.send_pipe_stop(process)
                self.wait_for_line(stdout_lines, "DRAIN_STARTED", timeout=5)
                self.assertEqual("DRAIN_WAIT active=1", self.wait_for_line(
                    stdout_lines, "DRAIN_WAIT", timeout=5))

                slow_client = self.connect_and_send_partial(
                    state["port"],
                    b"GET /v1/sessions HTTP/1.1\r\n"
                    b"Host: 127.0.0.1\r\n"
                    b"X-Trickle: ",
                )
                slow_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.wait_for_line(stdout_lines, "HANDLER_STARTED", timeout=5)

                def trickle():
                    while not stop_trickling.wait(0.1):
                        try:
                            slow_client.sendall(b"x")
                            with trickle_lock:
                                trickle_count[0] += 1
                        except OSError as exc:
                            trickle_errors.append(exc)
                            return

                trickler = threading.Thread(target=trickle, daemon=True)
                trickler.start()
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    with trickle_lock:
                        if trickle_count[0] >= 5:
                            break
                    if trickle_errors:
                        self.fail("slow header closed before admitted work was released")
                    time.sleep(0.01)
                with trickle_lock:
                    self.assertGreaterEqual(trickle_count[0], 5,
                                            "slow client did not keep trickling without an idle gap")
                self.assertIsNone(process.poll(), "broker exited while admitted operation was held")
                self.assertTrue(state_path.exists(), "state must remain during admitted work")

                # This releases a read-only admitted handler. The new header is
                # still incomplete and continues receiving bytes during shutdown.
                release.touch()
                self.wait_for_line(stdout_lines, "OPERATION_RELEASED", timeout=3)
                try:
                    return_code = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.fail(
                        "broker did not close the post-drain trickling header within 5 seconds "
                        "(less than the 12-second total drain timeout)"
                    )
                self.assertEqual(0, return_code, "drain should finish cleanly after the hard header deadline")
                self.assertFalse(state_path.exists(), "clean drain should remove broker state")
            finally:
                release.touch()
                stop_trickling.set()
                if trickler is not None:
                    trickler.join(timeout=2)
                self.cleanup_process(process, admitted_client)
                if slow_client is not None:
                    try:
                        slow_client.close()
                    except OSError:
                        pass
                with closing(sqlite3.connect(database_path)) as db:
                    self.assertEqual(0, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0],
                                     "neither read-only nor incomplete-header work may mutate sessions")


if __name__ == "__main__":
    unittest.main()
