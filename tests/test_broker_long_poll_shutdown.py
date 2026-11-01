"""Exercise broker shutdown with one admitted synthetic long poll."""

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECIPIENT = "123e4567-e89b-12d3-a456-426614174000"
HARNESS = r'''
import os, time
from pathlib import Path
import broker

original_wait = broker.Broker.wait_for_messages
def gated_wait(self, recipient, after, timeout=25, limit=100, *, credential=None):
    print('LONG_POLL_ENTERED', flush=True)
    release = Path(os.environ['BRIDGE_TEST_RELEASE'])
    deadline = time.monotonic() + 15
    while not release.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('test did not release the admitted long poll')
        time.sleep(.01)
    # Keep the real bounded database wait in the handler after the gate opens.
    return original_wait(self, recipient, after, timeout=min(timeout, .25),
                         limit=limit, credential=credential)

broker.Broker.wait_for_messages = gated_wait
broker.main()
'''


class BrokerLongPollShutdownTests(unittest.TestCase):
    def test_stop_rejects_new_work_then_drains_admitted_bounded_long_poll(self):
        with tempfile.TemporaryDirectory(prefix="broker-long-poll-stop-") as folder:
            root = Path(folder)
            state = root / "state.json"
            database = root / "synthetic-broker.sqlite3"
            release = root / "release-long-poll"
            env = dict(os.environ, BRIDGE_TEST_RELEASE=str(release))
            proc = subprocess.Popen(
                [
                    sys.executable, "-B", "-u", "-c", HARNESS,
                    "--control-stdin", "--database", str(database), "--state", str(state),
                    "--drain-timeout", "5",
                ],
                cwd=ROOT,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            output = queue.Queue()
            errors = queue.Queue()

            def collect(stream, destination):
                for line in stream:
                    destination.put(line.rstrip("\r\n"))
                destination.put(None)

            stdout_reader = threading.Thread(target=collect, args=(proc.stdout, output), daemon=True)
            stderr_reader = threading.Thread(target=collect, args=(proc.stderr, errors), daemon=True)
            stdout_reader.start()
            stderr_reader.start()

            waiter = None
            wait_result = queue.Queue()
            try:
                line = output.get(timeout=10)
                self.assertIsNotNone(line, "broker exited before announcing its local endpoint")
                self.assertIn("listening", line)
                config = json.loads(state.read_text(encoding="utf-8"))
                base = "http://127.0.0.1:%d" % config["port"]
                headers = {
                    "Authorization": "Bearer " + config["token"],
                    "Content-Type": "application/json",
                }
                register = urllib.request.Request(
                    base + "/v1/register",
                    headers=headers,
                    data=json.dumps({"id": RECIPIENT, "kind": "codex", "opt_in": True}).encode(),
                )
                with urllib.request.urlopen(register, timeout=3) as response:
                    self.assertEqual(200, response.status)

                def long_poll():
                    request = urllib.request.Request(
                        base + "/v1/messages/wait?recipient=" + RECIPIENT + "&after=0&timeout=1",
                        headers=headers,
                    )
                    try:
                        with urllib.request.urlopen(request, timeout=5) as response:
                            wait_result.put((response.status, json.load(response)))
                    except Exception as exc:
                        wait_result.put(exc)

                waiter = threading.Thread(target=long_poll, daemon=True)
                waiter.start()
                self.assertEqual("LONG_POLL_ENTERED", output.get(timeout=10))

                proc.stdin.write("stop\n")
                proc.stdin.flush()

                # A 503 is the synchronization point proving admission is closed.
                deadline = time.monotonic() + 5
                rejected = False
                while time.monotonic() < deadline:
                    request = urllib.request.Request(base + "/v1/sessions", headers=headers)
                    try:
                        with urllib.request.urlopen(request, timeout=1) as response:
                            self.assertEqual(200, response.status)
                    except urllib.error.HTTPError as exc:
                        self.assertEqual(503, exc.code)
                        rejected = True
                        break
                    except (ConnectionAbortedError, ConnectionResetError, urllib.error.URLError):
                        # A pre-gate connection may be interrupted while its
                        # headers are incomplete; continue until a later 503.
                        pass
                    time.sleep(.01)

                self.assertTrue(rejected, "broker did not reject new work after stop")
                self.assertIsNone(proc.poll(), "broker exited before the admitted long poll completed")
                self.assertFalse(release.exists(), "long poll gate must remain closed until rejection is observed")

                release.touch()
                result = wait_result.get(timeout=5)
                self.assertIsInstance(result, tuple, repr(result))
                self.assertEqual(200, result[0])
                self.assertEqual({"messages": [], "cursor": 0}, result[1])
                waiter.join(timeout=2)
                self.assertFalse(waiter.is_alive(), "admitted long-poll request did not finish")

                self.assertEqual(0, proc.wait(timeout=8), errors.get_nowait() if not errors.empty() else "")
                self.assertFalse(state.exists(), "clean drain should remove broker discovery state")
            finally:
                # This release/termination path is scoped exclusively to the test-owned child.
                release.touch()
                if waiter is not None:
                    waiter.join(timeout=2)
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
                for stream in (proc.stdin, proc.stdout, proc.stderr):
                    if stream is not None:
                        stream.close()
                stdout_reader.join(timeout=2)
                stderr_reader.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
