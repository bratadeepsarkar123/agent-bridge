"""Optional app-server MCP startup regression; never creates a chat.

Run with BRIDGE_RUN_APP_SERVER_E2E=1. Starts temporary stdio app-server
processes and a temporary copy of the MCP server; no model inference is used.
"""
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from codex_queue import DEFAULT_CODEX

ROOT = Path(__file__).resolve().parents[1]
FILES = ('codex_mcp.py', 'codex_client.py', 'codex_queue.py', 'status.py', 'message_status.py', 'credential_store.py')


def appserver_status(server):
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'cp437'
    options = [
        'mcp_servers.local_agent_messaging.command=' + json.dumps(sys.executable),
        'mcp_servers.local_agent_messaging.args=' + json.dumps(['-B', str(server)]),
        'mcp_servers.local_agent_messaging.startup_timeout_sec=3',
    ]
    command = [DEFAULT_CODEX, 'app-server']
    for option in options:
        command.extend(['-c', option])
    command.extend(['--listen', 'stdio://'])
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, env=env,
                               creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    lines = queue.Queue()

    def read():
        for line in process.stdout:
            lines.put(line)
        lines.put(None)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()

    def send(message):
        process.stdin.write((json.dumps(message) + '\n').encode())
        process.stdin.flush()

    def receive(request_id):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                break
            if line is None:
                raise RuntimeError('app-server exited before responding')
            response = json.loads(line)
            if response.get('id') == request_id:
                if 'error' in response:
                    raise RuntimeError('app-server rejected diagnostic request')
                return response.get('result', {})
        raise TimeoutError('app-server diagnostic timed out')

    try:
        send({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
            'clientInfo': {'name': 'bridge_encoding_test', 'title': 'Bridge test', 'version': '1'}}})
        receive(1)
        send({'jsonrpc': '2.0', 'method': 'initialized', 'params': {}})
        send({'jsonrpc': '2.0', 'id': 2, 'method': 'mcpServerStatus/list', 'params': {
            'serverName': 'local_agent_messaging', 'detail': 'toolsAndAuthOnly'}})
        data = receive(2).get('data', [])
        if len(data) != 1:
            raise RuntimeError('bridge server was not listed')
        return data[0]
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdin.close()
        process.stdout.close()
        reader.join(timeout=2)


@unittest.skipUnless(os.environ.get('BRIDGE_RUN_APP_SERVER_E2E') == '1',
                     'set BRIDGE_RUN_APP_SERVER_E2E=1 to run isolated app-server test')
class McpAppServerEncodingTests(unittest.TestCase):
    def test_cp437_failure_and_fix_through_real_appserver(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder)
            for name in FILES:
                shutil.copyfile(ROOT / name, target / name)
            server = target / 'codex_mcp.py'
            fixed = server.read_text(encoding='utf-8')
            needle = 'json.dumps(message, ensure_ascii=True)'
            self.assertIn(needle, fixed)
            server.write_text(fixed.replace(needle, 'json.dumps(message, ensure_ascii=False)'),
                              encoding='utf-8')
            before = appserver_status(server)
            self.assertNotIn('bridge_send', before.get('tools', {}))
            self.assertIn('timed out', before.get('toolsError', '').lower())
            server.write_text(fixed, encoding='utf-8')
            after = appserver_status(server)
            self.assertIn('bridge_send', after.get('tools', {}))
            self.assertIsNone(after.get('toolsError'))


if __name__ == '__main__':
    unittest.main()
