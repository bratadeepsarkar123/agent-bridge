"""Check MCP startup in a real subprocess without launching a chat."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class McpStdioStartupTests(unittest.TestCase):
    def test_malformed_requests_return_errors_without_ending_server(self):
        server = Path(__file__).resolve().parents[1] / 'codex_mcp.py'
        payload = (b'\xff\n' + b'{invalid\n' + b'[]\n'
                   + b'{"jsonrpc":"2.0","id":4}\n'
                   + b'{"jsonrpc":"2.0","id":5,"method":"initialize","params":null}\n'
                   + b'{"jsonrpc":"2.0","id":6,"method":"tools/call","params":[]}\n'
                   + b'{"jsonrpc":"2.0","id":8,"method":"missing/method","params":null}\n'
                   + b'{"jsonrpc":"2.0","id":true,"method":"ping"}\n'
                   + b'{"jsonrpc":"2.0","id":7,"method":"ping"}\n')
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, '-B', str(server)], cwd=folder,
                                    input=payload, capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.decode('ascii').splitlines()]
        self.assertEqual([-32700, -32700, -32600, -32600, -32602, -32602, -32601, -32600],
                         [item['error']['code'] for item in responses[:-1]])
        self.assertEqual([None, None, None, None, 5, 6, 8, None],
                         [item['id'] for item in responses[:-1]])
        self.assertEqual({'jsonrpc': '2.0', 'id': 7, 'result': {}}, responses[-1])

    def test_jsonrpc_batch_preserves_response_order_and_omits_notifications(self):
        server = Path(__file__).resolve().parents[1] / 'codex_mcp.py'
        batch = [
            {'jsonrpc': '2.0', 'id': 10, 'method': 'ping'},
            {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
            {'jsonrpc': '2.0', 'id': 11, 'method': 'tools/list'},
        ]
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, '-B', str(server)], cwd=folder,
                                    input=(json.dumps(batch) + '\n').encode('utf-8'),
                                    capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.decode('ascii').splitlines()]
        self.assertEqual(1, len(responses))
        self.assertEqual([10, 11], [item['id'] for item in responses[0]])
        self.assertEqual({}, responses[0][0]['result'])
        self.assertIn('bridge_send', [item['name'] for item in responses[0][1]['result']['tools']])

    def test_notification_only_batch_emits_no_response_and_server_continues(self):
        server = Path(__file__).resolve().parents[1] / 'codex_mcp.py'
        payload = (b'[{"jsonrpc":"2.0","method":"notifications/initialized"}]\n'
                   b'{"jsonrpc":"2.0","id":12,"method":"ping"}\n')
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, '-B', str(server)], cwd=folder,
                                    input=payload, capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.decode('ascii').splitlines()]
        self.assertEqual([{'jsonrpc': '2.0', 'id': 12, 'result': {}}], responses)

    def test_windows_oem_codepage_does_not_break_tools_list_or_unicode_input(self):
        server = Path(__file__).resolve().parents[1] / 'codex_mcp.py'
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'cp437'
        requests = [
            {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': 'unicode-Ω'}},
            {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
        ]
        payload = ''.join(json.dumps(item, ensure_ascii=False) + '\n' for item in requests).encode('utf-8')
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, '-B', str(server)], cwd=folder, env=env,
                                    input=payload, capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.decode('ascii').splitlines()]
        self.assertEqual([1, 2], [item['id'] for item in responses])
        self.assertEqual('unicode-Ω', responses[0]['result']['protocolVersion'])
        self.assertIn('bridge_send', [item['name'] for item in responses[1]['result']['tools']])

    def test_initialize_and_tools_list_from_unrelated_working_directory(self):
        server = Path(__file__).resolve().parents[1] / 'codex_mcp.py'
        requests = [
            {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2025-03-26'}},
            {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
        ]
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, '-B', str(server)], cwd=folder,
                                    input=''.join(json.dumps(item) + '\n' for item in requests),
                                    text=True, capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([1, 2], [item['id'] for item in responses])
        tools = responses[1]['result']['tools']
        self.assertIn('bridge_send', [item['name'] for item in tools])


if __name__ == '__main__':
    unittest.main()
