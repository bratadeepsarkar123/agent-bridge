import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import codex_mcp

THREAD = '123e4567-e89b-12d3-a456-426614174000'
GROUP_RECIPIENTS = [
    '123e4567-e89b-12d3-a456-426614174001',
    '123e4567-e89b-12d3-a456-426614174002',
]
BATCH_ID = '123e4567-e89b-12d3-a456-426614174003'


class CodexMcpTests(unittest.TestCase):
    def test_group_send_tool_requires_explicit_members_and_batch_uuid(self):
        response = codex_mcp.respond({'id': 1, 'method': 'tools/list'}, Mock())
        tools = {tool['name']: tool for tool in response['result']['tools']}
        group = tools['bridge_send_group']
        schema = group['inputSchema']

        self.assertEqual(
            ['sender', 'recipients', 'text', 'batch_id'],
            schema['required'],
        )
        self.assertEqual('array', schema['properties']['recipients']['type'])
        self.assertTrue(schema['properties']['recipients']['uniqueItems'])
        self.assertEqual('uuid', schema['properties']['batch_id']['format'])
        self.assertIn('explicit session IDs', group['description'])
        self.assertIn('Reuse the same canonical batch UUID', group['description'])

    def test_group_send_dispatches_exact_recipients_and_idempotency_key(self):
        client = Mock()
        client.send_group.return_value = {'batch_id': BATCH_ID, 'deliveries': []}
        args = {
            'sender': THREAD,
            'recipients': GROUP_RECIPIENTS,
            'text': 'hello group',
            'batch_id': BATCH_ID,
        }

        result = codex_mcp.dispatch('bridge_send_group', args, client)

        client.send_group.assert_called_once_with(THREAD, GROUP_RECIPIENTS, 'hello group', BATCH_ID)
        self.assertEqual({'batch_id': BATCH_ID, 'deliveries': []}, result)

    def test_group_send_tool_call_returns_client_result(self):
        client = Mock()
        expected = {'batch_id': BATCH_ID, 'deliveries': [{'recipient': GROUP_RECIPIENTS[0]}]}
        client.send_group.return_value = expected
        response = codex_mcp.respond({
            'id': 2,
            'method': 'tools/call',
            'params': {'name': 'bridge_send_group', 'arguments': {
                'sender': THREAD,
                'recipients': GROUP_RECIPIENTS,
                'text': 'hello group',
                'batch_id': BATCH_ID,
            }},
        }, client)

        client.send_group.assert_called_once_with(THREAD, GROUP_RECIPIENTS, 'hello group', BATCH_ID)
        self.assertEqual(expected, json.loads(response['result']['content'][0]['text']))

    def test_native_delivery_is_opt_in_and_targets_explicit_id(self):
        client = Mock()
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        client.register.assert_called_once_with(THREAD, 'codex')
        argv = popen.call_args.args[0]
        self.assertEqual(THREAD, argv[-1])
        self.assertEqual('worker_start_requested', result['status'])

    def test_existing_worker_state_does_not_start_duplicate(self):
        client = Mock()
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            (Path(folder) / ('native-worker-' + THREAD + '.json')).write_text('{}')
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        popen.assert_not_called()
        self.assertEqual('worker_state_invalid', result['status'])


if __name__ == '__main__':
    unittest.main()
