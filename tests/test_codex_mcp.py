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
    def test_status_tool_returns_explicit_delivery_evidence(self):
        client = Mock()
        with patch.object(codex_mcp, 'message_status', return_value={'broker_status': 'acknowledged'}) as status:
            result = codex_mcp.dispatch('bridge_delivery_status', {'recipient': THREAD, 'message_id': THREAD}, client)
        status.assert_called_once_with(client, THREAD, THREAD)
        self.assertEqual('acknowledged', result['broker_status'])

    def test_status_tool_documents_limits_of_missing_reply_link_evidence(self):
        response = codex_mcp.respond({'id': 1, 'method': 'tools/list'}, Mock())
        tools = {tool['name']: tool for tool in response['result']['tools']}
        description = tools['bridge_delivery_status']['description']
        self.assertIn('not recorded does not prove that no reply occurred', description)
        self.assertIn('older clients may omit reply_to', description)

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

    def test_direct_send_exposes_optional_reply_link(self):
        response = codex_mcp.respond({'id': 1, 'method': 'tools/list'}, Mock())
        tools = {tool['name']: tool for tool in response['result']['tools']}
        send_schema = tools['bridge_send']['inputSchema']
        self.assertEqual(['sender', 'recipient', 'text'], send_schema['required'])
        self.assertEqual('string', send_schema['properties']['reply_to']['type'])
        self.assertEqual('uuid', send_schema['properties']['message_id']['format'])
        self.assertIn('reuse it with identical sender, recipient, text, and reply_to values',
                      tools['bridge_send']['description'])

        client = Mock()
        expected = {'id': 'reply-id', 'status': 'queued'}
        client.send.return_value = expected
        args = {'sender': THREAD, 'recipient': GROUP_RECIPIENTS[0], 'text': 'answer',
                'reply_to': GROUP_RECIPIENTS[1]}
        self.assertEqual(expected, codex_mcp.dispatch('bridge_send', args, client))
        client.send.assert_called_once_with(
            THREAD, GROUP_RECIPIENTS[0], 'answer', reply_to=GROUP_RECIPIENTS[1])

    def test_direct_send_forwards_caller_message_id_for_retry_reuse(self):
        client = Mock()
        client.send.return_value = {'id': BATCH_ID, 'status': 'queued'}
        args = {
            'sender': THREAD,
            'recipient': GROUP_RECIPIENTS[0],
            'text': 'retry-safe message',
            'message_id': BATCH_ID,
        }

        result = codex_mcp.dispatch('bridge_send', args, client)

        client.send.assert_called_once_with(
            THREAD, GROUP_RECIPIENTS[0], 'retry-safe message', message_id=BATCH_ID)
        self.assertEqual({'id': BATCH_ID, 'status': 'queued'}, result)

    def test_direct_send_rejects_non_string_explicit_message_id(self):
        client = Mock()
        with self.assertRaisesRegex(ValueError, 'canonical UUID string'):
            codex_mcp.dispatch('bridge_send', {
                'sender': THREAD, 'recipient': GROUP_RECIPIENTS[0], 'text': 'hello',
                'message_id': None,
            }, client)
        client.send.assert_not_called()

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

    def test_room_tools_require_explicit_ids_and_batch_keys_without_auth_claims(self):
        response = codex_mcp.respond({'id': 1, 'method': 'tools/list'}, Mock())
        tools = {tool['name']: tool for tool in response['result']['tools']}

        expected = {
            'bridge_room_create': ['room_id', 'name', 'owner', 'members'],
            'bridge_room_info': ['room_id', 'member'],
            'bridge_room_update_member': ['room_id', 'actor', 'member', 'action'],
            'bridge_room_send': ['room_id', 'sender', 'text', 'batch_id'],
            'bridge_room_reply': ['room_id', 'sender', 'text', 'batch_id', 'reply_to'],
        }
        for name, required in expected.items():
            with self.subTest(tool=name):
                self.assertEqual(required, tools[name]['inputSchema']['required'])
                self.assertTrue(tools[name]['description'])
        self.assertEqual('uuid', tools['bridge_room_send']['inputSchema']['properties']['batch_id']['format'])
        self.assertEqual('uuid', tools['bridge_room_reply']['inputSchema']['properties']['batch_id']['format'])
        self.assertEqual(
            ['add', 'remove'],
            tools['bridge_room_update_member']['inputSchema']['properties']['action']['enum'],
        )
        self.assertIn('not caller authentication', tools['bridge_room_info']['description'])
        self.assertIn('does not authenticate', tools['bridge_room_update_member']['description'])

    def test_room_tools_dispatch_explicit_arguments_to_client(self):
        client = Mock()
        args_by_tool = [
            ('bridge_room_create', {
                'room_id': 'room:alpha', 'name': 'Alpha', 'owner': THREAD,
                'members': [THREAD, GROUP_RECIPIENTS[0]],
            }, 'create_room', ('room:alpha', 'Alpha', THREAD, [THREAD, GROUP_RECIPIENTS[0]])),
            ('bridge_room_info', {
                'room_id': 'room:alpha', 'member': THREAD,
            }, 'room_info', ('room:alpha', THREAD)),
            ('bridge_room_update_member', {
                'room_id': 'room:alpha', 'actor': THREAD, 'member': GROUP_RECIPIENTS[0], 'action': 'add',
            }, 'update_room_member', ('room:alpha', THREAD, GROUP_RECIPIENTS[0], 'add')),
            ('bridge_room_send', {
                'room_id': 'room:alpha', 'sender': THREAD, 'text': 'hello', 'batch_id': BATCH_ID,
            }, 'send_room_message', ('room:alpha', THREAD, 'hello', BATCH_ID)),
            ('bridge_room_reply', {
                'room_id': 'room:alpha', 'sender': THREAD, 'text': 'reply', 'batch_id': BATCH_ID,
                'reply_to': GROUP_RECIPIENTS[1],
            }, 'reply_room', ('room:alpha', THREAD, 'reply', BATCH_ID, GROUP_RECIPIENTS[1])),
        ]

        for tool_name, args, method_name, expected_args in args_by_tool:
            with self.subTest(tool=tool_name):
                method = getattr(client, method_name)
                method.return_value = {'ok': tool_name}
                result = codex_mcp.dispatch(tool_name, args, client)
                method.assert_called_once_with(*expected_args)
                self.assertEqual({'ok': tool_name}, result)
                method.reset_mock()

    def test_native_delivery_is_opt_in_and_targets_explicit_id(self):
        client = Mock()
        client.check_session.return_value = {'id': THREAD, 'kind': 'codex'}
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        client.check_session.assert_called_once_with(THREAD)
        client.register.assert_not_called()
        argv = popen.call_args.args[0]
        self.assertEqual(THREAD, argv[-1])
        self.assertEqual('worker_start_requested', result['status'])

    def test_native_delivery_does_not_opt_in_unregistered_target(self):
        client = Mock()
        client.check_session.return_value = {}
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        client.register.assert_not_called()
        popen.assert_not_called()
        self.assertEqual('session_not_opted_in', result['status'])

    def test_native_delivery_rejects_non_codex_session(self):
        client = Mock()
        client.check_session.return_value = {'id': THREAD, 'kind': 'claude'}
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        popen.assert_not_called()
        self.assertEqual('session_not_opted_in', result['status'])

    def test_native_delivery_rejects_wrong_profile_credential(self):
        client = Mock()
        client.check_session.side_effect = codex_mcp.ClientError('broker_error', 'unauthorized', 403)
        with patch.object(codex_mcp.subprocess, 'Popen') as popen:
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        popen.assert_not_called()
        self.assertEqual('session_not_opted_in', result['status'])

    def test_existing_worker_state_does_not_start_duplicate(self):
        client = Mock()
        client.check_session.return_value = {'id': THREAD, 'kind': 'codex'}
        with tempfile.TemporaryDirectory() as folder, patch.object(codex_mcp, 'ROOT', Path(folder)), \
             patch.object(codex_mcp.subprocess, 'Popen') as popen:
            (Path(folder) / ('native-worker-' + THREAD + '.json')).write_text('{}')
            result = codex_mcp.dispatch('bridge_enable_native', {'session_id': THREAD}, client)
        popen.assert_not_called()
        self.assertEqual('worker_state_invalid', result['status'])


if __name__ == '__main__':
    unittest.main()
