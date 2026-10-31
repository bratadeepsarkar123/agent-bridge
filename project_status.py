"""Report bridge service and named test-session status without message bodies."""
import json
from pathlib import Path

from service_guard import process_identity
from status import inspect_status, _process_is_alive

ROOT = Path(__file__).resolve().parent
CODEX = '01a18ed9-5766-7ef1-9328-67ed7465809a'
CLAUDE = '24f2b207-1068-4fff-88b3-97227d2a962f'


def get_status():
    result = {'broker': {'reachable': False}, 'services': [],
              'claude_test': {'process_status': 'unknown', 'agent_health': 'unknown'},
              'limits': ['Native Codex delivery requires a loaded thread.',
                         'A stopped Claude session does not receive channel events.',
                         'Service guard does not start automatically after reboot.']}
    try:
        queue = inspect_status([CODEX, CLAUDE])
        result['broker'] = queue['broker']
        result['queues'] = queue['queues']
    except Exception:
        result['broker'] = {'reachable': False}
    try:
        config = json.loads((ROOT / 'service-guard-config.json').read_text(encoding='utf-8'))
        for service in config['services']:
            actual = process_identity(service['pid'])
            state = 'running' if actual is not None and actual == service.get('creation') else 'unknown_or_stopped'
            result['services'].append({'name': service['script'], 'status': state})
    except (OSError, ValueError, KeyError, TypeError):
        result['services_error'] = 'service configuration unavailable'
    try:
        guard = json.loads((ROOT / 'service-guard.json').read_text(encoding='utf-8'))
        result['service_guard'] = 'process_present' if _process_is_alive(guard['pid']) else 'not_running'
    except (OSError, ValueError, KeyError, TypeError):
        result['service_guard'] = 'unknown'
    # This is the explicitly monitored process, not proof of session health or
    # proof that the session has not been resumed under another PID.
    result['claude_test']['process_status'] = 'present' if _process_is_alive(32684) else 'not_running'
    result['claude_test']['scope'] = 'original monitored PID only; resumed sessions are not detected'
    return result


if __name__ == '__main__':
    print(json.dumps(get_status(), ensure_ascii=False, sort_keys=True))
