"""Deliver addressed agent messages to one already-loaded Codex thread.

Runs until stopped. Does not create/resume threads, impersonate the user, or
automatically approve anything. Uncertain deliveries are never blindly retried.
"""
import argparse
import json
import time
from pathlib import Path

from codex_client import BrokerClient, ClientError
from codex_native import AppServerProxy, ProxyError, RPCRejected, TargetUnavailable, deliver_message
from codex_queue import DeliveryLedger, QueueError, canonical_uuid
from lockfile import acquire, release

ROOT = Path(__file__).resolve().parent


def drain_once(recipient, broker, ledger, proxy_factory=AppServerProxy, idle_only=False):
    recipient = canonical_uuid(recipient, 'recipient')
    messages = broker.poll(recipient).get('messages', [])
    if not isinstance(messages, list):
        raise ValueError('invalid broker response')
    results = []
    for message in messages:
        if not isinstance(message, dict) or message.get('status') != 'queued':
            continue
        ident = canonical_uuid(message.get('id'), 'message id')
        if ledger.get(ident) is not None:
            continue
        proxy = None
        claimed = False
        try:
            # Check connectivity and human-wait state before claiming. If the
            # thread is unavailable, leave the message for a future poll.
            proxy = proxy_factory()
            thread = proxy.request('thread/read', {'threadId': recipient, 'includeTurns': False}, 5).get('thread', {})
            status = thread.get('status', {})
            if (thread.get('id') != recipient or status.get('type') not in ('active', 'idle') or
                    (idle_only and status.get('type') != 'idle') or
                    set(status.get('activeFlags', [])) & {'waitingOnApproval', 'waitingOnUserInput'}):
                results.append({'id': ident, 'status': 'waiting_for_target'})
                continue
            claimed, _ = ledger.begin(ident, recipient)
            if not claimed:
                continue
            result = deliver_message(proxy, recipient, message, idle_only=idle_only)
            ledger.finish(ident, 'accepted', 0)
            results.append({'id': ident, 'status': 'submitted', 'turn_id': result.get('turn', {}).get('id')})
        except (RPCRejected, TargetUnavailable) as exc:
            if claimed:
                ledger.release_rejected(ident)
            results.append({'id': ident, 'status': 'waiting_for_target', 'error_type': type(exc).__name__})
        except (OSError, ProxyError, QueueError, ValueError) as exc:
            if claimed:
                ledger.finish(ident, 'uncertain', None)
                results.append({'id': ident, 'status': 'uncertain', 'error_type': type(exc).__name__})
            else:
                results.append({'id': ident, 'status': 'waiting_for_target', 'error_type': type(exc).__name__})
        finally:
            if proxy:
                proxy.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recipient', required=True)
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--idle-only', action='store_true', help='hold messages while the target is active')
    args = parser.parse_args()
    recipient = canonical_uuid(args.recipient, 'recipient')
    if args.interval < 5:
        parser.error('interval must be at least 5 seconds')
    lock = ROOT / ('native-worker-' + recipient + '.json')
    try:
        acquire(lock, {'recipient': recipient})
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    try:
        broker = BrokerClient()
        ledger = DeliveryLedger(ROOT / 'codex-native-ledger.sqlite3')
        previous = None
        while True:
            try:
                results = drain_once(recipient, broker, ledger, idle_only=args.idle_only)
                report = json.dumps(results, sort_keys=True)
                if results and report != previous:
                    print(report, flush=True)
                previous = report
            except (ClientError, OSError, ValueError, QueueError) as exc:
                report = type(exc).__name__
                if report != previous:
                    print(json.dumps({'status': 'worker_error', 'error_type': report}), flush=True)
                previous = report
            time.sleep(args.interval)
    finally:
        release(lock)


if __name__ == '__main__':
    main()
