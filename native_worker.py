"""Deliver addressed agent messages to one already-loaded Codex thread.

Runs until stopped. Does not create/resume threads, impersonate the user, or
automatically approve anything. Uncertain deliveries are never blindly retried.

One process serves one recipient and reads its credential from the environment
(BRIDGE_SESSION_CREDENTIAL_FILE). Several protected profiles must not share that
one environment credential: this worker refuses more than one --recipient. Use
multi_dispatcher.py with --credential SESSION_UUID=PATH for each profile.
"""
import argparse
import json
import time
from pathlib import Path

import child_stop
from codex_client import BrokerClient, ClientError, require_single_environment_profile
from codex_native import AppServerProxy, ProxyError, RPCRejected, TargetUnavailable, deliver_message
from codex_queue import DeliveryLedger, QueueError, canonical_uuid
from lockfile import acquire, release
from persistent_subscription import PersistentSubscription

ROOT = Path(__file__).resolve().parent


def start_holder(recipient, enabled, holder_factory=PersistentSubscription, proxy_factory=AppServerProxy):
    """Start a holder only after the operator explicitly opts this thread in."""
    if not enabled:
        return None
    holder = holder_factory(proxy_factory, recipient)
    try:
        holder.connect(resume=True)
    except (ProxyError, OSError) as exc:
        print(json.dumps({'status': 'subscription_unavailable', 'error_type': type(exc).__name__}), flush=True)
    return holder


def drain_once(recipient, broker, ledger, proxy_factory=AppServerProxy, idle_only=False):
    recipient = canonical_uuid(recipient, 'recipient')
    messages = broker.poll(recipient, status='queued').get('messages', [])
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recipient', action='append', required=True,
                        help='one Codex thread ID; repeating it is refused (use multi_dispatcher.py)')
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--idle-only', action='store_true', help='hold messages while the target is active')
    parser.add_argument('--keep-loaded', action='store_true',
                        help='explicitly permit resuming this thread and keeping a subscription open')
    args = parser.parse_args(argv)
    try:
        recipients = [canonical_uuid(value, 'recipient') for value in args.recipient]
        require_single_environment_profile(recipients)
    except (ClientError, ValueError) as exc:
        parser.error(getattr(exc, 'message', str(exc)))
    recipient = recipients[0]
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
        holder = start_holder(recipient, args.keep_loaded)
        previous = None
        holder_previous = None
        cursor = 0
        stop_request = child_stop.request_path(__file__)
        while True:
            # Checked before every pass: a stopped worker starts no new delivery.
            # Returning runs the finally block, which closes the holder and releases the lock.
            if stop_request.exists():
                print(json.dumps({'status': 'stop_requested'}), flush=True)
                child_stop.consume(stop_request)
                return
            results = []
            if holder is not None and holder.status != holder_previous:
                holder_previous = holder.status
                print(json.dumps({'status': 'subscription_' + holder.status,
                                  'error_type': type(holder.last_error).__name__ if holder.last_error else None}), flush=True)
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
            try:
                # A target awaiting a human decision needs periodic rechecks;
                # otherwise wait for the broker to signal a new message.
                pending = any(item.get('status') == 'waiting_for_target' for item in results)
                wait = broker.wait_for_messages(recipient, cursor, min(args.interval, 25) if pending else 25)
                new_cursor = wait.get('cursor')
                if isinstance(new_cursor, bool) or not isinstance(new_cursor, int) or new_cursor < cursor:
                    raise ValueError('invalid broker cursor')
                cursor = new_cursor
            except (ClientError, OSError, ValueError):
                # Older brokers do not expose the wait endpoint yet.
                child_stop.pause(args.interval, stop_request)
    finally:
        if 'holder' in locals() and holder is not None:
            holder.close()
        release(lock)


if __name__ == '__main__':
    main()
