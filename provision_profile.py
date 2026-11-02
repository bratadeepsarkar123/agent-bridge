"""Operator CLI for initial profile provisioning, status checks, and orphan reports.

Subcommands (all output is JSON on stdout; errors are short sentences on stderr):

  provision PROFILE_ID --credentials-dir DIR [--database PATH] [--state PATH]
      Create DIR privately if it is new (an existing DIR must already be private),
      then provision one new profile through Broker.provision_profile. The active
      shared token is read from --state only when the recorded broker PID is alive.
      When --state is absent, or its PID is not alive, the output says the
      shared-token check was not loaded and gives a "reason" field; a stored token
      from a dead broker is not trusted. A shared token that already belongs to a
      bound profile is refused (exit 1) before any file is created.
  check PROFILE_ID [--database PATH]
      Read-only: report whether the profile is bound and opted in. No secret.
      The output also states the limit: a profile is not a thread-identity
      guarantee (same-user software can decrypt the credential).
  recover --credentials-dir DIR [--database PATH]
      Read-only: report credential files as bound, orphan, retired, or unreadable.
  rotate-start PROFILE_ID --credentials-dir DIR --client-stopped
      --restart-window TEXT --retention-days N [--database PATH]
      Record intent, create one new encrypted generation, then CAS the binding.
  rotate-status PROFILE_ID --credentials-dir DIR [--database PATH]
      Read-only rotation journal, binding hash, file inventory, and next step.
  rotate-resolve PROFILE_ID --credentials-dir DIR
      --outcome committed|abandoned|client-relaunched|verified
      [--client-restart-passed] [--client-check-passed] [--database PATH]
      Reconcile only after checking the database; never generates a token.

Exit codes: 0 success, 1 refused (nothing bound), 2 usage, 3 outcome unsure: do
not retry blindly.

The CLI never prints a token and never deletes a file.
"""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

import credential_store
from broker import ROOT, Broker, require_current_schema_or_empty
from broker_stop import process_is_alive

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_USAGE = 2
EXIT_UNSURE = 3

NO_STATE_REASON = ('no broker state file; no broker is running, so the shared token '
                   'was not checked')
NOT_RUNNING_REASON = ('broker is not running (the recorded broker process is not alive); '
                      'the stored shared token is not trusted and was not checked')


class CliRefusal(Exception):
    """A refusal with a short operator-facing reason; no secret or traceback."""


def _open_broker(database, *, read_only=False):
    database = Path(database)
    if not database.is_file():
        raise CliRefusal('database file not found; start the broker once or check the path')
    try:
        require_current_schema_or_empty(database, require_existing=True)
        return Broker(database, initialize=False, read_only=read_only)
    except (ValueError, OSError, sqlite3.Error):
        raise CliRefusal('database schema is not current or cannot be read') from None


def _active_shared_token(state):
    """Return (token, reason) for the running broker.

    token is the shared token when the recorded broker PID is alive; otherwise token
    is None and reason says why it was not checked. The PID is only checked, never
    signalled. A PID that cannot be checked at all counts as alive, so the token is
    still loaded into the collision check (the safer direction).
    """
    state = Path(state)
    try:
        text = state.read_text(encoding='utf-8')
    except FileNotFoundError:
        return None, NO_STATE_REASON
    except OSError:
        raise CliRefusal('broker state file cannot be read; provisioning refused') from None
    try:
        data = json.loads(text)
        token = data['token']
        pid = data['pid']
    except (ValueError, KeyError, TypeError):
        raise CliRefusal('broker state file is invalid; provisioning refused') from None
    if not isinstance(token, str) or len(token) < 32:
        raise CliRefusal('broker state file is invalid; provisioning refused')
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise CliRefusal('broker state file is invalid; provisioning refused')
    try:
        alive = process_is_alive(pid)
    except OSError:
        alive = True
    if not alive:
        return None, NOT_RUNNING_REASON
    return token, None


def _emit(value):
    sys.stdout.write(json.dumps(value, sort_keys=True) + '\n')


def cmd_provision(args):
    if not Broker.valid_id(args.profile_id):
        raise CliRefusal('invalid profile id')
    shared, reason = _active_shared_token(args.state)
    broker = _open_broker(args.database)
    if shared is not None:
        # Check the shared token before any directory or file is created.
        try:
            broker.load_shared_token(shared)
        except ValueError:
            raise CliRefusal('the running broker\'s shared token already belongs to a bound '
                             'profile; provisioning refused (exit code 1); nothing was written'
                             ) from None
    try:
        credential_store.prepare_credentials_directory(args.credentials_dir)
    except credential_store.CredentialStoreError as exc:
        raise CliRefusal(str(exc) + '; nothing was written') from None
    try:
        result = broker.provision_profile(args.profile_id, str(args.credentials_dir),
                                          active_shared_token=shared)
    except credential_store.ProvisionError as exc:
        if exc.unsure:
            sys.stderr.write(str(exc) + '\n')
            if exc.credential_file:
                sys.stderr.write('Credential file (not deleted): ' + exc.credential_file + '\n')
            return EXIT_UNSURE
        message = str(exc)
        if exc.credential_file:
            message += '. Encrypted file kept as an orphan (not deleted): ' + exc.credential_file
        raise CliRefusal(message) from None
    except credential_store.CredentialStoreError as exc:
        extra = ''
        if getattr(exc, 'credential_file', None):
            extra = '. Encrypted file kept as an orphan (not deleted): ' + exc.credential_file
        raise CliRefusal(str(exc) + extra) from None
    result['shared_token_checked'] = shared is not None
    if reason is not None:
        result['reason'] = reason
    _emit(result)
    return EXIT_OK


PROFILE_LIMIT_NOTE = (
    'A bound profile authenticates a configured identity only. It is NOT proof that a real '
    'chat or thread sent a request: a process running as the same Windows user can decrypt '
    'the DPAPI credential, and the broker does not prove which thread a request came from. '
    'Do not treat a profile ID as a thread-identity guarantee.'
)


def cmd_check(args):
    broker = _open_broker(args.database, read_only=True)
    try:
        status = broker.profile_status(args.profile_id)
    except ValueError:
        raise CliRefusal('invalid profile id') from None
    status['note'] = PROFILE_LIMIT_NOTE
    _emit(status)
    return EXIT_OK


def cmd_recover(args):
    broker = _open_broker(args.database, read_only=True)
    try:
        report = credential_store.inventory_credential_files(broker, args.credentials_dir)
    except credential_store.CredentialStoreError as exc:
        raise CliRefusal(str(exc)) from None
    _emit({'directory': str(args.credentials_dir), 'files': report,
           'changed': False,
           'note': 'Nothing was bound, overwritten, or deleted. Orphan and unreadable '
                   'files need an owner decision.'})
    return EXIT_OK


def cmd_rotate_start(args):
    if not Broker.valid_id(args.profile_id):
        raise CliRefusal('invalid profile id')
    if not args.client_stopped:
        raise CliRefusal('confirm that this profile client is stopped before rotating')
    shared, reason = _active_shared_token(args.state)
    broker = _open_broker(args.database)
    if shared is not None:
        try:
            broker.load_shared_token(shared)
        except ValueError:
            raise CliRefusal('the running broker shared token conflicts with a profile; rotation refused') from None
    try:
        result = credential_store.rotate_profile_credential(
            broker, args.profile_id, args.credentials_dir,
            restart_window=args.restart_window, retention_days=args.retention_days)
    except credential_store.ProvisionError as exc:
        if exc.unsure:
            sys.stderr.write(str(exc) + '\n')
            if exc.credential_file:
                sys.stderr.write('Encrypted file kept (not deleted): ' + exc.credential_file + '\n')
            return EXIT_UNSURE
        message = str(exc)
        if exc.credential_file:
            message += '. Encrypted file kept (not deleted): ' + exc.credential_file
        raise CliRefusal(message) from None
    except (credential_store.CredentialStoreError, ValueError) as exc:
        raise CliRefusal(str(exc)) from None
    result['shared_token_checked'] = shared is not None
    if reason is not None:
        result['reason'] = reason
    _emit(result)
    return EXIT_OK


def _rotation_next_step(rotation, bound_hash):
    if rotation is None:
        return 'rotate-start' if bound_hash else 'provision'
    state = rotation['state']
    if state == 'prepared':
        return 'resolve-abandoned'
    if state == 'file_written':
        return 'resolve-abandoned' if bound_hash == rotation['old_sha256'] else 'operator-review'
    if state == 'committed':
        return 'verify-client' if rotation['client_relaunched_at'] else 'relaunch-client'
    if state == 'verified' and rotation['retired_at'] is None:
        return 'retire-old-file'
    return 'none'


def cmd_rotate_status(args):
    if not Broker.valid_id(args.profile_id):
        raise CliRefusal('invalid profile id')
    broker = _open_broker(args.database, read_only=True)
    try:
        files = credential_store.inventory_credential_files(broker, args.credentials_dir)
    except credential_store.CredentialStoreError as exc:
        raise CliRefusal(str(exc)) from None
    rotation = broker.rotation_for_profile(args.profile_id)
    bound_hash = broker.binding_for_profile(args.profile_id)
    _emit({'id': args.profile_id, 'rotation': rotation, 'bound_sha256': bound_hash,
           'files': files, 'next_step': _rotation_next_step(rotation, bound_hash),
           'changed': False})
    return EXIT_OK


def cmd_rotate_resolve(args):
    if not Broker.valid_id(args.profile_id):
        raise CliRefusal('invalid profile id')
    broker = _open_broker(args.database)
    rotation = broker.rotation_for_profile(args.profile_id)
    if rotation is None:
        raise CliRefusal('no rotation journal exists for this profile')
    bound_hash = broker.binding_for_profile(args.profile_id)

    if args.outcome == 'committed':
        retired = broker.retired_credential(rotation['old_sha256'])
        if (rotation['state'] != 'committed' or bound_hash != rotation['new_sha256'] or
                not retired):
            raise CliRefusal('database does not confirm the claimed committed outcome')
        _emit({'id': args.profile_id, 'state': 'committed', 'changed': False,
               'next_step': _rotation_next_step(rotation, bound_hash)})
        return EXIT_OK

    if args.outcome == 'client-relaunched':
        if rotation['state'] != 'committed' or bound_hash != rotation['new_sha256']:
            raise CliRefusal('database does not confirm a committed rotation')
        if not args.client_restart_passed:
            raise CliRefusal('confirm that the client restart completed')
        if rotation['client_relaunched_at'] is None:
            broker.mark_rotation_client_relaunched(rotation['id'])
            sys.stderr.write('Credential rotation client relaunch recorded for {}.\n'.format(
                args.profile_id))
        _emit({'id': args.profile_id, 'state': 'committed', 'client_relaunched': True,
               'changed': rotation['client_relaunched_at'] is None, 'next_step': 'verify-client'})
        return EXIT_OK

    if args.outcome == 'abandoned':
        if rotation['state'] == 'abandoned':
            _emit({'id': args.profile_id, 'state': 'abandoned', 'changed': False})
            return EXIT_OK
        if rotation['state'] not in ('prepared', 'file_written'):
            raise CliRefusal('a committed or verified rotation cannot be abandoned')
        if rotation['new_sha256'] and bound_hash == rotation['new_sha256']:
            raise CliRefusal('new credential is bound; refusing to mark this rotation abandoned')
        if bound_hash is None:
            raise credential_store.ProvisionError(
                'Rotation outcome is unsure. Do not retry blindly.', unsure=True)
        broker.abandon_credential_rotation(rotation['id'])
        sys.stderr.write('Rotation marked abandoned; existing encrypted files were kept.\n')
        _emit({'id': args.profile_id, 'state': 'abandoned', 'changed': True,
               'new_file_kept': rotation['new_file']})
        return EXIT_OK

    if not args.client_check_passed:
        raise CliRefusal('verified outcome requires --client-check-passed')
    if (rotation['state'] not in ('committed', 'verified') or
            rotation['client_relaunched_at'] is None or
            bound_hash != rotation['new_sha256'] or
            not broker.retired_credential(rotation['old_sha256'])):
        raise CliRefusal('database does not confirm the claimed verified outcome')
    try:
        result = credential_store.verify_and_retire_old_generation(
            broker, args.profile_id, args.credentials_dir, client_check_passed=True)
    except credential_store.ProvisionError as exc:
        if exc.unsure:
            sys.stderr.write(str(exc) + '\n')
            return EXIT_UNSURE
        raise CliRefusal(str(exc)) from None
    except credential_store.CredentialStoreError as exc:
        raise CliRefusal(str(exc)) from None
    result['changed'] = rotation['retired_at'] is None
    if result['changed']:
        sys.stderr.write('Credential rotation verified and old file retired for {}.\n'.format(
            args.profile_id))
    _emit(result)
    return EXIT_OK


def _parser():
    parser = argparse.ArgumentParser(prog='provision_profile.py')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('provision', 'check', 'recover', 'rotate-start', 'rotate-status', 'rotate-resolve'):
        child = sub.add_parser(name)
        child.add_argument('--database', type=Path, default=ROOT / 'broker.sqlite3')
        if name in ('provision', 'recover', 'rotate-start', 'rotate-status', 'rotate-resolve'):
            child.add_argument('--credentials-dir', type=Path, required=True)
        if name in ('provision', 'check', 'rotate-start', 'rotate-status', 'rotate-resolve'):
            child.add_argument('profile_id')
        if name == 'provision':
            child.add_argument('--state', type=Path, default=ROOT / 'broker-session.json')
        if name == 'rotate-start':
            child.add_argument('--state', type=Path, default=ROOT / 'broker-session.json')
            child.add_argument('--client-stopped', action='store_true', required=True)
            child.add_argument('--restart-window', required=True)
            child.add_argument('--retention-days', type=int, required=True)
        if name == 'rotate-resolve':
            child.add_argument('--outcome', choices=('committed', 'abandoned', 'client-relaunched',
                                                      'verified'), required=True)
            child.add_argument('--client-check-passed', action='store_true')
            child.add_argument('--client-restart-passed', action='store_true')
    return parser


def main(argv=None):
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return EXIT_USAGE if exc.code else EXIT_OK
    handlers = {'provision': cmd_provision, 'check': cmd_check, 'recover': cmd_recover,
                'rotate-start': cmd_rotate_start, 'rotate-status': cmd_rotate_status,
                'rotate-resolve': cmd_rotate_resolve}
    try:
        return handlers[args.command](args)
    except CliRefusal as exc:
        sys.stderr.write('Refused: ' + str(exc) + '\n')
        return EXIT_REFUSED
    except credential_store.ProvisionError as exc:
        sys.stderr.write(str(exc) + '\n')
        return EXIT_UNSURE if exc.unsure else EXIT_REFUSED
    except credential_store.CredentialStoreError as exc:
        sys.stderr.write('Refused: ' + str(exc) + '\n')
        return EXIT_REFUSED
    except (OSError, sqlite3.Error, ValueError):
        sys.stderr.write('Refused: rotation state could not be read or written safely.\n')
        return EXIT_REFUSED


if __name__ == '__main__':
    sys.exit(main())
