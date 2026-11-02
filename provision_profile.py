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
      Read-only: report each credential-*.dpapi file as bound, orphan, or
      unreadable. Nothing is bound, overwritten, renamed, or deleted.

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


def _open_broker(database):
    database = Path(database)
    if not database.is_file():
        raise CliRefusal('database file not found; start the broker once or check the path')
    try:
        require_current_schema_or_empty(database, require_existing=True)
        return Broker(database)
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
    broker = _open_broker(args.database)
    try:
        status = broker.profile_status(args.profile_id)
    except ValueError:
        raise CliRefusal('invalid profile id') from None
    status['note'] = PROFILE_LIMIT_NOTE
    _emit(status)
    return EXIT_OK


def cmd_recover(args):
    broker = _open_broker(args.database)
    try:
        report = credential_store.inventory_credential_files(broker, args.credentials_dir)
    except credential_store.CredentialStoreError as exc:
        raise CliRefusal(str(exc)) from None
    _emit({'directory': str(args.credentials_dir), 'files': report,
           'changed': False,
           'note': 'Nothing was bound, overwritten, or deleted. Orphan and unreadable '
                   'files need an owner decision.'})
    return EXIT_OK


def _parser():
    parser = argparse.ArgumentParser(prog='provision_profile.py')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('provision', 'check', 'recover'):
        child = sub.add_parser(name)
        child.add_argument('--database', type=Path, default=ROOT / 'broker.sqlite3')
        if name in ('provision', 'recover'):
            child.add_argument('--credentials-dir', type=Path, required=True)
        if name in ('provision', 'check'):
            child.add_argument('profile_id')
        if name == 'provision':
            child.add_argument('--state', type=Path, default=ROOT / 'broker-session.json')
    return parser


def main(argv=None):
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return EXIT_USAGE if exc.code else EXIT_OK
    handlers = {'provision': cmd_provision, 'check': cmd_check, 'recover': cmd_recover}
    try:
        return handlers[args.command](args)
    except CliRefusal as exc:
        sys.stderr.write('Refused: ' + str(exc) + '\n')
        return EXIT_REFUSED
    except credential_store.CredentialStoreError as exc:
        sys.stderr.write('Refused: ' + str(exc) + '\n')
        return EXIT_REFUSED


if __name__ == '__main__':
    sys.exit(main())
