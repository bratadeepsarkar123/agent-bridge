"""ISOLATED PROTOTYPE: supervisor-owned intent for test-owned pipe children.

No live broker/supervisor imports, discovery files, configured PIDs, network
endpoints, service hooks, or automatic recovery. Only the adjacent synthetic
test service can be launched. Existing synthetic_stop.creation_time is reused
to verify retained Windows process handles; a journaled PID never grants control.

Protocol: explicit initialization -> durable launch claim -> verified running
child -> durable stop/restart intent -> operation-bound completion ACK AND
verified zero exit. EOF/idle-lease zero exit without ACK remains pending.
Stop stays stopped. Restart requires an explicit call, retained ownership of the
exited child, and another durable launch claim BEFORE launching a replacement.
An incomplete claim, missing policy, corrupt journal, nonzero exit or lost handle
requires operator review. Reconstructing the coordinator does not adopt processes
or replay commands. In particular it cannot automatically finish a restart.

Journal entries are append-only, hash-linked and exclusively atomically published
by hard link after file fsync, under one persistent OS file lock. Failures leave
incomplete artifacts rather than pretending the previous state authorizes action.
All writers must cooperate and use the same trusted private directory/lock inode.
Restrictive Windows ACLs are the caller's responsibility; Python modes are not
ACL policy. Same-user malicious code, directory replacement, direct stop/start
bypass, deleting/rolling back the entire journal, or restoring old valid files
defeat these guarantees. Hash chaining does not establish rollback freshness.
Windows file fsync does not prove directory-entry durability across power loss;
filesystem, storage-controller and disk guarantees matter on every OS. No claim
of atomicity with child execution, broker markers, or databases is made.

REAL INTEGRATION REMAINS UNIMPLEMENTED: every eventual supervisor, --recover and
launcher path would need explicit lifecycle policy and a check under the SAME
recovery gate. Missing legacy policy must deny recovery, not silently opt in.
This prototype does not fix or wire those real paths and is not a rollout gate.
"""
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import secrets
import subprocess
import sys
import threading
import uuid

from synthetic_stop import creation_time


class LifecycleError(RuntimeError):
    """No implicit retry, adoption, journal repair or fallback is authorized."""


class ChildConstructionError(LifecycleError):
    """Cleanup could not be proven; retain the exact Popen owner for teardown."""
    def __init__(self, child):
        super().__init__('synthetic construction cleanup incomplete; retained owned child requires review')
        self.child = child


class TestTeardownError(LifecycleError):
    """All owned children were attempted; failures retain exact cleanup owners."""
    def __init__(self, failures):
        super().__init__('test teardown failed for %d owned child(ren)' % len(failures))
        self.failures = tuple(failures)


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode('ascii')


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _sync_directory(path):
    if os.name != 'nt':
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _timeout(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 10:
        raise LifecycleError('timeout must be finite and in (0, 10]')


class _Journal:
    def __init__(self, directory):
        self.directory = Path(directory)

    @classmethod
    def create(cls, directory):
        journal = cls(directory)
        journal.directory.mkdir()  # Exclusive explicit bootstrap, never adopt.
        with (journal.directory / '.gate').open('xb') as stream:
            stream.write(b'0')
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(journal.directory.parent)
        with journal.gate():
            journal.append({'state': 'empty', 'action': None, 'operation': None,
                            'target': None}, initial=True)
        return journal

    @contextmanager
    def gate(self):
        # Never create a missing gate or remove a stale-looking gate inode.
        with (self.directory / '.gate').open('r+b') as stream:
            if stream.read(1) != b'0':
                raise LifecycleError('missing or invalid lifecycle policy')
            stream.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                stream.seek(0)
                if os.name == 'nt':
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def load(self, *, initial=False):
        names = sorted(p.name for p in self.directory.iterdir() if p.name != '.gate')
        if len(names) > 1024:
            raise LifecycleError('journal capacity exceeded; operator review required')
        previous, previous_hash = None, None
        for sequence, name in enumerate(names):
            if name != '%08d.json' % sequence:
                raise LifecycleError('incomplete or foreign journal artifact')
            with (self.directory / name).open('rb') as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                raise LifecycleError('invalid journal record')
            try:
                record = json.loads(raw)
            except (ValueError, UnicodeError):
                raise LifecycleError('invalid journal record') from None
            if (not isinstance(record, dict) or raw != _encode(record) or
                    set(record) != {'version', 'sequence', 'previous', 'state',
                                    'action', 'operation', 'target'} or
                    type(record['version']) is not int or record['version'] != 1 or
                    type(record['sequence']) is not int or record['sequence'] != sequence or
                    record['previous'] != previous_hash):
                raise LifecycleError('journal chain is invalid')
            self._validate(previous, record)
            previous, previous_hash = record, _digest(raw)
        if previous is None and not initial:
            raise LifecycleError('missing lifecycle policy; recovery denied')
        return previous, previous_hash

    @staticmethod
    def _validate(previous, record):
        state, action, operation, target = (record[k] for k in
                                            ('state', 'action', 'operation', 'target'))
        if state == 'empty':
            if previous is not None or any(v is not None for v in (action, operation, target)):
                raise LifecycleError('invalid bootstrap')
            return
        try:
            if str(uuid.UUID(operation)) != operation:
                raise ValueError()
        except (TypeError, ValueError, AttributeError):
            raise LifecycleError('invalid operation identity') from None
        if target is not None:
            if (not isinstance(target, dict) or set(target) != {'pid', 'creation', 'instance_sha256'} or
                    type(target['pid']) is not int or target['pid'] <= 0 or
                    (target['creation'] is not None and
                     (type(target['creation']) is not int or target['creation'] <= 0)) or
                    not isinstance(target['instance_sha256'], str) or
                    len(target['instance_sha256']) != 64 or
                    any(c not in '0123456789abcdef' for c in target['instance_sha256'])):
                raise LifecycleError('invalid child identity')
        if previous is None:
            raise LifecycleError('bootstrap is missing')
        prior = previous['state']
        valid = False
        if state == 'launch_claimed':
            valid = ((prior == 'empty' and action == 'start' and target is None) or
                     (prior == 'restart_ready' and action == 'restart' and
                      operation == previous['operation'] and target == previous['target']))
        elif state == 'running':
            valid = (prior == 'launch_claimed' and action is None and target is not None and
                     operation == previous['operation'] and target != previous['target'])
        elif state == 'pending':
            valid = (prior == 'running' and action in ('stop', 'restart') and
                     target == previous['target'] and operation != previous['operation'])
        elif state in ('stopped', 'restart_ready'):
            valid = (prior == 'pending' and target == previous['target'] and
                     operation == previous['operation'] and action == previous['action'] and
                     action == ('stop' if state == 'stopped' else 'restart'))
        if not valid:
            raise LifecycleError('invalid lifecycle transition')

    def append(self, payload, *, initial=False):
        previous, previous_hash = self.load(initial=initial)
        if previous is not None and previous['sequence'] >= 1023:
            raise LifecycleError('journal capacity exceeded; operator review required')
        record = dict(payload, version=1, sequence=0 if previous is None else previous['sequence'] + 1,
                      previous=previous_hash)
        self._validate(previous, record)
        raw = _encode(record)
        temporary = self.directory / ('pending-' + uuid.uuid4().hex + '.tmp')
        # Never remove failure artifacts: their presence denies further actions.
        with temporary.open('xb') as stream:
            if stream.write(raw) != len(raw):
                raise OSError('short journal write')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, self.directory / ('%08d.json' % record['sequence']))
        _sync_directory(self.directory)
        temporary.unlink()
        _sync_directory(self.directory)
        return record


class _OwnedTestChild:
    """Only constructed here by Popen; identity files/PIDs cannot attach a child."""
    def __init__(self, *, marker_failure=False, stop_delay=0, ack_mode='normal', idle_lease=10,
                 on_spawn=None):
        self.token = secrets.token_hex(32)
        self.messages = queue.Queue()
        argv = [sys.executable, '-B', '-u',
                str(Path(__file__).with_name('lifecycle_intent_test_service.py')),
                '--synthetic-test-only']
        if marker_failure:
            argv.append('--marker-failure')
        argv.extend(['--stop-delay', str(stop_delay), '--ack-mode', ack_mode,
                     '--idle-lease', str(idle_lease)])
        self.reader = None
        self.creation = None
        self.identity = None
        self.requested = False
        self.stop_operation = None
        self._completion_checked = False
        self._completion_ack = None
        self._teardown_complete = False
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, text=True, encoding='utf-8',
                                        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            # Transfer cleanup ownership BEFORE creation-time queries, threads,
            # or handshake can fail. Only this retained Popen is ever reaped.
            if on_spawn is not None:
                on_spawn(self)
            self.creation = creation_time(int(self.process._handle)) if os.name == 'nt' else None
            self.identity = {'pid': self.process.pid, 'creation': self.creation,
                             'token': self.token, 'synthetic': True}
            self.reader = threading.Thread(target=self._read, daemon=True)
            self.reader.start()
            self.send({'op': 'hello', 'token': self.token})
            if self.receive(5) != self.identity:
                raise LifecycleError('synthetic startup identity mismatch')
            self.verify()
        except BaseException:
            try:
                self.close()
            except BaseException as cleanup_error:
                # Also retain ownership for callers constructing directly,
                # without the coordinator's on_spawn inventory callback.
                raise ChildConstructionError(self) from cleanup_error
            raise

    @property
    def target(self):
        return {'pid': self.process.pid, 'creation': self.creation,
                'instance_sha256': _digest(self.token.encode('ascii'))}

    def _read(self):
        try:
            for line in self.process.stdout:
                try:
                    self.messages.put(json.loads(line))
                except ValueError:
                    self.messages.put(None)
        finally:
            self.messages.put(None)

    def receive(self, timeout):
        try:
            value = self.messages.get(timeout=timeout)
        except queue.Empty:
            raise LifecycleError('synthetic child did not respond') from None
        if value is None:
            raise LifecycleError('synthetic child pipe closed')
        return value

    def send(self, value):
        self.process.stdin.write(json.dumps(value) + '\n')
        self.process.stdin.flush()

    def verify(self):
        if self._teardown_complete:
            raise LifecycleError('final test teardown released ownership; reconciliation denied')
        if os.name == 'nt' and creation_time(int(self.process._handle)) != self.creation:
            raise LifecycleError('retained child handle identity mismatch')
        expected = {'pid': self.process.pid, 'creation': self.creation,
                    'token': self.token, 'synthetic': True}
        if self.identity != expected:
            raise LifecycleError('synthetic identity mismatch')
        if self.process.poll() is None and not self.requested:
            self.send({'op': 'ping', 'identity': self.identity})
            if self.receive(3) != expected:
                raise LifecycleError('synthetic identity challenge failed')

    def close(self):
        """FINAL test teardown, not reconciliation: EOF, reap, then release.

        Ordinary request/finish never call this: they retain the Windows Popen
        handle for identity verification. Failed teardown retains it for retry.
        CPython's retained Handle.Close is used only after wait cached exit and
        both private streams and the reader are closed. No PID lookup or kill.
        """
        if self._teardown_complete:
            return
        try:
            if not self.process.stdin.closed:
                self.process.stdin.close()
        except OSError:
            pass  # Still reap our exact child when its pipe is already broken.
        self.process.wait(timeout=12)
        if self.reader is not None and self.reader.ident is not None:
            self.reader.join(timeout=2)
            if self.reader.is_alive():
                raise LifecycleError('owned child reader cleanup incomplete')
        self.process.stdout.close()
        if not self.process.stdin.closed or not self.process.stdout.closed:
            raise LifecycleError('owned pipe cleanup incomplete')
        if os.name == 'nt':
            self.process._handle.Close()
        self._teardown_complete = True

    def completion_verified(self, operation):
        """Only one exact completion frame followed by EOF is accepted.

        Reap/poll establishes exit; join establishes that all pipe frames arrived.
        No ACK is manufactured from EOF, timeout, lease expiry or zero exit.
        """
        if self._teardown_complete or self.stop_operation != operation or self.process.poll() != 0:
            return False
        self.reader.join(timeout=1)
        if self.reader.is_alive():
            return False
        if not self._completion_checked:
            frames = []
            while True:
                try:
                    frames.append(self.messages.get_nowait())
                except queue.Empty:
                    break
            expected = {'event': 'stop_complete', 'operation': operation,
                        'identity': self.identity}
            if frames == [expected, None]:
                self._completion_ack = expected
            self._completion_checked = True
        return self._completion_ack == {'event': 'stop_complete', 'operation': operation,
                                        'identity': self.identity}


class LifecyclePrototype:
    """One explicit test coordinator; constructor never launches or adopts.

    Callers must serialize object access. All journal/action transactions also
    hold the cross-process gate. close_test_children is teardown, a deliberate
    direct-stop bypass, not a journaled lifecycle operation. It retires owned
    handles after proven cleanup; reconciliation is denied after final teardown.
    """
    def __init__(self, directory):
        self.journal = _Journal(directory)
        self._owned = []
        with self.journal.gate():
            self.journal.load()

    @classmethod
    def create(cls, directory):
        _Journal.create(directory)
        return cls(directory)

    def snapshot(self):
        with self.journal.gate():
            return self.journal.load()[0]

    def _owned_target(self, record):
        matches = [child for child in self._owned if child.target == record['target']]
        if len(matches) != 1:
            raise LifecycleError('retained owned process handle required; operator review')
        child = matches[0]
        child.verify()
        return child

    def _launch(self, record, *, marker_failure=False, stop_delay=0, ack_mode='normal', idle_lease=10):
        claimed = self.journal.append(dict(state='launch_claimed', target=record['target'],
                                          operation=record['operation'] or str(uuid.uuid4()),
                                          action='start' if record['state'] == 'empty' else 'restart'))
        child = _OwnedTestChild(marker_failure=marker_failure, stop_delay=stop_delay,
                                ack_mode=ack_mode, idle_lease=idle_lease, on_spawn=self._owned.append)
        # If this publication fails, launch_claimed remains sticky. No replay.
        self.journal.append(dict(state='running', target=child.target, action=None,
                                 operation=claimed['operation']))
        return child.target

    def start_initial(self, *, marker_failure=False, stop_delay=0, ack_mode='normal', idle_lease=10):
        with self.journal.gate():
            record = self.journal.load()[0]
            if record['state'] != 'empty':
                raise LifecycleError('initial launch requires explicit empty policy')
            return self._launch(record, marker_failure=marker_failure,
                                stop_delay=stop_delay, ack_mode=ack_mode, idle_lease=idle_lease)

    def request(self, action, *, timeout=3):
        _timeout(timeout)
        if action not in ('stop', 'restart'):
            raise LifecycleError('explicit stop or restart intent required')
        with self.journal.gate():
            record = self.journal.load()[0]
            if record['state'] != 'running':
                raise LifecycleError('ambiguous or incomplete intent; operator review')
            child = self._owned_target(record)
            if child.process.poll() is not None:
                raise LifecycleError('unexpected child exit; no automatic recovery')
            pending = self.journal.append(dict(state='pending', action=action,
                                              operation=str(uuid.uuid4()), target=record['target']))
            # Recheck the retained handle immediately before sending, AFTER
            # durable publication. A dead/replaced child never authorizes launch.
            child = self._owned_target(pending)
            child.stop_operation = pending['operation']
            # Sending may fail after bytes were delivered. Keep that ambiguity
            # and stop pinging this pipe; only the matching ACK can resolve it.
            child.requested = True
            child.send({'op': 'stop', 'operation': pending['operation'], 'identity': child.identity})
            try:
                child.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                return 'pending'
            return self._finish(pending)

    def _finish(self, record):
        child = self._owned_target(record)
        code = child.process.poll()
        if code is None:
            return 'pending'
        if code != 0:
            raise LifecycleError('ambiguous/nonzero child exit; intent remains pending')
        if not child.completion_verified(record['operation']):
            return 'pending'
        state = 'stopped' if record['action'] == 'stop' else 'restart_ready'
        self.journal.append(dict(state=state, action=record['action'],
                                 target=record['target'], operation=record['operation']))
        return state

    def finish(self, operation):
        """Reconciliation needs exact operation, completion ACK and owned zero exit.

        This verifies this owned child's exit only, never absence of external
        writers. There is no force, timeout expiry, PID adoption or policy repair.
        """
        with self.journal.gate():
            record = self.journal.load()[0]
            if record['state'] != 'pending' or record['operation'] != operation:
                raise LifecycleError('pending owned operation required')
            return self._finish(record)

    def restart_explicitly(self, operation):
        with self.journal.gate():
            record = self.journal.load()[0]
            if (record['state'] != 'restart_ready' or record['action'] != 'restart' or
                    record['operation'] != operation):
                raise LifecycleError('completed explicit restart intent required')
            child = self._owned_target(record)
            if child.process.poll() != 0 or not child.completion_verified(record['operation']):
                raise LifecycleError('matching completion ACK and verified zero exit required before restart')
            return self._launch(record)

    def close_test_children(self):
        """Attempt every owner, then aggregate operational failures for retry.

        Successful children are finalized idempotently; failed owners remain in
        the inventory and in the exception. This never repairs lifecycle policy.
        """
        failures = []
        for child in self._owned:
            try:
                child.close()
            except Exception as error:
                failures.append((child, error))
        if failures:
            raise TestTeardownError(failures)
