"""Reusable ISOLATED lifecycle policy; not wired into any service entry point.

Supply a factory for the SAME broker recovery_gate used by EVERY cooperating
supervisor, launcher, recovery and policy writer. This module neither imports
the broker nor creates its own competing lock. Use ``with policy.locked() as p``
for reads, explicit bootstrap and transitions. Never retain a session after that
context exits. Missing legacy policy is denial, never permission to recover.

Effects run only after a successful claim commit, inside that gate. Their caller
must supply trusted ownership checks and perform only the claimed action. A
callback must only dispatch the action and return without waiting for any child
that may need the same gate; wait and verify outside the gate. Bootstrap alone
does not prove that a broker or legacy discovery state is absent: a real caller
must first verify a fresh installation or use an explicit migration procedure.
A callback error leaves the claim pending, with no automatic retry. Completion
requires exact operation/generation IDs, an operation-bound ACK, and verified
owned zero-exit evidence. Evidence objects are assertions from a TRUSTED owning
controller, not cryptographic proof: this module proves no process handles,
absence of external writers, database quiescence, or authenticity of the ACK.
Controllers must allocate never-reused operation/generation UUIDs: a compact
snapshot checks current bindings, not uniqueness across all historical IDs.

One bounded versioned snapshot is published by atomic replacement after file
fsync. A separate temporary hard-link witness survives failures before/after
replacement. Readers deny all actions if a witness/swap artifact exists. The
directory is synced before witness removal, which is the last publication step.
Witness removal is deliberately not directory-synced: its resurrection after
power loss is a safe denial, not permission. There is no TTL or automatic repair.
An operation can commit just before a crash without the caller learning that;
claims cannot be replayed. This does NOT make effects atomic with policy storage.

LIMITS: Windows file fsync does not establish directory-entry durability; all
filesystems/controllers/storage need suitable persistence and atomic-replace/
hard-link semantics. Rolling back or deleting the snapshot AND witnesses can
defeat freshness; revision checks are CAS, not a trusted anti-rollback counter.
Trusted private parent paths and restrictive Windows ACLs are caller obligations
(Python modes are not ACL policy). Same-user malicious code, lock inode/path
replacement, direct start/stop bypass, mixed/legacy versions that ignore policy,
and noncooperating external writers defeat guarantees. Future real integration
must enforce explicit policy under the same recovery gate everywhere. This new
module does not repair or authorize rollout of the existing live paths.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid


VERSION = 1
MAX_REVISION = 2 ** 63 - 1
MAX_SNAPSHOT_BYTES = 8192
_UNSET = object()
_KEYS = {'version', 'policy_id', 'service_id', 'revision', 'state', 'action',
         'operation_id', 'generation_id', 'previous_generation_id', 'completion'}


class PolicyError(RuntimeError):
    """Deny actions. No implicit retry, bootstrap, adoption or artifact cleanup."""


@dataclass(frozen=True)
class StopEvidence:
    """Trusted controller assertion, not proof obtained/verified by this module."""
    operation_id: str
    generation_id: str
    acknowledged: bool
    owned_exit_verified: bool
    exit_code: int


def _uuid(value):
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError()
    except (TypeError, ValueError, AttributeError):
        raise PolicyError('canonical UUID identity required') from None
    return value


def _revision(value):
    if type(value) is not int or not 0 <= value <= MAX_REVISION:
        raise PolicyError('invalid policy revision')


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('ascii')


def _serialize(value):
    # Accidental corruption detection only: trusted writers can recompute this.
    return _encode({'snapshot': value, 'sha256': hashlib.sha256(_encode(value)).hexdigest()})


def _sync_directory(directory):
    if os.name != 'nt':
        fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _evidence(value, operation, generation):
    if type(value) is not StopEvidence:
        raise PolicyError('trusted owning-controller completion evidence required')
    _uuid(value.operation_id)
    _uuid(value.generation_id)
    if (value.operation_id != operation or value.generation_id != generation or
            value.acknowledged is not True or value.owned_exit_verified is not True or
            type(value.exit_code) is not int or value.exit_code != 0):
        raise PolicyError('matching ACK and verified owned zero exit required')
    return asdict(value)


class LifecyclePolicy:
    def __init__(self, path, *, policy_id, service_id, recovery_gate):
        # Resolve once so a later working-directory change cannot silently
        # redirect a cooperating caller to a different policy snapshot.
        self.path = Path(path).resolve()
        self.policy_id = _uuid(policy_id)
        self.service_id = _uuid(service_id)
        if not callable(recovery_gate):
            raise PolicyError('caller-supplied shared recovery gate required')
        self._gate = recovery_gate

    @contextmanager
    def locked(self):
        # The caller is responsible for supplying the actual shared gate, not a
        # no-op or a different broker's gate. Python callers are trusted code.
        with self._gate():
            session = _Session(self)
            try:
                yield session
            finally:
                session._active = False


class _Session:
    def __init__(self, policy):
        self.policy = policy
        self._active = True
        self._failed = False

    def _check(self):
        if not self._active or self._failed:
            raise PolicyError('closed or failed policy session; action denied')

    def _artifacts(self):
        path = self.policy.path
        prefixes = (path.name + '.pending-', path.name + '.swap-')
        return any(p.name.startswith(prefixes) for p in path.parent.iterdir())

    def _validate(self, value):
        if (type(value) is not dict or set(value) != _KEYS or
                type(value['version']) is not int or value['version'] != VERSION or
                value['policy_id'] != self.policy.policy_id or value['service_id'] != self.policy.service_id):
            raise PolicyError('unknown or foreign lifecycle policy')
        _revision(value['revision'])
        state, action = value['state'], value['action']
        op, gen, previous = (value[k] for k in ('operation_id', 'generation_id', 'previous_generation_id'))
        proof = value['completion']
        if value['revision'] == 0:
            if (state != 'stopped' or any(v is not None for v in (action, op, gen, previous, proof))):
                raise PolicyError('invalid explicit bootstrap')
            return
        _uuid(op)
        _uuid(gen)
        if previous is not None:
            _uuid(previous)
            if previous == gen:
                raise PolicyError('replacement requires a distinct generation')
        valid = False
        if state == 'running':
            valid = action is None and previous is None and proof is None
        elif state == 'pending':
            valid = action in ('stop', 'restart') and previous is None and proof is None
        elif state == 'launch_claimed':
            valid = ((action == 'start' and previous is None and proof is None) or
                     (action == 'restart' and previous is not None and proof is not None))
        elif state in ('stopped', 'restart_ready'):
            valid = (action == ('stop' if state == 'stopped' else 'restart') and
                     previous is None and proof is not None)
        if not valid:
            raise PolicyError('invalid or ambiguous lifecycle state')
        if proof is not None:
            if type(proof) is not dict or set(proof) != set(StopEvidence.__dataclass_fields__):
                raise PolicyError('invalid completion evidence')
            _evidence(StopEvidence(**proof), op, previous if state == 'launch_claimed' else gen)

    def read(self):
        self._check()
        try:
            if self._artifacts():
                raise PolicyError('incomplete policy publication; operator review required')
            with self.policy.path.open('rb') as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise PolicyError('regular policy snapshot required')
                raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
            if len(raw) > MAX_SNAPSHOT_BYTES:
                raise PolicyError('oversized lifecycle policy')
            envelope = json.loads(raw)
            if (type(envelope) is not dict or set(envelope) != {'snapshot', 'sha256'} or
                    raw != _encode(envelope)):
                raise PolicyError('noncanonical or duplicate policy fields')
            value = envelope['snapshot']
            if envelope['sha256'] != hashlib.sha256(_encode(value)).hexdigest():
                raise PolicyError('snapshot checksum mismatch')
            self._validate(value)
            return value
        except (OSError, ValueError, TypeError, RecursionError, PolicyError) as error:
            self._failed = True
            raise PolicyError('policy missing, corrupt, foreign or incomplete; action denied') from error

    def bootstrap(self):
        """Explicit absent-policy initialization only; never reset existing state."""
        self._check()
        try:
            if self._artifacts():
                raise PolicyError('incomplete bootstrap; operator review required')
            try:
                self.policy.path.lstat()
            except FileNotFoundError:
                pass
            else:
                raise PolicyError('policy already exists; bootstrap refused')
            value = dict(version=VERSION, policy_id=self.policy.policy_id, service_id=self.policy.service_id,
                         revision=0, state='stopped', action=None, operation_id=None,
                         generation_id=None, previous_generation_id=None, completion=None)
            self._publish(value)
            return value
        except BaseException:
            self._failed = True
            raise

    def _publish(self, value):
        self._check()
        self._validate(value)
        raw = _serialize(value)
        path = self.policy.path
        witness = path.with_name(path.name + '.pending-' + uuid.uuid4().hex)
        swap = path.with_name(path.name + '.swap-' + uuid.uuid4().hex)
        try:
            fd = os.open(str(witness), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                if stream.write(raw) != len(raw):
                    raise OSError('short snapshot write')
                stream.flush()
                os.fsync(stream.fileno())
            os.link(witness, swap)
            os.replace(swap, path)
            _sync_directory(path.parent)
            # Last fallible publication step. A crash resurrecting this witness
            # denies future actions. Failure artifacts are NEVER auto-removed.
            witness.unlink()
        except BaseException:
            self._failed = True
            raise

    def _current(self, revision, operation=_UNSET, generation=_UNSET):
        self._check()
        _revision(revision)
        current = self.read()
        if current['revision'] != revision or revision == MAX_REVISION:
            self._failed = True
            raise PolicyError('stale or exhausted policy revision')
        if operation is not _UNSET:
            _uuid(operation)
            if operation != current['operation_id']:
                raise PolicyError('operation identity mismatch')
        if generation is not _UNSET:
            _uuid(generation)
            if generation != current['generation_id']:
                raise PolicyError('generation identity mismatch')
        return current

    def _commit(self, current, **changes):
        updated = dict(current, **changes)
        updated['revision'] += 1
        self._publish(updated)
        return updated

    def claim_launch(self, revision, *, operation_id, generation_id, effect, completion=None):
        """Commit a non-replayable launch claim BEFORE invoking trusted effect."""
        self._check()
        try:
            _uuid(operation_id)
            _uuid(generation_id)
            if not callable(effect):
                raise PolicyError('explicit trusted launch effect required')
            current = self._current(revision)
            if current['revision'] == 0:
                if completion is not None:
                    raise PolicyError('initial launch cannot borrow completion evidence')
                action, previous, proof = 'start', None, None
            elif current['state'] == 'restart_ready':
                if operation_id != current['operation_id'] or generation_id == current['generation_id']:
                    raise PolicyError('restart operation or replacement generation mismatch')
                proof = _evidence(completion, operation_id, current['generation_id'])
                if proof != current['completion']:
                    raise PolicyError('restart completion mismatch')
                action, previous = 'restart', current['generation_id']
            else:
                raise PolicyError('pending/stopped/claimed/running policy denies launch')
            claimed = self._commit(current, state='launch_claimed', action=action,
                                   operation_id=operation_id, generation_id=generation_id,
                                   previous_generation_id=previous, completion=proof)
            effect()
            return claimed
        except BaseException:
            self._failed = True
            raise

    def mark_running(self, revision, *, operation_id, generation_id):
        """Controller reports successful owned launch; this module proves no handle."""
        self._check()
        try:
            current = self._current(revision, operation_id, generation_id)
            if current['state'] != 'launch_claimed':
                raise PolicyError('exact launch claim required')
            return self._commit(current, state='running', action=None,
                                previous_generation_id=None, completion=None)
        except BaseException:
            self._failed = True
            raise

    def request(self, revision, *, action, operation_id, generation_id, effect):
        """Persist sticky stop/restart intent before the owning stop callback."""
        self._check()
        try:
            _uuid(operation_id)
            if action not in ('stop', 'restart') or not callable(effect):
                raise PolicyError('explicit stop/restart and trusted effect required')
            current = self._current(revision, generation=generation_id)
            if current['state'] != 'running' or operation_id == current['operation_id']:
                raise PolicyError('running generation and a fresh operation required')
            pending = self._commit(current, state='pending', action=action, operation_id=operation_id)
            effect()
            return pending
        except BaseException:
            self._failed = True
            raise

    def complete(self, revision, *, evidence):
        self._check()
        try:
            if type(evidence) is not StopEvidence:
                raise PolicyError('owning-controller evidence required, not a PID')
            current = self._current(revision, evidence.operation_id, evidence.generation_id)
            if current['state'] != 'pending':
                raise PolicyError('exact pending intent required')
            proof = _evidence(evidence, current['operation_id'], current['generation_id'])
            return self._commit(current, state='stopped' if current['action'] == 'stop' else 'restart_ready',
                                completion=proof)
        except BaseException:
            self._failed = True
            raise
