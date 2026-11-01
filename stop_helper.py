"""Controlled shutdown rehearsal; deliberately incapable of stopping OS processes.

Only this module's in-memory simulated runtime is supported. No PID discovery,
signals, process-tree termination, state files, database access, or restarts occur.
Real test services need a cooperative stop endpoint and a retained process handle
before a real runtime can safely be implemented. A manifest's 'test' label alone
would not authorize termination of a process.
"""
from dataclasses import dataclass
import math
import uuid
from types import MappingProxyType


# Stop restart authorities first, then producers/consumers, then storage.
STOP_ORDER = MappingProxyType({'guard': 0, 'supervisor': 1, 'worker': 2, 'broker': 3})


@dataclass(frozen=True)
class ServiceIdentity:
    name: str
    role: str
    pid: int
    creation: int
    instance: str


@dataclass
class _SimulatedProcess:
    identity: ServiceIdentity
    exit_delay: float
    running: bool = True
    stop_requested: bool = False


class SimulatedRuntime:
    """An isolated process table, never a view of the operating system.

    exit_delay models cooperative exit after a request. Infinity models a hung
    service. Waiting advances only the simulation; it never sleeps.
    """

    def __init__(self):
        self._processes = {}
        self.events = []

    def add(self, name, role, pid, creation, *, exit_delay=0):
        identity = ServiceIdentity(name, role, pid, creation, str(uuid.uuid4()))
        _validate_identity(identity)
        if (isinstance(exit_delay, bool) or not isinstance(exit_delay, (int, float))
                or math.isnan(exit_delay) or exit_delay < 0):
            raise ValueError('invalid simulated exit delay')
        if pid in self._processes and self._processes[pid].running:
            raise ValueError('simulated PID already running')
        if any(p.identity.name == name and p.running for p in self._processes.values()):
            raise ValueError('simulated service name already running')
        self._processes[pid] = _SimulatedProcess(identity, exit_delay)
        return identity

    def _resolve(self, identity):
        process = self._processes.get(identity.pid)
        if process is None or process.identity != identity:
            raise ValueError('unknown or replaced simulated process identity')
        return process

    def _stop(self, identity, timeout):
        # Revalidate immediately before requesting shutdown, not only at preflight.
        process = self._resolve(identity)
        if not process.running:
            return 'already_stopped'
        if not process.stop_requested:
            process.stop_requested = True
            self.events.append(('request_stop', identity.name))
        self.events.append(('wait', identity.name, timeout))
        if process.exit_delay > timeout:
            process.exit_delay -= timeout
            return 'timeout'
        process.running = False
        self.events.append(('exited', identity.name))
        return 'stopped'


def _validate_identity(identity):
    if type(identity) is not ServiceIdentity:
        raise ValueError('explicit simulated identity required')
    if (not isinstance(identity.name, str) or not identity.name.strip()
            or not isinstance(identity.role, str) or identity.role not in STOP_ORDER
            or type(identity.pid) is not int or identity.pid <= 0
            or type(identity.creation) is not int or identity.creation <= 0):
        raise ValueError('invalid simulated process identity')
    try:
        if str(uuid.UUID(identity.instance)) != identity.instance:
            raise ValueError('noncanonical instance token')
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError('invalid instance token') from exc


def stop_services(runtime, services, *, timeout=5):
    """Stop a complete simulated stack in order, aborting at the first timeout.

    The supplied inventory must cover every running simulated service, so an
    omitted restart authority cannot restart something stopped later. All inputs
    and identities are checked before the first shutdown request. Callers must
    serialize access to the simulated runtime. Timeout applies per service. A timeout leaves
    later services running; there is no escalation. Repeating a request is safe.
    """
    if type(runtime) is not SimulatedRuntime:
        raise ValueError('only the in-memory simulated runtime is supported')
    if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout) or not 0 < timeout <= 60):
        raise ValueError('timeout must be finite and between 0 and 60 seconds')
    services = tuple(services)
    for identity in services:
        _validate_identity(identity)
        runtime._resolve(identity)
    if (len({s.pid for s in services}) != len(services)
            or len({s.name for s in services}) != len(services)):
        raise ValueError('duplicate simulated service')
    provided = set(services)
    if any(p.running and p.identity not in provided for p in runtime._processes.values()):
        raise ValueError('complete running simulated inventory required')
    results = []
    for identity in sorted(services, key=lambda s: (STOP_ORDER[s.role], s.name)):
        status = runtime._stop(identity, timeout)
        results.append({'service': identity.name, 'status': status})
        if status == 'timeout':
            break
    return results
