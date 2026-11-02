"""Session-switch guard for the Claude adapter when a scoped credential is set.

A scoped credential (BRIDGE_SESSION_CREDENTIAL_FILE) belongs to exactly one profile.
The adapter follows the live window (for example after /resume). If that window shows
a different session ID while a scoped credential is set, switching would act under an
identity the credential does not authorize. So the switch is refused: the adapter keeps
its original ID and never registers or sends as the new one.

claude_adapter.py is owned by the operator and has uncommitted work, so this module
does not edit it. The hook for claude_adapter.current_id is in ROLLOUT.md (item 6).
"""
import os

SCOPED_CREDENTIAL_FILE_ENV = "BRIDGE_SESSION_CREDENTIAL_FILE"


def scoped_credential_set(environ=None):
    """True when the process was started with a scoped credential file.

    Matches claude_adapter: the variable being present counts, even when empty.
    """
    env = os.environ if environ is None else environ
    return env.get(SCOPED_CREDENTIAL_FILE_ENV) is not None


def switch_allowed(current_id, live_id, environ=None):
    """Return True when the adapter may move from current_id to live_id."""
    if not live_id or live_id == current_id:
        return True
    return not scoped_credential_set(environ)


class SwitchRefusalLog:
    """Writes the refusal once per distinct live ID, not on every poll of current_id."""

    def __init__(self, stream):
        self.stream = stream
        self._seen = set()

    def refuse(self, current_id, live_id):
        if live_id not in self._seen:
            self._seen.add(live_id)
            self.stream.write(
                "agent-bridge: refused switch to {}: this process uses a scoped credential "
                "for {}; restart the window with that session to use it\n".format(live_id, current_id))
            self.stream.flush()
        return current_id
