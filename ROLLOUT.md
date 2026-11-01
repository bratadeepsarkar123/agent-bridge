# Agent bridge credential rollout and rollback runbook

**Status: draft; incomplete for deployment.** This document covers source and
configuration changes only. The live project remains
`C:\Users\brata\Downloads\claude\agent-bridge-experiment`; this staged tree is
not active there. No live process state, broker database, session database, or
chat was inspected for this documentation update. No service was restarted. No
tests were run as part of this documentation update.

The staged working tree is modified and contains untracked implementation
files, so it is not an immutable release artifact. Record and review a clean
source revision before any rollout. Do not copy this directory over the live
project as an ad hoc deployment.

`lifecycle_intent_prototype.py` is a synthetic-only experiment. Its retained
test-owned process handles and stop acknowledgements do not control the real
supervisor, broker, or launchers. All real recovery and stop paths would need to
enforce the same intent policy before this experiment could address the marker
publication blocker. It does not prove that external database writers are gone.

`lifecycle_policy.py` is also isolated and unwired. It trusts its caller to use
the actual shared recovery gate, verify fresh bootstrap and process evidence,
and dispatch callbacks without waiting while that gate is held. It does not
independently prove a process identity or authorize recovery of the live bridge.

## Code-level gates

The SQLite backup rehearsal exercises synthetic WAL data and restores database
credentials and room metadata to a new path. It does not restore separate DPAPI
credential files, replace an existing database, fence real writers, or validate
open handles. `stop_helper.py` is an in-memory simulation, not a service control.
The CLI schema guard rejects many incompatible schemas but cannot identify a
stale snapshot with the current schema.
The broker now has an isolated `--control-stdin` HTTP drain. It publishes a
flushed recovery-inhibit marker before closing application admission. On Windows,
directory metadata is not guaranteed to survive power loss or rollback. After
successful marker publication, the supervisor's recovery path is fenced while
the marker remains, including after a failed drain. If marker publication fails,
drain is refused and the broker keeps serving (it does not exit), retrying the
marker on each pass, so the supervisor never sees it as crashed. The retry is
unlimited on purpose: exiting would leave no stop note and let the supervisor
auto-recover a broker that was asked to stop. Retries back off from 1 s, doubling
to a 30 s cap. The first three failures are logged, then at most once a minute;
an `ALERT` line is printed at 60 consecutive failures, and the broker keeps
running for operator review. A `stop` line whose marker write fails is retried
the same way (it is not dropped); only a refused publication (`RuntimeError`)
is refused, with a printed message. A refused
publication that makes the broker exit (no marker, discovery retained) still
exists only where the marker already exists or discovery ownership changed; the
startup chokepoint refuses recovery in the first case. A `stop` command or pipe EOF
rejects new parsed requests with 503, interrupts incomplete requests, and waits
for tracked handlers up to `--drain-timeout`. A failed drain reports failure and
retains discovery state and the marker; the controller does not perform a second
drain wait after the deadline. Non-daemon handlers may keep the process alive
until they finish. A successful drain removes discovery state and the marker,
but a final bounded server/process cleanup check may still be needed. The
listener remains open during the wait, so new connections can still be accepted
and rejected and may prolong shutdown.

Focused isolated tests cover an admitted long poll, a short POST body, slow-
trickling headers with a 1-second hard deadline, startup EOF, pipe EOF, blocked
work, new-request rejection, failed-drain behavior, and the recovery fence. This
documentation update did not rerun those tests. A sustained stream of new
arrivals is not tested and may extend shutdown. This does not provide a
coordinated stop/restart path.

Resolve these before a production-like cutover:

1. **No coordinated service stop/restart command.** `broker.py --control-stdin`
   can drain an isolated broker, but it is not wired to the service guard,
   supervisor, or production launcher. After successful stop-marker publication,
   the recovery fence blocks supervisor auto-recovery while the marker remains,
   including after a failed drain. If marker publication fails, drain is refused
   and the broker keeps serving, retrying the marker with backoff (see above); it does not exit into an
   auto-recoverable state. A hard kill before publication remains a plain crash.
   New connections can also extend the drain. A failed drain
   does not get a second drain wait, while successful drain may still need final
   bounded server/process cleanup. `service_guard.py` and `supervisor.py` still
   have no coordinated stop control. Force-stopping a child can cause it to
   respawn; force-stopping a parent can skip its `finally` cleanup and leave lock
   files. A reviewed helper must stop the guard first, stop the supervisor, drain
   and stop children in order, and confirm broker cleanup. Until then, a
   controlled live restart is blocked.
2. **The staged starter is path-bound.** `start-broker.ps1` recognizes a running
   broker only when its command line contains `agent-bridge-experiment`. It is
   wrong for a staged checkout and must be made path-independent and tested.
3. **No supported credential provisioning command.** The only provisioning
   entry point is the Python helper `credential_store.provision_initial_profile`.
   There is no CLI or operator UI, no ACL creation/verification, and no recovery
   flow that identifies an orphaned credential file after an ambiguous failure.
   Its caller must create a private directory. A failure after the database
   commit can leave the caller unsure whether the profile is bound; do not retry
   blindly.
4. **Shared-token collision checking depends on broker initialization.** The
   collision check is populated by `handler_for(broker, token)`. A standalone
   `Broker(database)` used by the provisioning helper does not load the active
   shared token into that check. Random token collision is unlikely, but the
   implementation does not enforce the stated invariant in that provisioning
   path. Add a broker-owned admin operation or otherwise close this gap before
   treating provisioning as complete.
5. **Credential rotation is not end-to-end.** The file helper provisions only
   an unbound profile with `replace=False`. The lower-level broker method can
   replace a hash, but there is no coordinated generation/file/client cutover or
   recovery path. Do not rotate in place. Existing profiles need an explicit,
   reviewed rotation design.
6. **The launcher cannot provision a fresh Claude identity cleanly.**
   `launch-claude.ps1` generates an ID internally for a new session; if an ID is
   supplied, it treats it as a resume. It does not create a fresh supplied ID or
   set session-specific credential environment variables. Add a prepare-then-
   provision-then-launch flow before protecting newly created Claude sessions.
7. **Some clients only support one credential per process.** `native_worker.py`
   has no credential argument and reads the process environment; the MCP client
   also uses one environment credential. `multi_dispatcher.py` supports
   per-recipient files with `--credential SESSION_UUID=PATH`. Do not put several
   protected profiles behind one environment credential; add per-recipient
   support or use the explicitly configured dispatcher.
8. **This authenticates a configured profile, not a real chat/thread.** A local
   process running as the same Windows user can decrypt DPAPI credentials, and
   the broker does not independently prove that a request came from the thread
   named by its profile ID. A profile ID must come from an operator-controlled
   mapping; this feature is not a same-user malware or thread-identity boundary.

The staged broker keeps the shared bearer for unprotected profiles. Protection
starts only after a profile is bound and its client presents that credential.
Staged registration requires explicit opt-in; existing registrations migrated
with the new `opted_in` column default to opted out until they register again.
The live source does not contain scoped credential support, so a scoped client
cannot work against the current live broker. Broker and clients must be upgraded
as a coordinated change.

Direct sends accept an optional stable caller UUID in the broker, Codex Python
client API, CLI (`--message-id`), and Codex and Claude `bridge_send` MCP tools. Reuse the
same UUID with the same sender, recipient, text, and `reply_to` on retries; the
broker returns the existing message for identical content and rejects reuse
with different content. When omitted, the broker creates a fresh ID, so retrying an uncertain no-ID send can create a
duplicate. Do not treat no-ID direct sends as retry-safe.

## Preconditions for a future rollout

- Resolve the code-level gates above, especially controlled shutdown and
  provisioning recovery.
- Review a clean, immutable release commit. The current staged working tree is
  not one.
- Choose one configured profile ID and kind from trusted configuration.
  Do not discover IDs by reading broker/session data. Confirm which client
  process will receive the credential and whether it can use a distinct file.
- Schedule a maintenance window. Existing Claude/Codex processes do not acquire
  changed MCP code or environment automatically. Claude trust/development
  prompts may require human interaction when a session is launched or resumed.
- Back up the source/configuration set and the broker database to an ACL-protected
  local location. The database backup contains queued message data: preserve it
  as opaque data and do not print, query, or share its contents. Use SQLite's
  backup API only while the broker is stopped. Protect any copy of
  `broker-session.json` as a secret; it contains the shared bearer.
- Create a dedicated credential directory under the current user's profile.
  Remove inherited ACLs and grant access only to that user and `SYSTEM`; inspect
  the resulting ACL before provisioning. `os.open(..., 0o600)` in the helper is
  not a Windows ACL guarantee.

## Initial credential provisioning

This is the intended manual sequence for the staged API, **not a supported
operator command yet**. Use it only after the provisioning and shutdown gates
are resolved and the staged broker code is the reviewed release.

1. Stop all bridge services through the reviewed stop helper. Do not kill an
   individual worker while the guard is active. Broker restart does not restart
   agent chats. Existing clients need their MCP processes refreshed separately;
   Claude trust/development prompts may require human interaction on launch or
   resume.
2. Set up the private directory and verify its ACL. Example for the current
   Windows user and `SYSTEM`:

   ```powershell
   $credentialDir = Join-Path $env:LOCALAPPDATA 'AgentBridge\credentials'
   New-Item -ItemType Directory -Force -Path $credentialDir | Out-Null
   $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
   & icacls.exe $credentialDir /inheritance:r /grant:r `
       "$($identity):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F'
   if ($LASTEXITCODE -ne 0) { throw 'Could not set credential directory ACL.' }
   & icacls.exe $credentialDir
   ```

   Verify the resulting ACL contains no unrelated users or groups before
   continuing. Never put a token in a command argument, shell history,
   environment variable, log, or checked-in file. The environment variables
   below contain only a file path and format.

3. With the broker stopped, call the local provisioning API against the exact
   broker database path. For example, after the code gates are resolved:

   ```powershell
   $env:BRIDGE_CREDENTIAL_DIR = 'C:\Users\<user>\AppData\Local\AgentBridge\credentials'
   $env:BRIDGE_PROFILE_ID = '<configured-profile-id>'
   @'
   import json, os
   from pathlib import Path
   from broker import Broker
   from credential_store import provision_initial_profile

   database = Path.cwd() / 'broker.sqlite3'
   if not database.is_file():
       raise SystemExit('broker.sqlite3 is missing from the reviewed release directory')
   result = provision_initial_profile(
       Broker(database),
       os.environ['BRIDGE_PROFILE_ID'],
       os.environ['BRIDGE_CREDENTIAL_DIR'],
   )
   print(json.dumps(result, sort_keys=True))
   '@ | python -B -
   ```

   Run from the reviewed release directory so the database path is unambiguous.
   The helper generates the token in-process, writes a user-bound DPAPI file,
   verifies the file, and stores only its hash in the broker database. The
   printed result contains the profile ID and credential-file path, not the
   token. `Broker(...)` runs schema setup, which is another reason to run only
   with the broker stopped and against the intended database.

4. If the command reports any error or the result is uncertain, stop. Do not
   retry for the same profile: an encrypted orphan or a committed binding may
   exist. The current code has no safe reconciliation command. Resolve this with
   a reviewed admin tool before proceeding.
5. Configure only the intended client process to read that file:

   ```powershell
   $env:BRIDGE_SESSION_CREDENTIAL_FILE = '<returned-DPAPI-file-path>'
   $env:BRIDGE_SESSION_CREDENTIAL_FORMAT = 'dpapi'
   ```

   Start the client under the intended Windows user. Do not set these values
   machine-wide. For a pooled dispatcher, use one explicit `--credential
<profile-id>=<file-path>` for each selected protected recipient. The native
   worker has no equivalent per-recipient option yet.

6. Explicitly opt in the intended profile using the client with that credential
   loaded. The Codex CLI supports:

   ```powershell
   python -B codex_client.py register --id $env:BRIDGE_PROFILE_ID --kind codex
   ```

   `claude_adapter.py` registers its configured Claude session on startup, but
   the current launcher cannot bind a newly generated session ID before launch;
   do not use that path for a protected new Claude profile until the launcher
   flow is fixed.

7. Verify authorization without reading messages. A local check can call
   `BrokerClient().check_session(profile_id)` with the scoped environment set;
   confirm only the expected ID and kind are returned. This proves the broker
   accepted the credential for that configured profile, not the identity of an
   actual chat/thread. Then verify the relevant client starts without an auth
   error. Do not send test content to a live chat as a provisioning check.

## Future restart sequence

The sequence below becomes executable only after a reviewed stop helper exists.
There is no safe equivalent in the current code.

1. Announce the maintenance window and pause new bridge sends. Keep a record of
   the reviewed source revision and configuration snapshot; store bearer-bearing
   configuration only in the private backup location.
2. Run the stop helper. It must stop the service guard first, stop the supervisor
   so it cannot recover the broker, stop workers/monitoring clients, and finally
   stop the broker gracefully. Confirm each process has exited through that
   helper and that the broker has removed its discovery state. Do not delete
   discovery or lock files manually to force a start.
3. Take the stopped-state database backup; preserve it as opaque private data.
   Deploy the complete reviewed source/configuration set atomically. Keep the
   source tree and runtime data paths separate and private.
4. Start the broker and confirm it is reachable on loopback without displaying
   its bearer. Start the supervisor, then only the explicitly selected workers.
   Update MCP configuration/environment for new MCP processes. Existing agent
   chats must not be assumed to reload tools. Claude trust/development prompts
   may require human interaction; the bridge does not handle those prompts.
5. Confirm: broker discovery succeeds; each opted-in profile can pass its
   credential check; unselected profiles are not configured with that
   credential; and no service is repeatedly respawning. Do not treat a queued
   message as proof of delivery. Stop and roll back on any authorization or
   startup uncertainty.

## Rollback

1. Pause sends and use the reviewed stop helper to stop the guard, supervisor,
   workers, and broker in order. Do not restore files while a process can reload
   or respawn the staged code.
2. After the broker is quiescent, make and protect a consistent copy of the
   **latest** database. Preserve it as opaque data; it contains queued messages.
   Keep the earlier pre-rollout backup separately. Do not overwrite either copy.
3. The rollback target must enforce explicit opt-in and per-profile credential
   checks and must be compatible with the current database schema. Restore that
   security-enforcing source/configuration set only. **Never start the old live
   broker against the upgraded database:** it does not enforce staged opt-in or
   protected-credential semantics, and its positional database writes are not
   compatible with the upgraded schema.
4. If no known-compatible security-enforcing broker build is available, leave
   the broker stopped and do not serve traffic. Preserve the latest quiescent
   database copy until a compatible build is ready. Do not reverse-migrate the
   database or restore the older pre-rollout copy as a routine code rollback;
   that can lose messages and remove security state.
5. Keep the encrypted DPAPI files and credential hashes private. Configure
   clients only for the matching security-enforcing broker. Claude
   trust/development prompts may require human interaction when MCP/client
   processes are refreshed.
6. Confirm the compatible broker/client combination is reachable and record the
   outcome without recording bearer values, message bodies, or chat contents.

## Current disposition

**Rollout is blocked.** The staged source has no complete provisioning or
service-control workflow; the start script is path-bound; clients have
single-credential and Claude fresh-session gaps; and the live source does not
support scoped credentials. This draft does not claim those gaps are fixed,
that staged tests pass, or that a restart/rollback has been rehearsed. A rollback
to the pre-upgrade live broker is explicitly unsafe; stop serving if no compatible
security-enforcing release is available.

## Paired backup and restore (gate items 3, 4, 6)

- Make a set only while the broker is stopped: `bridge_backup.make_set(database,
state, credentials_dir, new_set_dir)`. It copies the database with
  `backup_tool.checked_backup` (refuses if discovery state or a stop note exists),
  copies every `credential-*.dpapi` file, and writes `manifest.json` with digests.
  It refuses unless every credential bound in the database has its file.
- Restore only to NEW paths with `bridge_backup.restore_set(set_dir,
new_database, new_credentials_dir, expected_database_sha256=...)`. The
  expected digest must be typed by a person from the set they reviewed (the
  newest set). Restore never replaces an existing database or credential file.
- Keep older sets. Do not overwrite any set.
- Rollback status: the data half is rehearsed (tests/test_rollback_runbook_rehearsal.py).
  No compatible security-enforcing rollback build exists yet, so an upgrade of the
  live bridge stays blocked and the bridge stays stopped.
- Credential file ACLs are not verified by code. Inspect them before relying on a set.
