# Agent bridge credential rollout and rollback runbook

**Status: draft; incomplete for deployment.** This document covers source and
configuration changes only. The live project remains
`C:\Users\brata\Downloads\claude\agent-bridge-experiment`; this staged tree is
not active there. No live process state, broker database, session database, or
chat was inspected for this documentation update. No service was restarted. No
tests were run as part of this documentation update.

Correction (2026-11-02): the staged tree is now committed at `5293321` on the
local branch `bridge/integrated-plan`, with a clean working tree. That commit is
local only; it is not on the public remote (`origin/main` is `d6ff391`). Review
the commit before any rollout. Do not copy this directory over the live project
as an ad hoc deployment.

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

1. **No coordinated service-level stop/restart command.** `broker_stop.py` now
   requests a broker drain through the request file watched by `broker_launcher.py`;
   the staged starter, the Claude recovery launcher, and supervisor recovery all
   start the broker through `broker_launcher.py`, so each holds the control pipe.
   This still does not coordinate the service guard or the supervisor process
   itself (the stop-request channel for both is described in the status below). After successful stop-marker
   publication, the recovery fence blocks supervisor auto-recovery while the
   marker remains, including after a failed drain. If marker publication fails,
   drain is refused and the broker keeps serving, retrying the marker with
   backoff (see above); it does not exit into an auto-recoverable state. A hard
   kill before publication remains a plain crash. New connections can also
   extend the drain. A failed drain does not get a second drain wait, while
   successful drain may still need final bounded server/process cleanup.
   `service_guard.py` and `supervisor.py` have no coordinated stop control in the
   broker drain. Force-stopping a child can cause it to respawn; force-stopping a parent can
   skip its `finally` cleanup and leave lock files. A reviewed service-level
   helper must stop the guard first, stop the supervisor, request the broker
   drain, stop remaining children in order, and confirm cleanup. Until then, a
   controlled live restart is blocked.

   **Status (branch `feature/service-stop-helper`, not merged):** `service_stop.py`
   implements this order with `stop` and `restart` subcommands, `--dry-run` and
   `--root`. `tests/test_service_stop.py` covers it with injected fakes only; it
   has not been run against real processes.

   **Guard and supervisor stop channel (branch `feature/graceful-guard-supervisor-stop`):**
   `service_guard.py` checks for `service-guard.stop-request` before each pass and
   while it sleeps (in 1 s slices). On a request it consumes the file and exits its
   loop without spawning anything; it never signals its children. `supervisor.py`
   does the same with `supervisor.stop-request`, checked before each pass and again
   just before any recovery launch, so a stop cannot start a recovery. It never
   writes to or removes the discovery state, the stop-pending marker or the
   recovery lock. `service_stop.py` writes the request file for the guard and the
   supervisor, then waits with the same bounded timeout. A process that does not
   exit is reported as a failed step. It is not force-killed, and the request file
   stays so the process can still exit on its next check. A leftover request file
   fails the cleanup check. Tests use fakes and temp folders only. Still open: on
   Windows, the children's exit request is a non-forced `taskkill`, which may not
   stop a windowless process. Live use stays blocked until a real process has been
   stopped this way on a disposable copy.

2. **The staged starter is path-bound.** `start-broker.ps1` recognizes a running
   broker only when its command line contains `agent-bridge-experiment`. It is
   wrong for a staged checkout and must be made path-independent and tested.
3. **Provisioning CLI added (closed in code on this branch, pending review and a
   real-process check).** `provision_profile.py` offers `provision`, `check`, and
   `recover`. `recover` is report-only and never deletes or overwrites a file.
   A failure after a possible database commit exits 3 and says "unsure, do not
   retry blindly"; the outcome is confirmed by reading back the token hash. The
   credentials directory is created private through `credential_store`'s ACL
   helper and refused if an existing directory is not private.
4. **Shared-token collision check is loaded on the provisioning path (closed in
   code on this branch).** `Broker.provision_profile(...)` loads the active shared
   token into the collision check before it provisions, and the CLI calls only
   that operation. `handler_for` uses the same `Broker.load_shared_token` method.
   A standalone `Broker(database)` therefore enforces the invariant on this path.
   The CLI cannot see a shared token when the broker is stopped, so the check is
   not loaded in that case (reported in its output).
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
  Remove inherited ACLs and grant access only to that user and `SYSTEM`.
  `os.open(..., 0o600)` in the helper is not a Windows ACL guarantee. The code
  now enforces the check: `credential_store.apply_private_acl` sets the ACL with
  `icacls.exe` and verifies it, `provision_initial_profile` refuses any directory
  whose ACL is not private (fail closed, no file written), `bridge_backup.make_set`
  makes its set and credentials directories private, and `restore_set` refuses an
  existing credentials directory and makes the new one private.

## Initial credential provisioning

This is the intended manual sequence for the staged API. The provisioning CLI
(`provision_profile.py`) exists on this branch, but **the rollout is still
blocked** by the shutdown and other gates. Use this sequence only after those
gates are resolved and the staged broker code is the reviewed release.

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

   The provisioning helper repeats this check and stops if any other user,
   group, or DENY entry is present. Still inspect the printed ACL yourself: the
   check trusts the current account name from the environment and does not
   verify the owner SID. Never put a token in a command argument, shell history,
   environment variable, log, or checked-in file. The environment variables
   below contain only a file path and format.

3. With the broker stopped, provision the profile with the supported CLI from
   the reviewed release directory. The CLI reads the active shared token from
   `broker-session.json` when it exists; when the broker is stopped that file is
   absent and the output reports `shared_token_checked: false`:

   ```powershell
   python -B provision_profile.py provision '<configured-profile-id>' `
       --credentials-dir "$env:LOCALAPPDATA\AgentBridge\credentials"
   ```

   `provision` creates a NEW credentials directory with a private ACL, or
   verifies an existing one and refuses it if it is not private. It refuses a
   profile that is already bound, and a token that collides with the loaded
   shared token, before writing anything. The helper generates the token
   in-process, writes a user-bound DPAPI file, verifies it, and stores only its
   hash. Output contains the profile ID and credential-file path, never the token.

4. If `provision` exits 3 ("unsure"), stop and do not retry. The binding may be
   committed. Run `python -B provision_profile.py check '<profile-id>'` to see
   whether the profile is bound, and `recover --credentials-dir <dir>` to list
   each credential file as bound, orphan, or unreadable. `recover` is read-only:
   it never binds, overwrites, or deletes a file. An orphan is an owner decision.
   If `provision` exits 1, the profile was not bound; a kept orphan file is
   listed in the message and is never deleted automatically.
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
- Credential directory and file ACLs are checked by code (Windows `icacls`; only the
  current account and `SYSTEM` may hold entries; DENY entries fail). Limits: the owner
  SID is not checked, and same-account software can still decrypt DPAPI credentials
  (accepted, see LIMITS_REGISTER.md item 5). Inspect the ACL before relying on a set.
