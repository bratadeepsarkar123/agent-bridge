# Credential rotation design (design only, not implemented)

Status: DESIGN ONLY. Rotation is not implemented on this branch. Nothing in this
document changes broker, client, or launcher behaviour. Existing profiles must not
be rotated in place until the owner has answered the decisions at the end and a
reviewed implementation exists.

Scope: how one bound profile moves from its current credential to a new one, how a
client learns the new credential, how an unclear result is recovered, and how the
old credential is revoked. Sources read: `credential_store.py`, `broker.py`
(`provision_session_credential`, `_authorize`, `credential_is_valid`,
`credential_is_available`, `bound_profile_for_credential`, `profile_status`,
`provision_profile`), `provision_profile.py`, `ROLLOUT.md` items 5 and 8,
`bridge_backup.py` (`verify_pairing`), and `launch-claude.ps1` / `claude_adapter.py`
for where the credential file path lives.

## 1. Plain-language summary

- The broker never stores a credential. It stores only a SHA-256 hash per profile.
  The credential itself lives in one encrypted file per credential, protected by
  Windows DPAPI for the current Windows user.
- Today, the only supported way to give a profile a credential is first-time
  provisioning of an unbound profile. The broker method that can overwrite a hash
  (`replace=True`, the default) exists, but nothing coordinates it with the new
  file, the client, or recovery. That is the gap in ROLLOUT item 5.
- Rotation has to move three things together: the broker's hash, the encrypted file
  on disk, and the client that reads the file. Any of the three can stop halfway, so
  every step must leave the system in a state that is either safe to continue or
  clearly marked for an owner decision.
- The rules that make this safe: write the new file before touching the broker;
  never generate a new token during recovery; never retry a broker change blindly;
  stop and ask a person when the state is unclear (the same "unsure" idea that
  `provision_profile.py` already uses, exit code 3).
- Once the broker's hash is changed, the old credential stops working on the next
  request. No broker restart is needed. The old hash is then recorded as retired so
  that nothing can bind it again.
- A client picks up its new credential only when it starts again with the new file
  path in its environment. Today that path is set in the launcher's process
  environment only, not written to the MCP config, so a rotation needs a restart
  of that one client.

## 2. States a profile can be in

| State    | Broker row                                | Journal row                                | Client file                 | Meaning                                                                                           |
| -------- | ----------------------------------------- | ------------------------------------------ | --------------------------- | ------------------------------------------------------------------------------------------------- |
| unbound  | none in `session_credentials`             | none                                       | none                        | Never provisioned. Any leftover `credential-*.dpapi` file is an orphan.                           |
| bound    | one hash, `session_credentials`           | none                                       | matches that hash           | Normal working state.                                                                             |
| rotating | one hash (old or new), plus a journal row | `prepared`, `file_written`, or `committed` | may still hold the old file | A rotation is in progress. The journal says which step it reached.                                |
| retired  | the old hash is NOT bound                 | `verified` or `abandoned`                  | old file may still exist    | The old credential is revoked and recorded in `retired_credentials`. It can never be bound again. |

Notes:

- "Rotating" is not a separate broker binding. The broker only ever has one live hash
  per profile. The journal is what makes the rotation recoverable.
- A profile can only enter `rotating` from `bound`. A profile that is `unbound` uses
  the existing `provision` path, not rotation.
- `retired` is a property of a hash, not of a profile. A profile returns to `bound`
  with its new hash.

Required schema additions (not implemented; they need a schema version bump and the
broker's existing `require_current_schema_or_empty` check):

- `credential_rotations(id, session_id, state, old_sha256, new_sha256, new_file,
created_at, updated_at)`, with at most one open row per session.
- `retired_credentials(token_sha256 PRIMARY KEY, session_id, retired_at)`.

## 3. Order of operations for one profile

The profile's client is stopped before step 1. No other profile is touched.

### Step 1: record intent (journal `prepared`)

- Check the profile is `bound`, has no open rotation row, and the client is stopped.
- Insert the journal row with `state='prepared'`, the old hash, and the planned file
  name `credential-<new uuid>.dpapi`. No token exists yet.
- Stop if any check fails. Nothing else has changed.

If the process stops here: the journal row is `prepared` with no new file. Recovery
marks it `abandoned`. Nothing else changed.

### Step 2: write the new credential privately (journal `file_written`)

- Generate the token in-process with `secrets.token_urlsafe(32)` (the same rule as
  provisioning).
- Write it with `create_generation` (exclusive create, DPAPI, flushed). Read it back
  with `read_generation` and compare with the generated value. Compute its hash.
- Update the journal row to `state='file_written'` with `new_sha256`.
- The old credential is still the only one bound. The broker is unchanged.

If the process stops here: the new file is an orphan (`inventory_credential_files`
reports it as `orphan`). The old credential still works. Recovery marks the rotation
`abandoned` and keeps the orphan file. It is never deleted automatically.

If the file write itself fails, `create_generation` may leave a partial file. That
file is kept as an orphan and reported, never reused.

### Step 3: cut over the broker (one SQL transaction, journal `committed`)

This is the only broker change. It must be one transaction that does all of:

1. Compare-and-swap: `UPDATE session_credentials SET token_sha256=:new
WHERE session_id=:id AND token_sha256=:old`. Zero rows updated means the binding
   changed since step 1. Refuse; do not force it.
2. Insert the old hash into `retired_credentials`.
3. Set the journal row to `committed`.

The existing `provision_session_credential(replace=True)` does not do (1) or (2).
Rotation needs a new method. Using `replace=True` directly is not acceptable: it
has no expected-old-hash check and no retired record.

After commit, the old credential is rejected on the next request, because
`_authorize` reads `session_credentials` on every call. The live broker does not need
a restart for the hash change.

If the process stops after commit but before step 4: the broker accepts only the new
hash. The client still holds the old file, so it cannot authenticate. That is a safe
failure (no access is granted), and the profile is simply offline until step 4 is
done. Recovery reads the broker, finds the new hash, and continues from step 4.

### Step 4: point the client at the new file

- Stop-and-start the one client with `BRIDGE_SESSION_CREDENTIAL_FILE` set to the new
  file path and `BRIDGE_SESSION_CREDENTIAL_FORMAT=dpapi`, set only for that launch.
- The launcher's resume path does not set these variables today (ROLLOUT item 6).
  Rotation needs a relaunch flow that sets them. This is an open item (section 9).
- No MCP config file needs to change: the path is not stored there. (It is only in
  the launch environment.)

If the process stops here: the client may still have the old path and fail to
authenticate. Recovery shows the state as `committed` with the new hash bound.
Continue at step 4.

### Step 5: verify

- Start the client and run the authorization-only check (`check_session` for that
  profile). It must return the expected ID and kind. Do not send test messages to a
  live chat.
- On success, set the journal row to `verified`.

If verification fails: stop, keep the journal at `committed`, and tell the owner. Do
not roll back to the old hash automatically (see section 6).

### Step 6: retire the old file

- Only after `verified`, and only if no backup set still needs the file for pairing
  (see section 7), move the old file into a `retired` folder inside the credentials
  directory, or delete it, as the owner decides (section 8).
- Record in the journal that the old file was retired. The journal row stays
  `verified`; the retirement is a separate, idempotent mark.

If the process stops here: the old file remains. Its hash is already in
`retired_credentials`, so it has no effect on authorization. The inventory must be
taught to report it as `retired` instead of `orphan`.

## 4. How a client learns its new credential

- The client's only input is the environment variable
  `BRIDGE_SESSION_CREDENTIAL_FILE` (plus `..._FORMAT`). `claude_adapter.py` reads the
  file at start-up. The committed Codex client (`codex_client.py`) only checks that
  the variable is present. Confirm the read point of any client before it is relied
  on.
- The file path is therefore fixed for the life of a process. Changing the file
  requires a client restart. There is no in-process reload.
- The design uses a new file path for every generation (uuid in the name), so the
  old and new credentials never share a file, and a stale process can never read the
  new file by accident.
- A stable pointer file that the client re-reads was considered and rejected for
  now: it needs client code changes and a second source of truth. It is an owner
  decision (section 8).

## 5. Recovering an ambiguous failure without blind retries

The rule: recovery reads state. It does not regenerate, rebind, or retry a broker
write on its own.

The existing "unsure" concept (`ProvisionError(unsure=True)`, `UNSURE_MESSAGE`, CLI
exit code 3) is reused as is. A rotation that cannot establish its state exits with
code 3 and the message "do not retry blindly".

Where an ambiguous result can happen, and how it is decided:

| Failure point                            | Check                                                        | Result                                                                                                                                             |
| ---------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Broker commit raised an error (step 3)   | Read back: is `new_sha256` bound and `old_sha256` not bound? | New bound: treat as `committed`, go to step 4. Old still bound: `abandoned`, keep the new file as an orphan. Neither bound: exit 3, owner decides. |
| Process died between steps               | Journal state plus the broker read-back above                | Same table as the step descriptions in section 3.                                                                                                  |
| CAS updated zero rows                    | Re-read the binding                                          | Changed by someone else: refuse, exit 1, journal `abandoned`, owner reviews.                                                                       |
| Read-back of the new file fails (step 2) | Not bound yet                                                | `abandoned`, orphan kept, exit 1.                                                                                                                  |

Recovery commands (design, not implemented):

- `provision_profile.py rotate-status PROFILE --credentials-dir DIR`: read-only.
  Shows the journal row, the bound hash, the file classification, and the matching
  next step. Writes nothing.
- `provision_profile.py rotate-resolve PROFILE --outcome committed|abandoned`: the
  only write. It re-checks the database against the claimed outcome before writing
  anything. It never generates a token and never deletes a file.
- No automatic retry of step 3. A retry would reuse a CAS on a value that may already
  have been applied; the read-back decides instead.

## 6. Revoking the old credential, and why it cannot be restored

Revocation happens in one place: the step 3 transaction. Three things follow:

1. The old hash is gone from `session_credentials`, so `_authorize` rejects the old
   token immediately, for every process, with no restart.
2. The old hash is recorded in `retired_credentials`. `provision_session_credential`
   and `provision_initial_profile` must refuse any token whose hash is listed there.
   This is the enforced part of "cannot be restored".
3. The old file is retired in step 6. This is hygiene, not the security boundary.

Why it cannot be restored, stated precisely:

- The broker keeps no plaintext token. A hash cannot be turned back into a token. So
  the broker cannot recreate the old credential by itself.
- The only remaining copy of the old token is the old encrypted file. Anything
  running as the same Windows user can decrypt it (ROLLOUT item 8). So the file is
  not a safe rollback source, and the design treats it as a secret to retire.
- Restoring the old binding would mean deliberately re-binding that retired hash.
  The design forbids that path: the retired check refuses it. A rollback to the old
  credential is therefore not possible without changing that rule.
- Deleting the file is not a guaranteed erase. NTFS journaling, SSD wear levelling,
  shadow copies, and backup sets can keep old bytes. This is why the design does not
  claim deletion is secure.
- Backup sets are the largest hole. A backup taken before rotation contains the old
  hash and possibly the old file. Restoring it would bring back the old binding.
  `bridge_backup.verify_pairing` only checks that every bound hash has a file, so it
  would pass. The restore path must also refuse any hash in `retired_credentials`
  (or the owner must accept that a restore can revive a retired credential). Open
  item, section 9.

## 7. Interaction with existing code

- `provision_session_credential(replace=True)` stays as it is for now. Rotation must
  not call it. A new method performs the CAS and retired-hash write in one transaction.
- `provision_initial_profile` must also refuse retired hashes when generating a token.
  A collision with a retired hash is practically impossible, but the check costs
  nothing and keeps the rule in one place.
- `inventory_credential_files` must report `retired` (hash in `retired_credentials`)
  and `rotating` (journal row open) separately from `orphan`. Today it reports only
  `bound`, `orphan`, and `unreadable`, and a retired file would look like an orphan.
- `bridge_backup.verify_pairing` must treat a retired hash that is still in a backup
  as a warning, not a pass.
- The broker schema changes need the existing schema-version check. A rotation is not
  possible on a database without the new tables. The live database migration is a
  separate owner-approved step.

## 8. Decisions the owner must make

1. Old file after a verified rotation: (a) move it to a `retired` folder and delete
   it later by hand, (b) delete it at once, or (c) keep it in place. Recommendation:
   (a), with a stated retention period, because deletion is not a guaranteed erase but
   the folder keeps the file out of normal use.
2. How a client picks up the new file: (a) a planned restart of that one client in a
   stated window, using a relaunch flow that sets the variables, or (b) a stable
   pointer file that the client re-reads, which needs client code changes.
   Recommendation: (a), because it needs no client change and the window is short.
3. Whether a retired credential may ever be bound again, including by restoring an
   older backup set: (a) never; the restore refuses retired hashes, or (b) allowed
   after a written owner decision for each case. Recommendation: (a).

## 9. What the design leaves open

- The relaunch flow for a rotated Claude session (the launcher resume path does not
  set the credential variables; ROLLOUT item 6).
- The exact schema and migration for `credential_rotations` and
  `retired_credentials`, and the live database migration.
- The backup restore rule for retired hashes (section 6).
- Whether rotation can run while other profiles are active. The design assumes only
  one profile is rotated at a time, and that its client is stopped.
- Who may run `rotate-resolve` and where its audit record goes. The design assumes the
  local operator and a log line without any secret.
- Multi-profile rotation, scheduled rotation, and automatic rotation after suspected
  exposure are out of scope. Each needs its own design.
- Client read points for the Codex and Claude clients on every path; confirm before
  implementation.
- Owner-SID checks on the credentials directory (already listed as a limit in
  `credential_store.py`); not changed by this design.

## 10. Test plan (for the later implementation)

All tests use real DPAPI and real NTFS ACLs on Windows, and skip on other platforms,
as the existing `test_credential_store.py` does. They never touch the live
`broker.sqlite3`, the live credentials folder, or any live process.

Setup:

- Each test creates its own `tempfile.TemporaryDirectory()` for the credentials
  folder (created with `prepare_credentials_directory`, which applies the private ACL)
  and a new SQLite database built from the current schema.
- A guard assertion fails the test if any path resolves under the repository root or
  the live credentials folder.

Cases:

1. Happy path: bound profile, rotate, verify `bound` with the new hash, old hash
   rejected by `_authorize`, old hash listed in `retired_credentials`, inventory
   reports the old file as `retired` after step 6.
2. Crash injection between each pair of steps (1-2, 2-3, 3-4, 4-5, 5-6): simulate
   the stop by raising at the boundary, then run `rotate-status` and check the state
   and next step. No test may create a second new token.
3. Broker commit raises after the transaction (read-back path): new hash bound leads
   to `committed`; old hash bound leads to `abandoned` with the orphan kept; neither
   leads to exit code 3 and no file change.
4. CAS mismatch: the binding changed between steps 1 and 3; the commit is refused and
   nothing else changes.
5. Retired hash refusal: `provision_session_credential`, `provision_initial_profile`,
   and the new rotation method all refuse a retired token.
6. Partial new file: a truncated generation file is kept, reported as unreadable or
   orphan, and never bound.
7. Read-only commands: `rotate-status` and `recover` leave every file and row
   unchanged (compare a snapshot before and after).
8. Backup pairing: a backup set taken before rotation is flagged by
   `verify_pairing` (or by the new retired-hash check) and is not silently accepted.
9. Client start: a fake client launch reads only the new path; the old file is not
   opened after step 4. Use a fake environment only; do not launch `claude.exe`.
10. Exit codes: 0, 1, 2, and 3 map as in `provision_profile.py`.

Manual rehearsal (owner-run, after implementation and review): on a disposable copy of
the credentials folder and a copy of the database, with the broker stopped, run the
full sequence once and record the results. No live profile is rotated until the
owner says yes.
