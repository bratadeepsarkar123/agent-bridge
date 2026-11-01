# Snapshot freshness checks

`snapshot_freshness.py` compares a snapshot to a signed checkpoint marker that
must be obtained independently of the snapshot. It does not open a database
through SQLite, restore files, discover processes, or contact a service.
`sha256_file` reads the exact path supplied by its caller and returns only its
digest; this task uses it only with synthetic temporary files.

## Trust boundary

At a trusted checkpoint authority, hash the completed snapshot and sign its
source ID, generation, and SHA-256 digest with `issue_checkpoint_marker`. Keep
the HMAC key outside the snapshot and publish the serialized marker to a
separate, access-controlled checkpoint location. A verifier needs that key to
check the marker; because HMAC is symmetric, every verifier holding the key can
also issue markers.

During assessment, obtain the latest marker through a trusted external pointer
and read the generation from the snapshot's checkpoint metadata. Use
`assess_snapshot_file` to hash the supplied artifact, or hash it with
`sha256_file` and pass the digest to `assess_snapshot`.
The marker proves the metadata was signed by a key holder and binds the digest
to a source and generation. The `matches_external_checkpoint_marker` result
means only that the exact supplied artifact matches that marker.

A valid signature does not prove that a marker is the latest one. A previously
valid marker can be replayed. The marker must come from an independently
maintained latest-marker pointer or monotonic checkpoint service; never select
the marker from the snapshot being checked. This module reports a match to the
supplied marker and never labels a snapshot simply “fresh.” Without a trusted
marker, the result is `unknown_no_external_marker`; timestamps, schema version,
filenames, and the snapshot's own claims cannot replace the marker.

## Outcomes and failure cases

| Outcome | Meaning and safe interpretation |
| --- | --- |
| `unknown_no_external_marker` | No external evidence was supplied. Freshness is unknown. |
| `untrusted_external_marker` | Signature, key, marker shape, or version did not verify. Do not use it as freshness evidence. |
| `source_mismatch` | The snapshot and marker identify different sources; the comparison is invalid. |
| `behind_external_checkpoint` | Snapshot metadata reports a lower generation than the supplied marker. Treat it as behind that checkpoint; the digest may also differ. |
| `ahead_of_external_checkpoint` | Snapshot reports a higher generation. The marker may be old, or the snapshot may be inconsistent; obtain the trusted latest marker again. |
| `content_does_not_match_marker` | Generations agree but bytes do not. This may mean corruption, modification, a different artifact, or a bad marker. It is not a match. |
| `matches_external_checkpoint_marker` | Source, generation, digest, and marker signature agree. This is a match to the supplied marker, not proof that the marker is current or the backup is complete. |

Other failure limits:

- A stale but correctly signed marker can pass signature verification. External
  latest-pointer freshness and anti-replay state are responsibilities of the
  checkpoint store and caller.
- A compromised HMAC key can authorize false markers. Keep it out of snapshots
  and restrict access; this implementation does not provide public-key
  separation or key rotation.
- A matching digest proves byte identity only. It does not prove a backup
  captured every required record, is restorable, is schema-compatible, or came
  from a quiescent database. Those require separate backup and restore checks.
- The file is not locked or pinned while hashing. If it changes during or after
  assessment, the result does not authorize restoring later bytes. Assess an
  immutable copy and ensure the restore consumes those same bytes (or recheck
  the digest immediately before restore).
- A mismatch is deliberately not classified as corruption: wrong source,
  wrong generation, an older artifact, altered bytes, and a wrongly published
  marker require investigation before any restore decision.
- If snapshot metadata has no reliable generation, the caller cannot make a
  generation comparison. It must report the result as unknown or compare the
  artifact digest against an externally published expected digest only.

`tests/test_snapshot_freshness.py` uses synthetic bytes and temporary files only.
It does not access live databases, processes, or services.
