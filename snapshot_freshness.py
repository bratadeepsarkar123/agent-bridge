"""Compare a snapshot with an independently trusted checkpoint marker.

This module cannot establish freshness from a snapshot, its timestamp, or its
schema. A match means only that the supplied snapshot digest and generation
match a valid signed marker. The caller must obtain the marker from a trusted
external source that identifies the latest checkpoint.
"""

from dataclasses import dataclass
from enum import Enum
import hashlib
import hmac
import json
from pathlib import Path
import re
from typing import Any


MARKER_VERSION = 1
MINIMUM_SIGNING_KEY_BYTES = 32
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class AssessmentStatus(str, Enum):
    UNKNOWN_NO_MARKER = "unknown_no_external_marker"
    UNTRUSTED_MARKER = "untrusted_external_marker"
    SOURCE_MISMATCH = "source_mismatch"
    BEHIND_MARKER = "behind_external_checkpoint"
    AHEAD_OF_MARKER = "ahead_of_external_checkpoint"
    CONTENT_MISMATCH = "content_does_not_match_marker"
    MATCHES_MARKER = "matches_external_checkpoint_marker"


@dataclass(frozen=True)
class CheckpointMarker:
    """Signed metadata stored outside the snapshot it describes."""

    source_id: str
    generation: int
    snapshot_sha256: str
    signature: str
    version: int = MARKER_VERSION

    def to_json(self) -> str:
        """Serialize the marker without including any signing key material."""
        _validate_marker_fields(self)
        return json.dumps(
            {
                "version": self.version,
                "source_id": self.source_id,
                "generation": self.generation,
                "snapshot_sha256": self.snapshot_sha256,
                "signature": self.signature,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, value: str) -> "CheckpointMarker":
        """Parse a marker, rejecting duplicate or unknown fields."""
        if not isinstance(value, str):
            raise ValueError("marker JSON must be text")
        try:
            document = json.loads(value, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("invalid marker JSON") from exc
        expected = {"version", "source_id", "generation", "snapshot_sha256", "signature"}
        if not isinstance(document, dict) or set(document) != expected:
            raise ValueError("marker JSON has an invalid shape")
        marker = cls(
            source_id=document["source_id"],
            generation=document["generation"],
            snapshot_sha256=document["snapshot_sha256"],
            signature=document["signature"],
            version=document["version"],
        )
        _validate_marker_fields(marker)
        return marker


@dataclass(frozen=True)
class SnapshotAssessment:
    status: AssessmentStatus
    snapshot_generation: int
    marker_generation: int | None

    @property
    def matches_marker(self) -> bool:
        """Whether exact snapshot bytes match the supplied signed marker."""
        return self.status is AssessmentStatus.MATCHES_MARKER


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash a file incrementally; file contents are never returned or logged."""
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def issue_checkpoint_marker(
    snapshot_sha256: str,
    source_id: str,
    generation: int,
    signing_key: bytes,
) -> CheckpointMarker:
    """Sign snapshot metadata at the trusted checkpoint authority.

    Keep ``signing_key`` outside snapshots and make marker publication atomic
    in the external checkpoint store. This helper does not store either one.
    """
    _validate_source_and_generation(source_id, generation)
    _validate_digest(snapshot_sha256, "snapshot_sha256")
    _validate_key(signing_key)
    unsigned = CheckpointMarker(
        source_id=source_id,
        generation=generation,
        snapshot_sha256=snapshot_sha256,
        signature="0" * 64,
    )
    signature = hmac.new(
        signing_key, _canonical_payload(unsigned), hashlib.sha256
    ).hexdigest()
    return CheckpointMarker(source_id, generation, snapshot_sha256, signature)


def assess_snapshot(
    snapshot_sha256: str,
    source_id: str,
    generation: int,
    marker: CheckpointMarker | None,
    verification_key: bytes | None,
) -> SnapshotAssessment:
    """Compare snapshot evidence with a marker obtained independently.

    ``generation`` must be read from the snapshot's own checkpoint metadata.
    It is not trusted on its own; the signed digest binds the exact artifact.
    A marker match is not a claim that the marker is current or that the
    snapshot contains every expected record.
    """
    _validate_digest(snapshot_sha256, "snapshot_sha256")
    _validate_source_and_generation(source_id, generation)
    if marker is None:
        return SnapshotAssessment(AssessmentStatus.UNKNOWN_NO_MARKER, generation, None)

    if verification_key is None or not verify_checkpoint_marker(marker, verification_key):
        return SnapshotAssessment(AssessmentStatus.UNTRUSTED_MARKER, generation, None)

    if source_id != marker.source_id:
        status = AssessmentStatus.SOURCE_MISMATCH
    elif generation < marker.generation:
        status = AssessmentStatus.BEHIND_MARKER
    elif generation > marker.generation:
        status = AssessmentStatus.AHEAD_OF_MARKER
    elif not hmac.compare_digest(snapshot_sha256, marker.snapshot_sha256):
        status = AssessmentStatus.CONTENT_MISMATCH
    else:
        status = AssessmentStatus.MATCHES_MARKER
    return SnapshotAssessment(status, generation, marker.generation)


def assess_snapshot_file(
    snapshot_path: str | Path,
    source_id: str,
    generation: int,
    marker: CheckpointMarker | None,
    verification_key: bytes | None,
) -> SnapshotAssessment:
    """Hash the supplied artifact, then compare it with an external marker."""
    return assess_snapshot(
        sha256_file(snapshot_path), source_id, generation, marker, verification_key
    )


def verify_checkpoint_marker(marker: Any, verification_key: bytes) -> bool:
    """Verify marker integrity/authenticity; this does not prove it is latest."""
    if type(marker) is not CheckpointMarker:
        return False
    try:
        _validate_marker_fields(marker)
        _validate_key(verification_key)
    except (TypeError, ValueError):
        return False
    expected = hmac.new(
        verification_key, _canonical_payload(marker), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, marker.signature)


def _canonical_payload(marker: CheckpointMarker) -> bytes:
    return json.dumps(
        {
            "version": marker.version,
            "source_id": marker.source_id,
            "generation": marker.generation,
            "snapshot_sha256": marker.snapshot_sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _validate_marker_fields(marker: CheckpointMarker) -> None:
    if type(marker) is not CheckpointMarker or type(marker.version) is not int or marker.version != MARKER_VERSION:
        raise ValueError("unsupported checkpoint marker")
    _validate_source_and_generation(marker.source_id, marker.generation)
    _validate_digest(marker.snapshot_sha256, "snapshot_sha256")
    _validate_digest(marker.signature, "signature")


def _validate_source_and_generation(source_id: str, generation: int) -> None:
    if (not isinstance(source_id, str) or not source_id or source_id != source_id.strip()
            or len(source_id) > 256):
        raise ValueError("source_id must be non-empty canonical text")
    if type(generation) is not int or generation < 0:
        raise ValueError("generation must be a non-negative integer")


def _validate_digest(value: str, name: str) -> None:
    if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _validate_key(key: bytes) -> None:
    if not isinstance(key, bytes) or len(key) < MINIMUM_SIGNING_KEY_BYTES:
        raise ValueError(f"key must contain at least {MINIMUM_SIGNING_KEY_BYTES} bytes")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate marker JSON field")
        result[key] = value
    return result
