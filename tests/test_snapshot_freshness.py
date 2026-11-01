"""Local synthetic tests for externally checkpointed snapshot assessment."""

from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from snapshot_freshness import (
    AssessmentStatus,
    CheckpointMarker,
    assess_snapshot,
    assess_snapshot_file,
    issue_checkpoint_marker,
    sha256_file,
    verify_checkpoint_marker,
)


KEY = b"synthetic-checkpoint-key-32-bytes-minimum"
SOURCE = "synthetic-broker"
SNAPSHOT = b"synthetic snapshot bytes"
SNAPSHOT_DIGEST = hashlib.sha256(SNAPSHOT).hexdigest()


class SnapshotFreshnessTests(unittest.TestCase):
    def marker(self, *, data=SNAPSHOT, source=SOURCE, generation=12, key=KEY):
        return issue_checkpoint_marker(
            hashlib.sha256(data).hexdigest(), source, generation, key
        )

    def assess(self, *, digest=SNAPSHOT_DIGEST, source=SOURCE, generation=12,
               marker=None, key=KEY):
        return assess_snapshot(digest, source, generation, marker, key)

    def test_snapshot_without_external_marker_has_unknown_status(self):
        result = self.assess()
        self.assertIs(AssessmentStatus.UNKNOWN_NO_MARKER, result.status)
        self.assertFalse(result.matches_marker)

    def test_exact_artifact_and_generation_match_signed_marker(self):
        marker = self.marker()
        result = self.assess(marker=marker)
        self.assertIs(AssessmentStatus.MATCHES_MARKER, result.status)
        self.assertTrue(result.matches_marker)

    def test_older_snapshot_is_behind_external_generation(self):
        marker = self.marker(generation=13)
        result = self.assess(generation=12, marker=marker)
        self.assertIs(AssessmentStatus.BEHIND_MARKER, result.status)

    def test_snapshot_generation_ahead_of_marker_is_not_accepted(self):
        marker = self.marker(generation=11)
        result = self.assess(generation=12, marker=marker)
        self.assertIs(AssessmentStatus.AHEAD_OF_MARKER, result.status)

    def test_same_generation_with_different_bytes_is_not_a_match(self):
        marker = self.marker()
        result = self.assess(
            digest=hashlib.sha256(b"different synthetic bytes").hexdigest(),
            marker=marker,
        )
        self.assertIs(AssessmentStatus.CONTENT_MISMATCH, result.status)

    def test_source_mismatch_is_not_compared_as_same_checkpoint(self):
        result = self.assess(marker=self.marker(source="another-source"))
        self.assertIs(AssessmentStatus.SOURCE_MISMATCH, result.status)

    def test_modified_marker_and_wrong_key_fail_closed(self):
        marker = self.marker()
        modified = replace(marker, generation=13)
        self.assertFalse(verify_checkpoint_marker(modified, KEY))
        result = self.assess(marker=modified)
        self.assertIs(AssessmentStatus.UNTRUSTED_MARKER, result.status)
        self.assertIsNone(result.marker_generation)
        self.assertIs(AssessmentStatus.UNTRUSTED_MARKER,
                      self.assess(marker=marker, key=b"x" * 32).status)

    def test_marker_json_round_trip_and_rejects_ambiguous_or_unknown_fields(self):
        marker = self.marker()
        self.assertEqual(marker, CheckpointMarker.from_json(marker.to_json()))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            CheckpointMarker.from_json(
                '{"version":1,"version":1,"source_id":"x",'
                '"generation":1,"snapshot_sha256":"' + "0" * 64 + '",'
                '"signature":"' + "0" * 64 + '"}'
            )
        with self.assertRaisesRegex(ValueError, "shape"):
            CheckpointMarker.from_json(
                marker.to_json()[:-1] + ',"extra":true}'
            )

    def test_sha256_file_reads_synthetic_file_incrementally(self):
        with tempfile.TemporaryDirectory(prefix="snapshot-check-") as directory:
            path = Path(directory) / "synthetic.snapshot"
            path.write_bytes(SNAPSHOT)
            self.assertEqual(SNAPSHOT_DIGEST, sha256_file(path, chunk_size=3))

    def test_file_assessment_hashes_the_exact_synthetic_artifact(self):
        with tempfile.TemporaryDirectory(prefix="snapshot-check-") as directory:
            path = Path(directory) / "synthetic.snapshot"
            path.write_bytes(SNAPSHOT)
            result = assess_snapshot_file(path, SOURCE, 12, self.marker(), KEY)
        self.assertIs(AssessmentStatus.MATCHES_MARKER, result.status)

    def test_invalid_identity_generation_digest_and_key_are_rejected(self):
        for source, generation in (("", 1), (" source ", 1), (SOURCE, True), (SOURCE, -1)):
            with self.subTest(source=source, generation=generation), self.assertRaises(ValueError):
                self.marker(source=source, generation=generation)
        with self.assertRaisesRegex(ValueError, "lowercase SHA-256"):
            issue_checkpoint_marker("A" * 64, SOURCE, 1, KEY)
        with self.assertRaisesRegex(ValueError, "at least 32 bytes"):
            issue_checkpoint_marker(SNAPSHOT_DIGEST, SOURCE, 1, b"short")
        with self.assertRaisesRegex(ValueError, "positive integer"):
            sha256_file("unused", chunk_size=0)


if __name__ == "__main__":
    unittest.main()
