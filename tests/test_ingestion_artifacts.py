import tempfile
import unittest
from pathlib import Path

from dags.ingestion_artifacts import (
    chunk_page_records,
    create_artifact_path,
    read_records,
    write_records,
)


class IngestionArtifactsTest(unittest.TestCase):
    def test_records_round_trip_through_jsonl_artifact(self):
        records = [
            {
                "page_content": "Policy wording",
                "metadata": {"source": "/raw/policy.pdf", "page_num": 1},
            }
        ]

        with tempfile.TemporaryDirectory() as temporary_directory:
            artifact_path = create_artifact_path(
                temporary_directory, "/raw/policy.pdf", "pages"
            )
            self.assertEqual(write_records(artifact_path, records), artifact_path)
            self.assertEqual(list(read_records(artifact_path)), records)

    def test_chunking_preserves_metadata_and_overlap(self):
        records = [
            {
                "page_content": "abcdefghij",
                "metadata": {"source": "/raw/policy.pdf", "page_num": 3},
            },
            {
                "page_content": "",
                "metadata": {"source": "/raw/policy.pdf", "page_num": 4},
            },
        ]

        chunks = chunk_page_records(records, chunk_size=4, chunk_overlap=1)

        self.assertEqual([chunk["page_content"] for chunk in chunks], ["abcd", "defg", "ghij"])
        self.assertEqual(
            [chunk["metadata"] for chunk in chunks],
            [
                {"source": "/raw/policy.pdf", "page_num": 3, "chunk_index": 0},
                {"source": "/raw/policy.pdf", "page_num": 3, "chunk_index": 1},
                {"source": "/raw/policy.pdf", "page_num": 3, "chunk_index": 2},
            ],
        )

    def test_invalid_chunk_settings_are_rejected(self):
        with self.assertRaises(ValueError):
            chunk_page_records([], chunk_size=0)
        with self.assertRaises(ValueError):
            chunk_page_records([], chunk_size=5, chunk_overlap=5)

    def test_artifact_name_includes_source_and_kind(self):
        artifact_path = create_artifact_path("/tmp/staging", "/raw/annual-policy.pdf", "chunks")

        self.assertEqual(artifact_path.parent, Path("/tmp/staging"))
        self.assertTrue(artifact_path.name.startswith("annual-policy-chunks-"))
        self.assertEqual(artifact_path.suffix, ".jsonl")
