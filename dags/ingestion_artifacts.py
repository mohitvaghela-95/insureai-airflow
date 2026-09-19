"""Helpers for exchanging ingestion payloads through staged JSONL artifacts."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

ArtifactRecord = dict[str, Any]


def create_artifact_path(
    staging_directory: str | Path, source_path: str | Path, artifact_kind: str
) -> Path:
    """Return a collision-resistant path for an ingestion-run artifact."""

    source_name = Path(source_path).stem
    return Path(staging_directory) / f"{source_name}-{artifact_kind}-{uuid4().hex}.jsonl"


def write_records(path: str | Path, records: Iterable[ArtifactRecord]) -> Path:
    """Persist records atomically as JSON Lines and return their artifact path."""

    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = artifact_path.with_suffix(f"{artifact_path.suffix}.tmp")

    with temporary_path.open("w", encoding="utf-8") as artifact_file:
        for record in records:
            artifact_file.write(json.dumps(record))
            artifact_file.write("\n")

    temporary_path.replace(artifact_path)
    return artifact_path


def read_records(path: str | Path) -> Iterator[ArtifactRecord]:
    """Yield JSONL records from a staged ingestion artifact."""

    with Path(path).open(encoding="utf-8") as artifact_file:
        for line in artifact_file:
            if line.strip():
                yield json.loads(line)


def extract_page_records(pdf_path: str | Path) -> list[ArtifactRecord]:
    """Extract each PDF page into a JSON-serializable record."""

    from pypdf import PdfReader

    source = str(pdf_path)
    reader = PdfReader(source)
    return [
        {
            "page_content": page.extract_text() or "",
            "metadata": {"source": source, "page_num": page_number},
        }
        for page_number, page in enumerate(reader.pages, start=1)
    ]


def chunk_page_records(
    records: Iterable[ArtifactRecord], chunk_size: int = 900, chunk_overlap: int = 150
) -> list[ArtifactRecord]:
    """Split page records while preserving source, page, and chunk metadata."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if not 0 <= chunk_overlap < chunk_size:
        raise ValueError("chunk_overlap must be at least zero and less than chunk_size")

    chunks: list[ArtifactRecord] = []
    for record in records:
        text = record["page_content"]
        if not text:
            continue

        metadata = record["metadata"]
        start = 0
        chunk_index = 0
        while start < len(text):
            end = min(start + chunk_size, len(text))
            chunk = text[start:end]
            if chunk:
                chunks.append(
                    {
                        "page_content": chunk,
                        "metadata": {
                            "source": str(metadata["source"]),
                            "page_num": metadata["page_num"],
                            "chunk_index": chunk_index,
                        },
                    }
                )
                chunk_index += 1

            if end == len(text):
                break
            start = end - chunk_overlap

    return chunks
