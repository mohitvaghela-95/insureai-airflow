import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

from airflow.sdk import PokeReturnValue, dag, task
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone, ServerlessSpec
from uuid6 import uuid6

from ingestion_artifacts import (
    chunk_page_records,
    create_artifact_path,
    extract_page_records,
    read_records,
    write_records,
)

load_dotenv()
logger = logging.getLogger(__name__)


def required_setting(setting_name: str) -> str:
    """Read required runtime configuration without breaking DAG parsing."""

    value = os.getenv(setting_name)
    if not value:
        raise ValueError(f"{setting_name} must be configured before this task runs")
    return value


@dag(
    start_date=datetime(2026, 9, 18),  # noqa: DTZ001
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
)
def policy_ingestion():
    @task.sensor(poke_interval=30, timeout=60 * 60 * 24, mode="reschedule")
    def wait_for_pdf():
        raw_path = Path(required_setting("FILE_PATH_RAW"))
        pdf_files = list(raw_path.glob("*.pdf"))

        if not pdf_files:
            return PokeReturnValue(is_done=False)

        return PokeReturnValue(is_done=True, xcom_value=str(pdf_files[0]))

    @task()
    def extract_documents(pdf_path: str) -> str:
        """Extract PDF pages to a staged artifact and return its path via XCom."""

        page_artifact = create_artifact_path(
            required_setting("FILE_PATH_STAGING"), pdf_path, "pages"
        )
        return str(write_records(page_artifact, extract_page_records(pdf_path)))

    @task()
    def chunk_documents(page_artifact_path: str) -> str:
        """Chunk staged pages and return the chunk artifact path via XCom."""

        chunk_artifact = create_artifact_path(
            required_setting("FILE_PATH_STAGING"), page_artifact_path, "chunks"
        )
        chunks = chunk_page_records(read_records(page_artifact_path))
        return str(write_records(chunk_artifact, chunks))

    @task()
    def embed_chunks(chunk_artifact_path: str):
        """Create LangChain Documents locally from staged records for embedding."""

        pc = Pinecone(required_setting("PINECONE_API_KEY"))
        index_name = required_setting("PINECONE_INDEX_NAME")

        if not pc.has_index(index_name):
            pc.create_index(
                name=index_name,
                metric="cosine",
                dimension=1536,
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )

        documents = [
            Document(page_content=record["page_content"], metadata=record["metadata"])
            for record in read_records(chunk_artifact_path)
        ]
        if not documents:
            logger.info(
                "No text chunks found in %s; skipping embedding.", chunk_artifact_path
            )
            return

        index = pc.Index(index_name)
        embeddings = OpenAIEmbeddings(model=required_setting("EMBEDDING_MODEL"))
        vector_store = PineconeVectorStore(index=index, embedding=embeddings)
        uuids = [str(uuid6()) for _ in documents]
        vector_store.add_documents(documents=documents, uuids=uuids)

    @task()
    def finalize_ingestion(
        source_path: str, page_artifact_path: str, chunk_artifact_path: str
    ) -> str:
        """Remove successful-run artifacts and move the source PDF to processed."""

        for artifact_path in (page_artifact_path, chunk_artifact_path):
            try:
                Path(artifact_path).unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove staging artifact %s", artifact_path)

        source = Path(source_path)
        destination_directory = Path(required_setting("FILE_PATH_PROCESSED"))
        destination_directory.mkdir(parents=True, exist_ok=True)
        destination = destination_directory / source.name
        shutil.move(str(source), str(destination))
        return f"File moved to: {destination}"

    pdf_path = wait_for_pdf()
    page_artifact = extract_documents(pdf_path)  # type: ignore[arg-type]
    chunk_artifact = chunk_documents(page_artifact)
    embeddings = embed_chunks(chunk_artifact)
    finalization = finalize_ingestion(pdf_path, page_artifact, chunk_artifact)  # type: ignore[arg-type]

    embeddings >> finalization


policy_ingestion()
