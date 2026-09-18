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
from pypdf import PdfReader
from uuid6 import uuid6

load_dotenv()
FILE_PATH_RAW = os.environ["FILE_PATH_RAW"]
FILE_PATH_PROCESSED = os.environ["FILE_PATH_PROCESSED"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
EMBEDDING_MODEL = os.environ["EMBEDDING_MODEL"]
PINECONE_API_KEY = os.environ["PINECONE_API_KEY"]
PINECONE_INDEX_NAME = os.environ["PINECONE_INDEX_NAME"]


@dag(
    start_date=datetime(2026, 9, 18),  # noqa: DTZ001
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
)
def policy_ingestion():

    @task.sensor(poke_interval=30, timeout=60 * 60 * 24, mode="reschedule")
    def wait_for_pdf():
        raw_path = FILE_PATH_RAW

        if raw_path:
            raw_path = Path(raw_path)
            pdf_files = list(raw_path.glob("*.pdf"))

        if not pdf_files:
            return PokeReturnValue(is_done=False)

        file_path = pdf_files[0]

        return PokeReturnValue(
            is_done=True,
            xcom_value=str(file_path),
        )

    @task()
    def extract_documents(pdf_path: str | Path):
        """Extract text from every page in a PDF file."""

        reader = PdfReader(pdf_path)

        documents = []
        for i, page in enumerate(reader.pages):
            doc = Document(
                page_content=page.extract_text(),
                metadata={"source": pdf_path, "page_num": i + 1},
            )
            documents.append(doc)

        return documents

    @task()
    def chunk_documents(
        documents: list[Document], chunk_size: int = 900, chunk_overlap: int = 150
    ) -> list[Document] | None:
        chunks = []
        for doc in documents:
            text = doc.page_content
            text_length = len(text)

            if text_length == 0:
                continue

            start = 0
            c_index = 0
            while start < text_length:
                # Calculate end position
                end = min(start + chunk_size, text_length)
                # print(f"end: {end}")

                # Extract chunk
                chunk = text[start:end]
                if chunk:  # Only add non-empty chunks
                    d = Document(
                        page_content=chunk,
                        metadata={
                            "source": str(doc.metadata["source"]),
                            "page_num": doc.metadata["page_num"],
                            "chunk_index": c_index,
                        },
                    )
                    chunks.append(d)

                    c_index += 1

                # If we have reached the last chunk then break
                if end >= text_length:
                    break

                # Calculate next starting position
                start = end - chunk_overlap
                # print(f"Starting new chunk from index: {start}")

        return chunks

    @task()
    def embed_chunks(chunks: list[Document]):
        pc = Pinecone(PINECONE_API_KEY)

        # Create index if it doesnt exist
        if not pc.has_index(PINECONE_INDEX_NAME):
            pc.create_index(
                name=PINECONE_INDEX_NAME,
                metric="cosine",
                dimension=1536,
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )

        index = pc.Index(PINECONE_INDEX_NAME)
        embeddings = OpenAIEmbeddings(model=EMBEDDING_MODEL)
        vector_store = PineconeVectorStore(index=index, embedding=embeddings)
        uuids = [str(uuid6()) for _ in range(len(chunks))]

        vector_store.add_documents(documents=chunks, uuids=uuids)

    @task()
    def structure_metadata():
        pass

    @task()
    def store_embeddings():
        pass

    @task()
    def move_file_to_processed(source):
        destination = Path(FILE_PATH_PROCESSED)  # type: ignore
        source = Path(source)

        if destination:
            destination = destination / source.name
            shutil.move(str(source), str(destination))

        return f"File moved to: {destination}"

    pdf_path = wait_for_pdf()
    documents = extract_documents(pdf_path)  # type: ignore
    chunks = chunk_documents(documents)  # type: ignore
    embeddings = embed_chunks(chunks)  # type: ignore

    move_task = move_file_to_processed(pdf_path)

    embeddings >> move_task  # type: ignore


policy_ingestion()
