import os
import shutil
from datetime import datetime
from pathlib import Path

from airflow.sdk import PokeReturnValue, dag, task
from dotenv import load_dotenv
from openai import OpenAI
from pypdf import PdfReader

load_dotenv()


@dag(
    start_date=datetime(2026, 9, 18),  # noqa: DTZ001
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
)
def policy_ingestion():

    @task.sensor(poke_interval=30, timeout=60 * 60 * 24, mode="reschedule")
    def wait_for_pdf():
        raw_path = os.getenv("FILE_PATH_RAW")

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
    def extract_text(pdf_path: str | Path):
        """Extract text from every page in a PDF file."""

        reader = PdfReader(pdf_path)

        pages = [page.extract_text() for page in reader.pages]

        text = " ".join(pages)

        return text

    @task()
    def chunk_text(
        text: str, chunk_size: int = 900, chunk_overlap: int = 150
    ) -> list[str] | None:

        chunks: list[str] = []
        text_length = len(text)
        # print("text len: ", text_length)

        if text_length == 0:
            return chunks

        start = 0
        while start < text_length:
            # Calculate end position
            end = min(start + chunk_size, text_length)
            # print(f"end: {end}")

            # Extract chunk
            chunk = text[start:end]
            if chunk:  # Only add non-empty chunks
                chunks.append(chunk)
                # print(f"appended {start}:{end}")

            # If we have reached the last chunk then break
            if end >= text_length:
                break

            # Calculate next starting position
            start = end - chunk_overlap
            # print(f"Starting new chunk from index: {start}")

        return chunks

    @task()
    def embed_chunks(chunks: list[str]):
        api_key = os.getenv("OPENAI_API_KEY")
        model = os.getenv("EMBEDDING_MODEL")

        if api_key and model:
            client = OpenAI(api_key=api_key)
            response = client.embeddings.create(model=model, input=chunks)

        return response.data

    @task()
    def structure_metadata():
        pass

    @task()
    def store_embeddings():
        pass

    @task()
    def move_file_to_processed(source):
        destination = Path(os.getenv("FILE_PATH_PROCESSED"))  # type: ignore
        source = Path(source)

        if destination:
            destination = destination / source.name
            shutil.move(str(source), str(destination))

        return f"File moved to: {destination}"

    pdf_path = wait_for_pdf()
    text = extract_text(pdf_path)  # type: ignore
    chunks = chunk_text(text)  # type: ignore
    embeddings = embed_chunks(chunks)  # type: ignore

    move_task = move_file_to_processed(pdf_path)

    embeddings >> move_task  # type: ignore


policy_ingestion()
