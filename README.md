# InsureAI Airflow

Apache Airflow pipeline for ingesting insurance policy documents, preparing them for retrieval, and loading them into a vector database.

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/mohitvaghela-95/insureai-airflow.git
cd insureai-airflow
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create your local environment file

Copy the example environment file:

```bash
cp .env.example .env
```

The default `.env.example` configures Airflow to:

- Store its local runtime files under `.airflow/`
- Load DAGs from this repository's `dags/` directory
- Disable Airflow's example DAGs

Example:

```bash
AIRFLOW_HOME="$(pwd)/.airflow"
AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Add any application-specific secrets or configuration required by the DAG to `.env`.

> Do not commit `.env` to Git.

### 5. Make the startup script executable

This only needs to be done once after cloning the repository:

```bash
chmod +x run_airflow.sh
```

### 6. Start Airflow

```bash
./run_airflow.sh
```

The startup script:

1. Loads the variables from `.env`
2. Exports them into the Airflow process
3. Runs `airflow standalone`

`airflow standalone` initializes the local Airflow environment and starts the services required for local development.

Once Airflow has started, open:

```text
http://localhost:8080
```

### 7. Log in to the Airflow UI

With Airflow 3's default Simple Auth Manager, the default user is:

```text
Username: admin
```

The generated password is stored in:

```text
$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```

With the default project configuration, this will be:

```text
./.airflow/simple_auth_manager_passwords.json.generated
```

You can view the generated credentials from another terminal with:

```bash
cat .airflow/simple_auth_manager_passwords.json.generated
```

The generated credentials may also be printed in the Airflow startup logs when the user is first created.

### Starting Airflow Again

For future sessions:

```bash
cd insureai-airflow
source .venv/bin/activate
./run_airflow.sh
```

---

## Policy Ingestion DAG Overview

The policy ingestion DAG orchestrates the processing of insurance policy PDFs so that their content can be searched and retrieved later using vector search.

At a high level, the pipeline follows this flow:

```text
Policy PDF
    ↓
Detect / receive file
    ↓
Load PDF and extract page content
    ↓
Split content into chunks
    ↓
Attach document metadata
    ↓
Generate embeddings
    ↓
Store vectors and metadata in Pinecone
    ↓
Move the processed source file
```

### 1. Detect the policy document

The workflow begins when a policy PDF is available in the configured input location.

A file sensor can be used to wait for a document to appear before allowing the rest of the DAG to run.

### 2. Load the PDF

The document is loaded and its text is extracted.

Where possible, page information is retained so that downstream chunks can be traced back to the original document and page.

### 3. Split the document into chunks

The extracted text is divided into smaller chunks that are suitable for embedding and semantic retrieval.

Chunking the policy allows retrieval to return the specific section of a document that is relevant to a user's query rather than returning the entire policy.

### 4. Attach metadata

Each chunk is stored alongside metadata describing its source.

Example metadata:

```python
{
    "source": "example_policy.pdf",
    "page": 12,
    "text": "Policy chunk content..."
}
```

This metadata allows retrieved vectors to be linked back to their original policy document and page.

### 5. Generate embeddings

Each text chunk is converted into a vector embedding.

Embeddings represent the semantic meaning of the text numerically, allowing similar policy content to be found using vector similarity search.

### 6. Store the chunks in Pinecone

The embeddings are uploaded to Pinecone together with their associated metadata.

Conceptually, each stored record contains:

```text
Vector
+
Policy filename
+
Page number
+
Source text / chunk metadata
```

These records can later be queried as part of a Retrieval-Augmented Generation (RAG) workflow.

### 7. Move the processed file

After the ingestion pipeline completes successfully, the original PDF is moved out of the input location.

This prevents the same document from being repeatedly detected and processed and provides a clear separation between pending and processed documents.

## Pipeline Purpose

The output of this DAG is a searchable vector representation of the insurance policy.

A downstream application can then:

```text
User question
    ↓
Create query embedding
    ↓
Search Pinecone
    ↓
Retrieve relevant policy chunks
    ↓
Provide the chunks to an LLM
    ↓
Generate an answer grounded in the policy
```
