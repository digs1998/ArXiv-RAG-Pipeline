## ArXiv RAG Pipeline Setup Guide

This guide provides step-by-step instructions to set up the ArXiv RAG Pipeline, a Retrieval-Augmented Generation (RAG) system for summarizing and querying academic papers from ArXiv using Docker, FastAPI, PostgreSQL, Airflow, and Ollama.
Prerequisites

System Requirements: At least 8GB RAM and ~10GB disk space for PDFs and indices.
Operating System: Linux, macOS, or Windows (with WSL2 recommended for Windows).
Dependencies: Docker, Docker Compose, and Git installed.

### Folder Structure

```
.
├── __pycache__
│   └── textChunker.cpython-313-pytest-8.4.2.pyc
├── airflow
│   ├── dags
│   ├── Dockerfile
│   ├── entrypoint.sh
│   └── requirements-airflow.txt
├── api
├── compose.yml
├── data
│   └── arxiv_pdfs
├── Dockerfile
├── gradio_launcher.py
├── Makefile
├── rag_env
│   ├── bin
│   ├── include
│   ├── lib
│   ├── pyvenv.cfg
│   └── share
├── README.md
├── requirements.txt
├── src
│   ├── __pycache__
│   ├── config.py
│   ├── db
│   ├── dependencies.py
│   ├── exceptions.py
│   ├── gradio_app.py
│   ├── main.py
│   ├── models
│   ├── repositories
│   ├── routers
│   ├── schemas
│   └── services
└── textChunker.py
```

### Setup Instructions
1. Install Docker and Docker Compose

Follow the official documentation based on the OS from https://www.docker.com/get-started/


2. Clone the Repository
Clone the ArXiv RAG Pipeline repository to your local machine:

`git clone https://github.com/digs1998/ArXiv-RAG-Pipeline.git`

`cd ArXiv-RAG-Pipeline`

3. Set Up Environment Variables

Create a .env file in the project root to configure environment variables:
`touch .env`

Add the following content to .env:
# Ollama Configuration
OLLAMA_URL=http://ollama:11434
OLLAMA_EMBEDDING_MODEL=nomic-embed-text:latest

# Data Storage
DATA_DIR=/opt/airflow/data

# Database Configuration
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

4. Build and Start the Docker Stack
Build and start all services defined in docker-compose.yml:

`docker-compose up -d --build`

Verify that all containers are running:

`docker ps`

You should see containers for Airflow, PostgreSQL, Ollama, and OpenSearch.

5. Initialize JINA API Key and Ollama

Refer Jina AI (1024) api page to get free API key: https://jina.ai/

While in Ollama, once it is setup, login to the container and pull the latest llama model

`docker exec -it rag-ollama bash`

`docker pull <image-name>/llama3.2` refer https://hub.docker.com/r/ollama/ollama, to get latest image downloaded

6. Configure Airflow

Access the Airflow web interface:
URL: http://localhost:8080
Default credentials: Username: admin, Password: admin


Enable the arxiv_daily_pipeline DAG:
In the Airflow UI, locate the arxiv_daily_pipeline DAG.
Toggle the switch to enable it.



7. (Optional) Set Up Gradio Interface
If you want to use a Gradio interface for querying the pipeline, ensure you have uv installed for running Python scripts with dependencies. Install uv if needed:
curl -LsSf https://astral.sh/uv/install.sh | sh

Run the Gradio app (assuming a Gradio script exists in the project, e.g., app.py):
`uv run python gradio_launcher.py`

If no Gradio script is provided, you can query the system using the OpenSearch client as described in the Usage section below.
Configuration
Key Parameters
Modify arxiv_daily_pipeline.py to adjust pipeline settings:
ARXIV_QUERY = "Explainable AI in medicine"  # Search query
YEAR_START = 2020                           # Earliest paper year
YEAR_END = 2025                            # Latest paper year
BATCH_SIZE = 50                            # Papers per batch

Directory Structure
Ensure the ./data/pdfs/ directory is writable for storing downloaded PDFs:
mkdir -p data/pdfs
chmod -R 777 data/pdfs

Usage
Querying the RAG System
Use the OpenSearch client to perform semantic searches:

from opensearchpy import OpenSearch

client = OpenSearch([{'host': 'localhost', 'port': 9200}])

# Example: Semantic search
```
query = {
    "query": {
        "knn": {
            "embedding": {
                "vector": your_query_embedding,  # Generate using Ollama
                "k": 10
            }
        }
    }
}
```

results = client.search(index="chunks", body=query)

To generate your_query_embedding, use the Ollama API:
curl http://localhost:11434/api/embeddings -d '{"model": "nomic-embed-text:latest", "prompt": "Your query text"}'

Monitoring

Airflow Logs: Check task logs in the Airflow UI (DAG → Task → Logs).
Ollama Logs: Monitor Ollama container logs:docker logs ollama -f


Storage Usage: Check the number of downloaded PDFs:ls -la ./data/pdfs/ | wc -l


Database Status: Verify the number of stored papers:docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM papers;"



Troubleshooting

Ollama Model Not Found:
docker exec ollama ollama pull nomic-embed-text:latest


PDF Download Issues:Verify PDFs are downloading:
ls -la ./data/pdfs/


Database Errors:Ensure text cleaning handles corrupted PDFs (see clean_text_for_db() in processing/parser.py).

Network Issues:Test Ollama connectivity from the Airflow container:
docker exec -it <airflow-container> curl http://ollama:11434/api/tags



Contributing

Fork the repository.
Create a feature branch: git checkout -b feature/your-feature.
Commit changes: git commit -m "Add your feature".
Push to the branch: git push origin feature/your-feature.
Submit a pull request.

License
[Add your license here]
Acknowledgments

Built with guidance from Claude (Anthropic).
Uses open-source models from Ollama.
Leverages Apache Airflow for orchestration.
ArXiv.org for open access to academic papers.
