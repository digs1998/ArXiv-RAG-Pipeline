# ArXiv RAG Pipeline

A comprehensive **Retrieval-Augmented Generation (RAG)** pipeline that automatically ingests academic papers from ArXiv, processes them into searchable chunks, generates embeddings, and stores them for semantic search and question-answering.

## 🔧 Architecture

This pipeline combines several technologies to create an end-to-end RAG system:

- **Apache Airflow** - Orchestrates the entire workflow
- **ArXiv API** - Fetches academic paper metadata
- **PDF Processing** - Downloads and parses full-text papers  
- **Ollama** - Generates embeddings locally using `nomic-embed-text`
- **OpenSearch** - Stores and indexes embeddings for semantic search
- **PostgreSQL** - Stores paper metadata and text chunks

## 🚀 Features

- ✅ **Automated Daily Ingestion** - Scheduled to run daily at 10:30 PM CST
- ✅ **Smart PDF Download** - Downloads papers within specified year range
- ✅ **Text Extraction & Chunking** - Processes PDFs into searchable segments
- ✅ **Local Embeddings** - Uses Ollama for privacy-preserving embedding generation
- ✅ **Batch Processing** - Optimized batch embedding generation (~0.2s per embedding)
- ✅ **Error Handling** - Robust error handling with retries and fallbacks
- ✅ **Volume Persistence** - Downloaded PDFs and data persist across restarts

## 📂 Project Structure

```
├── airflow/
│   └── dags/
│       └── arxiv_daily_pipeline.py     # Main Airflow DAG
├── ingestion/
│   ├── client/
│   │   └── arxivClient.py              # ArXiv API client
│   ├── downloaders/
│   │   └── pdfDownloader.py            # Async PDF downloader
│   ├── processing/
│   │   └── parser.py                   # PDF parsing and chunking
│   ├── embeddings.py                   # Ollama embedding generation
│   ├── opensearchClient.py             # OpenSearch indexing
│   └── db.py                          # Database models and operations
├── data/
│   └── pdfs/                          # Downloaded PDF storage
└── docker-compose.yml                 # Complete stack definition
```

## 🛠️ Setup

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM recommended
- ~10GB disk space for PDFs and indices

### 1. Clone and Start Services

```bash
# Clone your repository
git clone <your-repo-url>
cd arxiv-rag-pipeline

# Start the complete stack
docker-compose up -d

# Verify all services are running
docker ps
```

### 2. Initialize Ollama

```bash
# Pull the embedding model
docker exec ollama ollama pull nomic-embed-text:latest

# Verify model is available
docker exec ollama ollama list
```

### 3. Access Airflow

- **Airflow UI**: http://localhost:8080
- **Default credentials**: admin/admin
- **Enable the DAG**: Toggle on `arxiv_daily_pipeline`

## ⚙️ Configuration

### Key Parameters

In `arxiv_daily_pipeline.py`:

```python
ARXIV_QUERY = "Explainable AI in medicine"  # Search query
YEAR_START = 2020                           # Earliest paper year
YEAR_END = 2025                            # Latest paper year  
BATCH_SIZE = 50                            # Papers per batch
```

### Environment Variables

```bash
# Ollama Configuration
OLLAMA_URL=http://ollama:11434
OLLAMA_EMBEDDING_MODEL=nomic-embed-text:latest

# Data Storage
DATA_DIR=/opt/airflow/data

# Database (auto-configured in docker-compose)
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
```

## 🔄 Pipeline Workflow

The DAG consists of 5 sequential tasks:

### 1. **Initialize** (`init`)
- Creates database tables
- Ensures OpenSearch indices exist

### 2. **Fetch Metadata** (`fetch_metadata`)  
- Queries ArXiv API for papers matching search criteria
- Extracts titles, authors, abstracts, and PDF URLs

### 3. **Store Metadata** (`store_metadata`)
- Saves paper metadata to PostgreSQL
- Indexes metadata in OpenSearch
- Handles duplicate papers gracefully

### 4. **Download PDFs** (`download_pdfs`)
- Downloads PDFs asynchronously (max 10 concurrent)
- Filters papers by publication year
- Saves to `./data/pdfs/` directory

### 5. **Process & Index** (`parse_chunk_embed_index`)
- Parses PDFs into text chunks
- Generates embeddings using Ollama (batched for performance)
- Stores chunks in PostgreSQL
- Indexes embeddings in OpenSearch for semantic search

## 📊 Performance

### Current Metrics
- **Embedding Speed**: ~0.2 seconds per chunk (CPU-only)
- **Batch Processing**: 10 chunks per batch
- **Average Paper**: ~166 chunks, ~33 seconds processing time
- **Daily Volume**: 43 papers (~25-30 minutes total)

### Scaling Considerations
- **GPU Acceleration**: Add GPU support to Ollama for 10x faster embeddings
- **Parallel Processing**: Process multiple papers simultaneously  
- **Larger Batches**: Increase batch size to 20-50 chunks
- **Smaller Models**: Use lighter embedding models for speed

## 🐛 Troubleshooting

### Common Issues

**1. Ollama Model Not Found**
```bash
# Ensure model is pulled with correct name
docker exec ollama ollama pull nomic-embed-text:latest
```

**2. PDF Download Issues** 
```bash
# Check if PDFs are downloading to correct location
ls -la ./data/pdfs/
```

**3. Database NUL Character Errors**
- The pipeline includes text cleaning to handle corrupted PDF text
- Look for `clean_text_for_db()` function in processing

**4. Network Connectivity**
```bash
# Test Ollama from Airflow container
docker exec -it <airflow-container> curl http://ollama:11434/api/tags
```

### Monitoring

**View Airflow Logs:**
- Go to Airflow UI → DAG → Task → Logs

**Monitor Ollama:**
```bash
docker logs ollama -f
```

**Check Storage:**
```bash
# View downloaded PDFs
ls -la ./data/pdfs/ | wc -l

# Check database
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM papers;"
```

## 🔍 Usage

### Querying the RAG System

Once papers are ingested, you can query the system:

```python
# Example: Semantic search through OpenSearch
from opensearchpy import OpenSearch

client = OpenSearch([{'host': 'localhost', 'port': 9200}])

# Search for similar content
query = {
    "query": {
        "knn": {
            "embedding": {
                "vector": your_query_embedding,
                "k": 10
            }
        }
    }
}

results = client.search(index="chunks", body=query)
```

### Extending the Pipeline

**Add New Data Sources:**
- Modify `arxivClient.py` or create new clients
- Add new tasks to the DAG

**Change Embedding Models:**
- Update `OLLAMA_EMBEDDING_MODEL` in `embeddings.py`
- Pull new model: `docker exec ollama ollama pull <model-name>`

**Custom Chunking Strategies:**
- Modify `parse_and_chunk()` in processing module
- Adjust chunk size, overlap, or splitting logic

## 📈 Scaling to Production

### Performance Optimizations
- **GPU Support**: Add NVIDIA runtime to Ollama container
- **Distributed Processing**: Use Celery with multiple workers
- **Caching**: Implement Redis for intermediate results
- **Monitoring**: Add Prometheus/Grafana for metrics

### Reliability Improvements  
- **Health Checks**: Add container health monitoring
- **Backup Strategy**: Regular database and index backups
- **Alert System**: Slack/email notifications for failures
- **Data Validation**: Add data quality checks

### Security Considerations
- **API Authentication**: Secure Airflow and OpenSearch
- **Network Isolation**: Use Docker networks properly
- **Secrets Management**: Use environment files for credentials
- **Access Control**: Implement role-based permissions

## 📝 License

[Add your license here]

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 💡 Acknowledgments

- Built with guidance from Claude (Anthropic)
- Uses open-source models from Ollama
- Leverages Apache Airflow for orchestration
- ArXiv.org for providing open access to academic papers
