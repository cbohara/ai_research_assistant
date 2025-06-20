# AI Research Assistant 

A scalable data pipeline for ingesting, processing, and searching academic papers using **Flyte** orchestration, **Python** and **PySpark** processing, and **Iceberg** storage. This data is loaded into a **Qdrant** vector database to power an AI research assistant service.

## Architecture

Data flow
```
Academic APIs → Raw Data Lake → Refined Data Lakehouse → Vector database
```

### Components

- **`raw_data_lake/`** - PDF ingestion service that fetches papers from APIs
- **`refined_data_lakehouse/`** - PySpark job that processes raw data into Iceberg tables  
- **`load_vector_database/`** - Service that generates embeddings and loads into Qdrant
- **`pipeline.py`** - Flyte workflow that orchestrates the entire pipeline
- **`config.py`** - Configuration for data sources and processing parameters

## Quick Start

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure environment**: Set S3 bucket, region, and other settings in `config.py`
3. **Deploy**: Follow the deployment instructions below

## Data Flow

1. **Raw Data Lake**: PDF ingestion service downloads papers from academic APIs and stores metadata in S3
2. **Refined Data Lakehouse**: PySpark job processes raw data, validates quality, and writes to Iceberg tables
3. **Load Vector Search**: Service reads from Iceberg tables, generates embeddings, and loads into Qdrant for real-time search

## Features

- **Multi-source ingestion** from arXiv, PubMed, IEEE, ACM
- **Data validation** and deduplication using hash-based IDs
- **Iceberg storage** with date partitioning for efficient querying
- **Flyte orchestration** for reliable pipeline execution
- **Containerized services** for easy deployment

## Configuration

See `config.py` for:
- Data source configurations
- Processing parameters
- Spark and Iceberg settings
- Field mappings for different sources

---

## Deployment Guide

### Prerequisites

1. **Flyte Installation**: Flyte cluster running (local or cloud)
2. **Docker Registry**: Access to push/pull container images
3. **AWS Credentials**: Configured for S3 access
4. **Spark Cluster**: Accessible from Flyte (or use Spark-in-Kubernetes)

### Step 1: Build and Push Container Images

```bash
# Build all images
docker build -f raw_data_lake/Dockerfile -t ghcr.io/cbohara/ai-research-assistant-raw-data-lake:latest .
docker build -f refined_data_lakehouse/Dockerfile -t ghcr.io/cbohara/ai-research-assistant-refined-data-lakehouse:latest .
docker build -f load_vector_database/Dockerfile -t ghcr.io/cbohara/ai-research-assistant-load-vector-search:latest .

# Push to registry
docker push ghcr.io/cbohara/ai-research-assistant-raw-data-lake:latest
docker push ghcr.io/cbohara/ai-research-assistant-refined-data-lakehouse:latest
docker push ghcr.io/cbohara/ai-research-assistant-load-vector-search:latest
```

### Step 2: Deploy to Flyte

```bash
# Register the workflow
flyte-cli register-files --project your-project --domain development pipeline.py

# Launch the workflow
flyte-cli launch --project your-project --domain development \
  --workflow academic_papers_pipeline \
  --inputs '{"s3_bucket": "ai-research-assistant-data-lake", "sources": ["arxiv", "pubmed"], "days_back": 7}'
```

### Step 3: Environment Configuration

Set these environment variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

# S3 Configuration
S3_BUCKET=ai-research-assistant-data-lake

# Docker Registry
DOCKER_REGISTRY=ghcr.io/cbohara
```

### Step 4: Testing

```bash
# Test individual components
flyte-cli launch --project your-project --domain development \
  --workflow raw_data_extraction_only_workflow \
  --inputs '{"s3_bucket": "test-bucket", "sources": ["arxiv"], "days_back": 1}'
```

## Local Development

```bash
# Install development dependencies
pip install -r requirements.txt

# Run individual jobs locally
python raw_data_lake/job.py
python refined_data_lakehouse/job.py
python load_vector_database/job.py

# Test the pipeline
python pipeline.py
```

## Troubleshooting

### Common Issues

1. **Container Image Not Found** - Verify image is pushed to registry
2. **S3 Access Denied** - Check AWS credentials and bucket permissions
3. **Spark Job Fails** - Verify Spark cluster connectivity and Iceberg configuration

### Debug Commands

```bash
# Check workflow status
flyte-cli get-execution --project your-project --domain development <execution-id>

# Get task logs
flyte-cli get-task-logs --project your-project --domain development <execution-id> <task-id>
```
