# AI Research Assistant 

A scalable data pipeline for ingesting, processing, and searching academic papers using **Flyte** orchestration, **Python** and **PySpark** processing, and **Iceberg** storage. This data is loaded into a **Qdrant** vector database to power an AI research assistant service.

## Architecture

```
Academic APIs → Raw Data Lake → Refined Data Lakehouse → Vector Database
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
3. **Load Vector Search**: Python job reads from Iceberg tables, generates embeddings, and loads into Qdrant for real-time search

## Features

- **Multi-source ingestion** from arXiv, PubMed, IEEE, ACM
- **Data validation** and deduplication using hash-based IDs
- **Iceberg storage** with date partitioning for efficient querying
- **Flyte orchestration** for reliable pipeline execution
- **Containerized services** for easy deployment