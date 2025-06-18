# Academic Papers Data Pipeline

A scalable data pipeline for ingesting, processing, and searching academic papers using **Flyte** orchestration, **PySpark** processing, and **Iceberg** storage.

## Architecture

```
Academic APIs → Extract Service → S3 → Transform Service → Iceberg Tables → Vector Search
```

### Components

- **`extract/`** - PDF ingestion service that fetches papers from APIs
- **`transform/`** - PySpark job that processes raw data into Iceberg tables  
- **`pipeline.py`** - Flyte workflow that orchestrates the entire pipeline
- **`config.py`** - Configuration for data sources and processing parameters

## Quick Start

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure environment**: Set S3 bucket, region, and other settings in `config.py`
3. **Deploy**: Follow the deployment instructions below

## Data Flow

1. **Extract**: PDF ingestion service downloads papers from academic APIs and stores metadata in S3
2. **Transform**: PySpark job processes raw data, validates quality, and writes to Iceberg tables
3. **Search**: Vector embeddings are generated and indexed for real-time querying

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
# Build the PDF ingestion image
docker build -f extract/Dockerfile -t your-registry/academic-papers-pdf-ingestion:latest .

# Build the PySpark image
docker build -f transform/Dockerfile -t your-registry/academic-papers-pyspark:latest .

# Push to registry
docker push your-registry/academic-papers-pdf-ingestion:latest
docker push your-registry/academic-papers-pyspark:latest
```

### Step 2: Update Flyte Workflow

Update the image references in `pipeline.py`:

```python
# Replace with your actual registry
image="your-registry/academic-papers-pdf-ingestion:latest"
image="your-registry/academic-papers-pyspark:latest"
```

### Step 3: Deploy to Flyte

#### Option 1: Using Flyte CLI

```bash
# Register the workflow
flyte-cli register-files --project your-project --domain development pipeline.py

# Launch the workflow
flyte-cli launch --project your-project --domain development \
  --workflow academic_papers_pipeline \
  --inputs '{"s3_bucket": "your-academic-data-bucket", "sources": ["arxiv", "pubmed"], "days_back": 7}'
```

#### Option 2: Using Python SDK

```python
from flytekit import remote
from pipeline import academic_papers_pipeline

# Connect to Flyte cluster
remote.initialize("your-flyte-cluster-url")

# Launch workflow
result = academic_papers_pipeline.remote(
    s3_bucket="your-academic-data-bucket",
    sources=["arxiv", "pubmed"],
    days_back=7
)
print(f"Workflow result: {result}")
```

### Step 4: Configure Scheduling (Optional)

```python
from flytekit import CronSchedule

@workflow(
    schedule=CronSchedule(
        schedule="0 2 * * *",  # Daily at 2 AM
        kickoff_time_input_arg="kickoff_time"
    )
)
def scheduled_academic_papers_pipeline(
    s3_bucket: str = "your-academic-data-bucket",
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7,
    kickoff_time: datetime = None
) -> Dict[str, Any]:
    return academic_papers_pipeline(
        s3_bucket=s3_bucket,
        sources=sources,
        days_back=days_back
    )
```

### Step 5: Environment Configuration

#### Environment Variables

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

# S3 Configuration
S3_BUCKET=your-academic-data-bucket

# Spark Configuration (if using external Spark cluster)
SPARK_MASTER_URL=spark://your-spark-master:7077
```

#### Flyte Configuration

Create a `flyte.yaml` configuration:

```yaml
# flyte.yaml
project: academic-papers
domain: production
version: v1

# Resource limits
defaults:
  cpu: "2"
  memory: "4Gi"
  storage: "10Gi"

# Task-specific configurations
tasks:
  pdf_ingestion_task:
    cpu: "2"
    memory: "4Gi"
    timeout: "1h"
  
  pyspark_processing_task:
    cpu: "4"
    memory: "8Gi"
    timeout: "2h"
```

### Step 6: Testing

```bash
# Test with small dataset
flyte-cli launch --project your-project --domain development \
  --workflow academic_papers_pipeline \
  --inputs '{"s3_bucket": "test-bucket", "sources": ["arxiv"], "days_back": 1}'
```

### Step 7: Monitoring

Access the Flyte console to monitor:
- Workflow execution status
- Task logs and metrics
- Resource utilization
- Error handling

### Troubleshooting

#### Common Issues

1. **Container Image Not Found**
   - Verify image is pushed to registry
   - Check image name and tag in workflow

2. **S3 Access Denied**
   - Verify AWS credentials
   - Check S3 bucket permissions

3. **Spark Job Fails**
   - Check Spark cluster connectivity
   - Verify Iceberg configuration

#### Debug Commands

```bash
# Check workflow status
flyte-cli get-execution --project your-project --domain development <execution-id>

# Get task logs
flyte-cli get-task-logs --project your-project --domain development <execution-id> <task-id>

# List recent executions
flyte-cli list-executions --project your-project --domain development
```

### Production Considerations

- **Security**: Use IAM roles, enable VPC, use encrypted S3 buckets
- **Scalability**: Configure auto-scaling, use spot instances, implement retry logic
- **Monitoring**: Set up alerts, monitor costs, track API rate limits
- **Backup**: Implement data backup strategies and disaster recovery procedures

## Next Steps

1. **Embedding Pipeline**: Add task for generating embeddings
2. **Vector Search**: Integrate with Qdrant or similar
3. **Real-time API**: Build API service for querying results
4. **Advanced Monitoring**: Implement comprehensive observability