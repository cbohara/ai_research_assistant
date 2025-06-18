# Pipeline Configuration
import os

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "your-academic-data-bucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Data Sources Configuration
# Note: In production, these would be populated by separate API ingestion services
# that fetch data from academic APIs and store results in S3
DATA_SOURCES = {
    "arxiv": {
        "s3_path": f"s3://{S3_BUCKET}/raw/arxiv/*.parquet",  # API responses stored as Parquet
        "format": "parquet",
        "enabled": True,
        "priority": 1,
        "api_info": "arXiv API - https://arxiv.org/help/api"
    },
    "pubmed": {
        "s3_path": f"s3://{S3_BUCKET}/raw/pubmed/*.json",  # PubMed API responses as JSON
        "format": "json", 
        "enabled": True,
        "priority": 2,
        "api_info": "PubMed API - https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/"
    },
    "ieee": {
        "s3_path": f"s3://{S3_BUCKET}/raw/ieee/*.parquet",  # IEEE API responses as Parquet
        "format": "parquet",
        "enabled": True,
        "priority": 3,
        "api_info": "IEEE Xplore API - https://developer.ieee.org/"
    },
    "acm": {
        "s3_path": f"s3://{S3_BUCKET}/raw/acm/*.json",  # ACM API responses as JSON
        "format": "json",
        "enabled": True,
        "priority": 4,
        "api_info": "ACM Digital Library API"
    },
    "springer": {
        "s3_path": f"s3://{S3_BUCKET}/raw/springer/*.parquet",
        "format": "parquet", 
        "enabled": False,  # Disabled by default
        "priority": 5,
        "api_info": "Springer Nature API"
    }
}

# Processing Configuration
PARTITION_LOOKBACK_DAYS = int(os.getenv("PARTITION_LOOKBACK_DAYS", "7"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

# Iceberg Configuration
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "hive")
MASTER_TABLE_NAME = os.getenv("MASTER_TABLE_NAME", "spark_catalog.default.papers_master")

# Data Quality Configuration
MIN_TITLE_LENGTH = int(os.getenv("MIN_TITLE_LENGTH", "10"))
MAX_ABSTRACT_LENGTH = int(os.getenv("MAX_ABSTRACT_LENGTH", "50000"))  # ~50KB
MAX_FULL_TEXT_LENGTH = int(os.getenv("MAX_FULL_TEXT_LENGTH", "1000000"))  # ~1MB

# Spark Configuration
SPARK_CONFIGS = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type": ICEBERG_CATALOG_TYPE,
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.adaptive.coalescePartitions.maxPartitionNum": "100"
}

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Field Mappings (for different source formats)
FIELD_MAPPINGS = {
    "arxiv": {
        "title": "title",
        "authors": "authors", 
        "abstract": "abstract",
        "full_text": "full_text",
        "publication_date": "publication_date",
        "doi": "doi",
        "journal": "journal",
        "keywords": "keywords"
    },
    "pubmed": {
        "title": "title",
        "authors": "authors",
        "abstract": "abstract", 
        "full_text": "full_text",
        "publication_date": "publication_date",
        "doi": "doi",
        "journal": "journal",
        "keywords": "keywords"
    }
    # Add more source-specific mappings as needed
} 